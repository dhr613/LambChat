"""
聊天路由

支持后台执行的聊天接口。
每次对话生成独立的 run_id，实现多轮对话隔离。
"""

import asyncio
import json
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from src.agents.core.base import AgentFactory
from src.api.deps import get_current_user_required, require_permissions
from src.api.routes.session import verify_session_ownership
from src.infra.logging import get_logger
from src.infra.session.manager import SessionManager
from src.infra.task.concurrency import register_executor
from src.infra.task.manager import get_task_manager
from src.infra.task.status import TaskStatus
from src.kernel.schemas.agent import AgentRequest
from src.kernel.schemas.session import SessionUpdate
from src.kernel.schemas.user import TokenPayload

router = APIRouter()
logger = get_logger(__name__)


async def _update_session_config(
    session_id: str,
    run_id: str,
    agent_id: str,
    request: AgentRequest,
) -> None:
    """Update session metadata with conversation configuration."""
    session_manager = SessionManager()
    conversation_config = {
        "current_run_id": run_id,
        "agent_id": agent_id,
        "agent_options": request.agent_options or {},
        "disabled_skills": request.disabled_skills or [],
        "disabled_mcp_tools": request.disabled_mcp_tools or [],
    }
    await session_manager.update_session(
        session_id,
        SessionUpdate(metadata=conversation_config),
    )


async def _execute_agent_stream(
    session_id: str,
    agent_id: str,
    message: str,
    user_id: str,
    presenter=None,
    disabled_tools: list[str] | None = None,
    agent_options: dict | None = None,
    attachments: list[dict] | None = None,
    disabled_skills: list[str] | None = None,
    disabled_mcp_tools: list[str] | None = None,
):
    """执行 Agent 并流式输出事件（供 TaskManager 调用）"""
    from src.infra.task.manager import TaskInterruptedError

    agent = await AgentFactory.get(agent_id)
    run_id = presenter.run_id if presenter else None

    try:
        async for event in agent.stream(
            message,
            session_id,
            user_id=user_id,
            presenter=presenter,
            disabled_tools=disabled_tools,
            agent_options=agent_options,
            attachments=attachments,
            disabled_skills=disabled_skills,
            disabled_mcp_tools=disabled_mcp_tools,
        ):
            yield event
    except (asyncio.CancelledError, TaskInterruptedError):
        # 取消/中断时，调用 agent.close 清理资源
        if run_id:
            await agent.close(run_id)
        raise


# 注册默认的 agent-stream 执行器，以便任何工作节点都能分发队列中的任务
register_executor("agent_stream", _execute_agent_stream)


@router.post("/stream")
async def chat_stream(
    request: AgentRequest,
    agent_id: str = "search",
    user: TokenPayload = Depends(require_permissions("chat:write")),
):
    """
    提交聊天任务，立即返回 session_id 和 run_id

    任务在后台执行，前端可通过 SSE 或轮询获取结果。
    支持基于角色的并发限制：达到上限时排队等待，队列满时返回 429。

    Args:
        request: 包含 message 和 session_id
        agent_id: 要使用的 Agent ID（默认: search）

    Returns:
        session_id: 会话 ID
        run_id: 当前对话轮次的运行 ID
        trace_id: 追踪 ID
        status: 任务状态 (pending / queued)
        queue_position: 排队位置（仅排队时返回）
    """
    from src.infra.task.concurrency import ConcurrencyResult, get_concurrency_limiter
    from src.infra.task.manager import _generate_run_id

    # 获取当前会话的id或生成一个随机的id
    session_id = request.session_id or str(uuid.uuid4())

    # 如果用户传入了 session_id，验证该会话id的所有权（该数据是储存在mongoDB中的），避免被其他用户调用
    if request.session_id:
        session_manager = SessionManager()
        existing_session = await session_manager.get_session(session_id)
        if existing_session:
            verify_session_ownership(existing_session, user)

    # 生成 run_id（不管是否排队都需要唯一 ID）
    run_id = _generate_run_id()

    # 准备附件信息（附件信息是上传的文档信息）
    attachments_data = (
        [a.model_dump() for a in request.attachments] if request.attachments else None
    )

    # 提前生成一个presenter来获取一个train_id（由时间戳与uuid组成）
    from src.infra.writer.present import Presenter, PresenterConfig

    _pre_presenter = Presenter(
        PresenterConfig(
            session_id=session_id,
            agent_id=agent_id,
            user_id=user.sub,
            run_id=run_id,
            enable_storage=False,
        )
    )
    trace_id = _pre_presenter.trace_id

    task_context = {
        "executor_key": "agent_stream",
        "agent_id": agent_id,
        "message": request.message,
        "disabled_tools": request.disabled_tools,
        "agent_options": request.agent_options,
        "attachments": attachments_data,
        "trace_id": trace_id,
        "user_message_written": True,
        "disabled_skills": request.disabled_skills,
        "disabled_mcp_tools": request.disabled_mcp_tools,
    }

    # 检查并发限制
    limiter = get_concurrency_limiter()
    concurrency_result = await limiter.acquire(
        user_id=user.sub,# 用户id
        roles=user.roles,# 该用户允许的角色列表
        run_id=run_id,# run_id，以时间戳与uuid拼接而成的id
        session_id=session_id,# 会话id，直接传递或由uuid随机生成
        task_context=task_context,# 当前对话任务的上下文信息
    )
    # ConcurrencyResult有三种属性：started,queued,rejected_queue
    # 如果结果为rejected_queue，则表示排队已满
    # 如果结果为queued，则表示排队中
    # 如果结果为started，则表示已经开始
    if concurrency_result.result == ConcurrencyResult.REJECTED_QUEUE: # 如果此时排队已满
        raise HTTPException(
            status_code=429,
            detail={
                "error": "too_many_requests",
                "message": f"排队已满，当前活跃 {concurrency_result.active_count}/{concurrency_result.max_concurrent}，排队 {concurrency_result.queue_length}",
                "active": concurrency_result.active_count,
                "max_concurrent": concurrency_result.max_concurrent,
                "queue_length": concurrency_result.queue_length,
            },
        )

    task_manager = get_task_manager()

    # 如果当前任务的状态为排队中
    if concurrency_result.result == ConcurrencyResult.QUEUED:
        # 此时任务上下文已经存储在Redis排队队列中
        # 确保执行器已初始化并立即创建会话
        if task_manager._executor is None:
            from src.infra.task.executor import TaskExecutor

            task_manager._executor = TaskExecutor(
                task_manager.storage, task_manager._run_info, task_manager._heartbeat
            )

        # 确保会话id存在，如果不存在则创建
        await task_manager._executor.ensure_session(
            session_id, agent_id, user.sub, project_id=request.project_id
        )
        # 更新会话的状态为pending
        await task_manager._executor._update_session_status(
            session_id, TaskStatus.PENDING, run_id=run_id
        )

        # 立即写入用户消息事件到MongoDB，以便页面刷新可以加载它
        presenter = Presenter(
            PresenterConfig(
                session_id=session_id,
                agent_id=agent_id,
                user_id=user.sub,
                run_id=run_id,
                trace_id=trace_id,
                enable_storage=True,
            )
        )
        
        await presenter._ensure_trace()
        await presenter.emit_user_message(
            request.message,
            attachments=[a.model_dump() for a in request.attachments]
            if request.attachments
            else None,
        )

        # Mark user message as already written so executor skips re-emitting
        task_manager._run_info[run_id] = {
            "session_id": session_id,
            "agent_id": agent_id,
            "user_id": user.sub,
            "trace_id": trace_id,
            "user_message_written": True,
        }

        # 更新 session metadata，存储完整的对话配置（排队状态）
        await _update_session_config(session_id, run_id, agent_id, request)

        return {
            "session_id": session_id,
            "run_id": run_id,
            "status": "queued",
            "queue_position": concurrency_result.queue_position,
            "max_concurrent": concurrency_result.max_concurrent,
        }

    # STARTED — 正常提交后台任务
    _, _ = await task_manager.submit(
        session_id=session_id,# 会话id
        agent_id=agent_id,# 智能体id
        message=request.message,# 用户请求的消息
        user_id=user.sub,# 用户id
        executor=_execute_agent_stream,# 智能体流式执行组件
        disabled_tools=request.disabled_tools,# 禁用的工具列表
        agent_options=request.agent_options,# 智能体选项
        attachments=attachments_data,# 文件附件列表
        run_id=run_id,# 运行id
        project_id=request.project_id,# 项目id
        disabled_skills=request.disabled_skills,# 禁用的技能列表
        disabled_mcp_tools=request.disabled_mcp_tools,# 禁用的MCP工具列表
    )

    # 更新 session metadata，存储完整的对话配置
    await _update_session_config(session_id, run_id, agent_id, request)

    return {
        "session_id": session_id,
        "run_id": run_id,
        "status": "pending",
    }


@router.get("/sessions/{session_id}/stream")
async def session_stream(
    session_id: str,
    run_id: str = Query(..., description="Run ID for isolating conversation turns"),
    user: TokenPayload = Depends(get_current_user_required),
):
    """
    SSE 流式读取特定 run 的事件

    从 Redis Stream 读取。
    run_id: 对话轮次 ID，用于隔离多轮对话。
    流会在收到 complete 或 error 事件后自动结束。
    """
    from src.infra.logging import get_logger
    from src.infra.session.dual_writer import get_dual_writer

    logger = get_logger(__name__)

    # 验证用户对该 session 的所有权
    session_manager = SessionManager()
    session = await session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    verify_session_ownership(session, user)

    logger.info(f"[SSE] New connection: session={session_id}, run_id={run_id}")

    dual_writer = get_dual_writer()

    async def event_generator():
        logger.info(f"[SSE] Generator started for session={session_id}, run_id={run_id}")
        try:
            # 使用 run_id 读取特定轮次的事件
            event_count = 0
            async for event in dual_writer.read_from_redis(
                session_id,
                run_id=run_id,
            ):
                # 心跳事件：发送 SSE 注释（: 开头的行被 EventSource 忽略）
                # 这样能检测到客户端断开，同时不干扰前端逻辑
                if event["event_type"] == "heartbeat":
                    yield ": heartbeat\n\n"
                    continue

                event_count += 1
                # Include timestamp in the data payload for deduplication
                event_data = event["data"]
                if isinstance(event_data, dict) and event.get("timestamp"):
                    # Create a copy to avoid modifying the original
                    event_data = {**event_data, "_timestamp": event["timestamp"]}
                yield f"event: {event['event_type']}\ndata: {json.dumps(event_data, ensure_ascii=False)}\nid: {event['id']}\n\n"

            logger.info(f"[SSE] Stream ended after {event_count} events")

        except Exception as e:
            logger.error(f"[SSE] Generator error: {e}")
            yield 'event: error\ndata: {"error": "An internal error occurred"}\n\n'

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


@router.get("/sessions/{session_id}/status")
async def get_session_status(
    session_id: str,
    run_id: str = Query(None, description="Run ID (optional, defaults to current run)"),
    user: TokenPayload = Depends(get_current_user_required),
):
    """
    获取任务状态

    Args:
        session_id: 会话 ID
        run_id: 运行 ID（可选，默认为当前 run）
    """
    # 验证用户对该 session 的所有权
    session_manager = SessionManager()
    session = await session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    verify_session_ownership(session, user)

    task_manager = get_task_manager()

    if run_id:
        status = await task_manager.get_run_status(session_id, run_id)
        error = await task_manager.get_run_error(run_id)
    else:
        status = await task_manager.get_status(session_id)
        error = await task_manager.get_error(session_id)

    return {
        "session_id": session_id,
        "run_id": run_id,
        "status": status.value,
        "error": error,
    }


@router.post("/sessions/{session_id}/cancel")
async def cancel_session(
    session_id: str,
    user: TokenPayload = Depends(get_current_user_required),
):
    """
    取消正在运行的任务（包括排队中的任务）

    Args:
        session_id: 会话 ID

    Returns:
        success: 是否成功设置取消信号
        cancelled_locally: 是否在本地实例取消
        run_id: 被取消的运行 ID
        message: 状态信息
    """
    # 验证用户对该 session 的所有权
    session_manager = SessionManager()
    session = await session_manager.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="会话不存在")
    verify_session_ownership(session, user)

    task_manager = get_task_manager()
    result = await task_manager.cancel(session_id, user_id=user.sub)

    # 如果本地没有取消到，尝试从排队队列中移除
    if not result.get("cancelled_locally"):
        try:
            from src.infra.task.concurrency import get_concurrency_limiter

            limiter = get_concurrency_limiter()
            removed = await limiter.remove_from_queue(user.sub, session_id)
            if removed:
                result["message"] = f"已从排队中移除 ({removed} 个任务)"
        except Exception as e:
            logger.warning(f"Failed to remove from queue: {e}")

    return result
