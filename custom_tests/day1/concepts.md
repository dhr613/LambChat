"""
LangChain/LangGraph 流式输出事件传递流程 - 伪代码解析

这个文件展示了当一个基于 LangChain + LangGraph 的 Agent 执行任务时，
事件是如何从 LLM → LangGraph → 自定义处理器 → 前端 的完整流程。

================================================================================
                              完整数据流图
================================================================================

┌─────────────────────────────────────────────────────────────────────────────┐
│                           LLM (大语言模型)                                   │
│  例如: OpenAI GPT-4, Anthropic Claude, 本地模型等                            │
│                                                                             │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐                 │
│  │思考开始 │───→│推理中...│───→│调用工具 │───→│生成回复 │                 │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘                 │
│       │              │              │              │                       │
└───────┼──────────────┼──────────────┼──────────────┼───────────────────────┘
        │              │              │              │
        ▼              ▼              ▼              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    LangGraph 内部事件 (astream_events)                       │
│                                                                             │
│  事件类型:                                                                   │
│  ┌──────────────────┬──────────────────────────────────────────────────┐   │
│  │ on_chat_model_    │ • on_chat_model_stream: LLM 输出片段              │   │
│  │ start/end        │ • on_chat_model_end: LLM 输出完成                  │   │
│  ├──────────────────┼──────────────────────────────────────────────────┤   │
│  │ on_tool_         │ • on_tool_start: 工具开始调用                      │   │
│  │ start/end/error  │ • on_tool_end: 工具调用完成                       │   │
│  │                  │ • on_tool_error: 工具调用失败                      │   │
│  ├──────────────────┼──────────────────────────────────────────────────┤   │
│  │ on_chain_        │ • on_chain_start/end: 节点开始/结束               │   │
│  │ start/end        │                                                   │   │
│  └──────────────────┴──────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          你的 Agent 代码                                    │
│                                                                             │
│  # 1. 创建工作流 (通常在 build_graph() 中)                                  │
│  📂 src/agents/search_agent/graph.py                                       │
│      第 85-97 行: build_graph() 构建图结构                                  │
│                                                                             │
│  # 2. 启动流式执行 (在 _stream() 中)                                        │
│  📂 src/agents/search_agent/nodes.py                                       │
│      第 248-261 行: astream_events 循环                                    │
│                                                                             │
│      async for event in graph.astream_events(input, config, version="v2"): │
│          await event_processor.process_event(event)  # ← 处理每个事件      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       EventProcessor (你的事件处理器)                        │
│                                                                             │
│  📂 src/infra/agent/events/                                                │
│                                                                             │
│  processor.py  第 32-166 行                                                │
│  ├── class AgentEventProcessor                                            │
│  │   └── async process_event()  第 103 行                                  │
│  │       • 分发事件到不同的 Mixin                                          │
│  │                                                                     │
│  ├── stream.py                                                            │
│  │   └── class StreamEventMixin                                          │
│  │       ├── _handle_chat_stream()    第 146-210 行                      │
│  │       ├── _handle_summary_stream()  第 115-144 行                      │
│  │       └── _flush_chunk_buffer()     第 25-38 行                        │
│  │                                                                     │
│  ├── tool_events.py                                                       │
│  │   └── class ToolEventMixin                                           │
│  │       ├── _handle_tool_start()    第 23-54 行                        │
│  │       └── _handle_tool_end()      第 56-98 行                        │
│  │                                                                     │
│  └── subagents.py                                                        │
│      └── class SubagentEventMixin                                        │
│          • 处理子 Agent 调用/结果事件                                       │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Presenter (输出格式化器)                           │
│                                                                             │
│  📂 src/infra/writer/present.py                                           │
│                                                                             │
│  第 86-902 行: Presenter 类                                                │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │ present_tool_start()   第 523-551 行                                │  │
│  │ present_tool_result()  第 553-589 行                                │  │
│  │ present_thinking()     第 415-439 行                                │  │
│  │ present_text()         第 363-387 行                                │  │
│  │ present_todo()         第 441-462 行                                │  │
│  │ present_summary()      第 389-413 行                                │  │
│  │ present_agent_call()   第 464-490 行                                │  │
│  │ present_agent_result() 第 492-521 行                                │  │
│  │ async emit()            第 334-345 行                                │  │
│  │ async save_event()     第 255-291 行                                │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SSE 流 (Server-Sent Events)                          │
│                                                                             │
│  📂 src/api/routes/chat.py                                                │
│      SSE 端点: /chat/sessions/{session_id}/stream                          │
│                                                                             │
│  事件格式:                                                                 │
│  event: tool:start                                                        │
│  data: {"tool": "web_search", "args": {...}, "timestamp": "..."}          │
│                                                                             │
│  event: thinking                                                          │
│  data: {"content": "正在思考...", "thinking_id": "xxx"}                   │
│                                                                             │
│  event: message:chunk                                                     │
│  data: {"content": "根据搜索结果...", "text_id": "xxx"}                  │
│                                                                             │
│  event: todo:updated                                                      │
│  data: {"todos": [...], "timestamp": "..."}                              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           前端 (浏览器)                                     │
│                                                                             │
│  📂 frontend/src/hooks/useAgent/                                          │
│                                                                             │
│  ├── sseConnection.ts                                                     │
│  │   第 57-185 行: connectToSSE() - 建立 SSE 连接                         │
│  │                                                                     │
│  ├── eventHandlers.ts                                                    │
│  │   第 43-284 行: handleStreamEvent() - 事件处理入口                    │
│  │                                                                     │
│  ├── eventProcessor.ts                                                   │
│  │   第 83-445 行: processMessageEvent() - 转换消息状态                  │
│  │                                                                     │
│  ├── messageParts.ts                                                     │
│  │   createThinkingPart()  - 创建思考气泡                               │
│  │   createToolPart()     - 创建工具卡片                                │
│  │   createSubagentPart() - 创建子 Agent 面板                            │
│  │                                                                     │
│  └── types.ts                                                            │
│      定义各种 Part 类型 (ThinkingPart, ToolPart, TodoPart 等)              │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

================================================================================
                           完整伪代码实现
================================================================================
"""

# ==============================================================================
# 第一部分: 定义 LangGraph 工作流
# ==============================================================================

"""
📂 src/agents/search_agent/graph.py  第 85-97 行
    class SearchAgent
    └── build_graph()

📂 src/agents/search_agent/state.py
    定义 SearchAgentState
"""

# 1. 定义状态 (State)
from typing import TypedDict, Annotated
from langgraph.graph import add_messages

class AgentState(TypedDict):
    messages: Annotated[list, add_messages]      # 对话历史
    todo_list: list[dict]                        # 任务列表
    current_step: str                            # 当前执行步骤
    search_results: dict                         # 搜索结果

# ==============================================================================
# 第二部分: 定义节点 (Nodes)
# ==============================================================================

"""
📂 src/agents/search_agent/nodes.py
    async agent_node()  第 58-300 行

    这是实际被调用的节点，内部创建 deep agent 并处理事件流
"""

# 规划节点 - 负责分析任务并生成执行计划

async def planner_node(state: AgentState, config: RunnableConfig) -> AgentState:
    '''
    这个节点分析用户请求，生成执行计划
    '''
    # 📂 src/agents/core/base.py
    # get_presenter() 从 config 中获取 presenter
    presenter = config["configurable"]["presenter"]

    # 获取用户消息
    user_message = state["messages"][-1].content

    # 发送"正在分析任务"事件
    # 📂 src/infra/writer/present.py  第 807-811 行
    # async emit_thinking()
    await presenter.emit_thinking("正在分析任务...")

    # 模拟 LLM 调用来分解任务
    # (实际代码中这里会调用 LLM 来分析任务)
    plan = [
        {"content": "检索相关信息", "status": "pending", "activeForm": "正在检索..."},
        {"content": "分析检索结果", "status": "pending", "activeForm": "正在分析..."},
        {"content": "生成回答", "status": "pending", "activeForm": "正在生成..."},
    ]

    # 发送任务列表更新事件 (这是关键!)
    # 📂 src/infra/writer/present.py  第 441-462 行
    # present_todo() / 第 807-811 emit_thinking()
    await presenter.emit_todo(plan)

    return {"todo_list": plan, "current_step": "planning"}

# 执行节点 - 负责执行计划中的各个步骤

async def executor_node(state: AgentState, config: RunnableConfig) -> AgentState:
    '''
    这个节点执行规划节点生成的计划
    '''
    presenter = config["configurable"]["presenter"]
    todo_list = state["todo_list"]

    # 遍历任务列表执行
    for i, task in enumerate(todo_list):
        # 更新当前任务状态为进行中
        todo_list[i]["status"] = "in_progress"
        await presenter.emit_todo(todo_list)

        # 发送思考事件 (说明正在做什么)
        await presenter.emit_thinking(
            f"正在执行: {task['content']}"
        )

        # 模拟工具调用
        if "检索" in task["content"]:
            # 开始工具调用
            # 📂 src/infra/writer/present.py  第 523-551 行
            # present_tool_start()
            await presenter.emit_tool_start(
                tool_name="web_search",
                tool_input={"query": "Python tutorial"}
            )

            # 模拟工具执行
            result = await simulate_search()

            # 工具调用完成
            # 📂 src/infra/writer/present.py  第 553-589 行
            # present_tool_result()
            await presenter.emit_tool_result(
                tool_name="web_search",
                result=result,
                success=True
            )

        # 标记任务完成
        todo_list[i]["status"] = "completed"
        await presenter.emit_todo(todo_list)

    # 生成最终回答
    await presenter.emit_thinking("正在整理回答...")
    response = "基于检索结果，Python 是一种高级编程语言..."

    return {"messages": [response], "todo_list": todo_list}

# ==============================================================================
# 第三部分: 构建和编译工作流
# ==============================================================================

"""
📂 src/agents/core/base.py
    class BaseGraphAgent
    └── build_graph()  - 子类重写此方法定义图结构
"""

from langgraph.graph import StateGraph

def build_workflow():
    '''
    构建 LangGraph 工作流

    架构:
        START → planner_node → executor_node → END
                     ↓              ↓
               发送 todo      执行任务列表
               任务列表         更新进度
    '''
    # 创建状态图
    builder = StateGraph(AgentState)

    # 添加节点
    builder.add_node("planner", planner_node)
    builder.add_node("executor", executor_node)

    # 设置入口点
    builder.set_entry_point("planner")

    # 添加边
    builder.add_edge("planner", "executor")
    builder.add_edge("executor", END)

    # 编译
    return builder.compile()

# ==============================================================================
# 第四部分: 核心 - 流式事件处理
# ==============================================================================

"""
📂 src/infra/agent/events/processor.py  第 32-166 行
    class AgentEventProcessor

📂 src/infra/agent/events/stream.py
    class StreamEventMixin

📂 src/infra/agent/events/tool_events.py
    class ToolEventMixin
"""

class EventProcessor:
    '''
    事件处理器 - 捕获 LangGraph 的 astream_events 并转换为 SSE 事件
    '''

    def __init__(self, presenter):
        self.presenter = presenter

    async def process_event(self, event: dict):
        '''
        📂 src/infra/agent/events/processor.py  第 103 行

        LangGraph 的 astream_events 会产生以下类型的事件:

        1. on_chain_start/end    - 节点开始/结束
        2. on_chat_model_start   - LLM 调用开始
        3. on_chat_model_stream  - LLM 输出片段 (token by token)
        4. on_chat_model_end     - LLM 调用结束
        5. on_tool_start         - 工具调用开始
        6. on_tool_end           - 工具调用结束
        7. on_tool_error         - 工具调用出错
        '''
        event_type = event.get("event")
        name = event.get("name", "")

        # 1. 处理 LLM 流式输出
        if event_type == "on_chat_model_stream":
            await self._handle_llm_stream(event)

        # 2. 处理工具调用
        elif event_type == "on_tool_start":
            await self._handle_tool_start(event)

        elif event_type == "on_tool_end":
            await self._handle_tool_end(event)

    async def _handle_llm_stream(self, event: dict):
        '''
        📂 src/infra/agent/events/stream.py  第 146-210 行
            _handle_chat_stream()

        处理 LLM 的流式输出

        重要: LangGraph v2 的事件结构:
        {
            "event": "on_chat_model_stream",
            "data": {
                "chunk": {
                    "content": "一个",      # 文本内容
                    "additional_kwargs": {
                        "reasoning_content": "我正在思考..."  # 思考内容
                    }
                }
            },
            "metadata": {...}
        }
        '''
        chunk = event.get("data", {}).get("chunk", {})

        # 1. 处理思考内容 (reasoning_content)
        reasoning = chunk.get("additional_kwargs", {}).get("reasoning_content")
        if reasoning:
            # 这是模型的思考过程 - 显示给用户看
            # 📂 src/infra/writer/present.py  第 415-439 行
            event = self.presenter.present_thinking(
                content=reasoning,
                thinking_id=chunk.get("id")
            )
            await self.presenter.emit(event)

        # 2. 处理正常文本输出
        content = chunk.get("content", "")
        if content:
            # 📂 src/infra/writer/present.py  第 363-387 行
            event = self.presenter.present_text(
                content=content,
                text_id=chunk.get("id")
            )
            await self.presenter.emit(event)

    async def _handle_tool_start(self, event: dict):
        '''
        📂 src/infra/agent/events/tool_events.py  第 23-54 行
            _handle_tool_start()

        处理工具开始调用

        事件结构:
        {
            "event": "on_tool_start",
            "name": "web_search",           # 工具名
            "data": {
                "input": {"query": "xxx"}   # 工具输入
            },
            "run_id": "tool_xxx"            # 工具调用ID
        }
        '''
        tool_name = event.get("name")
        tool_input = event.get("data", {}).get("input", {})
        run_id = event.get("run_id")

        # 发送工具开始事件 → 前端显示 "正在检索 web_search"
        # 📂 src/infra/writer/present.py  第 523-551 行
        event = self.presenter.present_tool_start(
            tool_name=tool_name,
            tool_input=tool_input,
            tool_call_id=run_id
        )
        await self.presenter.emit(event)

    async def _handle_tool_end(self, event: dict):
        '''
        📂 src/infra/agent/events/tool_events.py  第 56-98 行
            _handle_tool_end()

        处理工具调用完成

        事件结构:
        {
            "event": "on_tool_end",
            "name": "web_search",
            "data": {
                "output": "搜索结果..."    # 工具返回结果
            }
        }
        '''
        tool_name = event.get("name")
        result = event.get("data", {}).get("output", "")
        run_id = event.get("run_id")

        # 📂 src/infra/writer/present.py  第 553-589 行
        event = self.presenter.present_tool_result(
            tool_name=tool_name,
            result=result,
            tool_call_id=run_id,
            success=True
        )
        await self.presenter.emit(event)

# ==============================================================================
# 第五部分: 运行工作流
# ==============================================================================

"""
📂 src/agents/search_agent/nodes.py  第 116-233 行
    async _stream()

📂 src/agents/search_agent/nodes.py  第 248-261 行
    async for event in inner_graph.astream_events(...)
"""

async def run_agent_streaming(user_input: str):
    '''
    运行 Agent 并流式返回事件
    '''
    # 1. 初始化
    # 📂 src/infra/writer/present.py  第 86-109 行
    presenter = Presenter()
    event_processor = EventProcessor(presenter)
    graph = build_workflow()

    # 2. 准备配置 (注入 presenter 和其他依赖)
    # 📂 src/agents/core/base.py
    # get_presenter() 从 config 中获取
    config = {
        "configurable": {
            "presenter": presenter,       # 关键: 注入 presenter
            "thread_id": "session_123",
        }
    }

    # 3. 初始状态
    initial_state = {
        "messages": [HumanMessage(content=user_input)],
        "todo_list": [],
        "current_step": "",
        "search_results": {},
    }

    # 4. 核心: 使用 astream_events 捕获所有事件
    #
    # 📂 src/agents/search_agent/nodes.py  第 254-261 行
    #
    # 这里才是真正的流式输出!
    # astream_events 会实时 yield 每一个 LLM/工具 事件
    async for event in graph.astream_events(initial_state, config, version="v2"):
        # 5. 处理每个事件
        await event_processor.process_event(event)

        # 6. 同时 yield 事件给 SSE 端点
        # (presenter.emit 会保存并返回事件)
        # 📂 src/infra/writer/present.py  第 334-345 行
        processed_event = await presenter.emit({})  # 实际会从缓冲区获取
        yield processed_event

# ==============================================================================
# 第六部分: SSE API 端点
# ==============================================================================

"""
📂 src/api/routes/chat.py
    查找 SSE 端点实现
"""

# FastAPI 端点示例

from fastapi import APIRouter
from sse_starlette.sse import EventSourceResponse

router = APIRouter()

@router.get("/chat/stream")
async def stream_chat(message: str):
    '''
    SSE 流式聊天端点

    前端通过 EventSource 连接此端点，
    实时接收 Agent 执行过程中的各种事件
    '''

    async def event_generator():
        # 创建异步生成器
        async for event in run_agent_streaming(message):
            # 将事件转换为 SSE 格式
            yield {
                "event": event["event"],
                "data": json.dumps(event["data"]),
            }

    return EventSourceResponse(event_generator())

# ==============================================================================
# 第七部分: 前端事件处理
# ==============================================================================

"""
📂 frontend/src/hooks/useAgent/
"""

# 前端事件处理伪代码

# 1. 建立 SSE 连接
# 📂 frontend/src/hooks/useAgent/sseConnection.ts  第 57-185 行
async function connectToSSE(sessionId, messageId):
    await fetchEventSource(
        `/api/chat/sessions/${sessionId}/stream?run_id=${messageId}`,
        {
            onmessage: (event) => {
                # 2. 接收事件
                # 📂 frontend/src/hooks/useAgent/eventHandlers.ts  第 43 行
                handleStreamEvent(event, messageId, eventId, timestamp, ctx)
            }
        }
    )

# 2. 处理 SSE 事件
# 📂 frontend/src/hooks/useAgent/eventHandlers.ts  第 43-284 行
function handleStreamEvent(event, messageId, eventId, timestamp, ctx):
    eventType = event.event  # 例如: "tool:start", "thinking", "message:chunk"

    # 3. 根据事件类型转换消息状态
    # 📂 frontend/src/hooks/useAgent/eventProcessor.ts  第 83-445 行
    result = processMessageEvent(
        eventType,
        data,
        parts,
        content,
        toolCalls,
        depth,
        subagentStack,
        isStreaming,
        messageId
    )

    # 4. 更新 React 状态
    setMessages(prev => prev.map(m => {
        if (m.id === messageId) {
            return {
                ...m,
                parts: result.parts,
                content: result.content,
                toolCalls: result.toolCalls,
            }
        }
        return m
    }))

# 3. 事件类型与 UI 组件映射
# 📂 frontend/src/hooks/useAgent/eventProcessor.ts

MESSAGE_EVENTS = {
    "agent:call":      createSubagentPart,    # 子 Agent 调用面板
    "agent:result":    updateSubagentResult,  # 更新子 Agent 结果
    "thinking":        createThinkingPart,     # 思考气泡
    "message:chunk":   appendText,            # 追加文本
    "tool:start":      createToolPart,         # 工具卡片 (显示 "正在检索...")
    "tool:result":     updateToolResult,      # 更新工具结果
    "sandbox:*":       upsertSandboxPart,     # 沙箱状态
    "token:usage":     updateTokenUsage,      # Token 统计
    "todo:updated":    upsertTodoPart,        # 任务列表
    "summary":         createSummaryPart,      # 摘要
    "error":           showError,              # 错误显示
}

# ==============================================================================
# 关键点总结
# ==============================================================================

'''
1. astream_events 是 LangGraph 的核心流式 API
   📂 src/agents/search_agent/nodes.py  第 254 行
   - 它会实时 yield LLM 输出、工具调用等所有事件
   - version="v2" 提供更丰富的事件元数据

2. 事件类型:
   📂 src/infra/agent/events/processor.py  第 103 行
   - on_chat_model_stream: LLM 输出片段
   - on_tool_start/end: 工具调用
   - on_chain_start/end: 节点执行

3. EventProcessor 的分发逻辑:
   📂 src/infra/agent/events/processor.py  第 137-151 行
   match event_type:
       case "on_chat_model_stream":  → _handle_chat_stream()
       case "on_tool_start":         → _handle_tool_start()
       case "on_tool_end":           → _handle_tool_end()

4. Presenter 的作用:
   📂 src/infra/writer/present.py  第 334-345 行
   - 统一格式化事件为 SSE 格式
   - 保存事件到存储 (Redis/MongoDB)
   - 提供 emit() 方法一步完成

5. 前端事件处理:
   📂 frontend/src/hooks/useAgent/eventHandlers.ts  第 184-198 行
   const MESSAGE_EVENTS = new Set([
       "agent:call", "agent:result", "thinking", "message:chunk",
       "tool:start", "tool:result", "sandbox:*", "token:usage",
       "todo:updated", "summary", "error"
   ])

这就是为什么你能看到 "正在检索..." 这种进度提示!
'''
