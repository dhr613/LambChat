# CLAUDE.md

This file provides guidance to Claude (claude.ai/code) when working with code in this repository.

## 常用命令

### 安装依赖
```bash
make install          # 安装 Python 后端依赖（使用 uv）
make frontend-install # 安装前端依赖
make install-all      # 安装所有依赖
```

### 开发运行
```bash
make dev              # 启动后端（http://127.0.0.1:8000）
make frontend-dev     # 启动前端（http://127.0.0.1:3001）
make dev-all          # 同时启动前后端
```

### 代码质量
```bash
uv run ruff check .   # Lint 检查
uv run ruff format .  # 格式化代码
uv run pytest         # 运行所有测试
uv run pytest tests/path/to/test_file.py::test_name  # 运行单个测试
```

### 前端
```bash
cd frontend && npm run dev      # 前端开发服务器
cd frontend && npm run build    # 构建前端
cd frontend && npm run lint     # 前端 lint
cd frontend && npm run i18n:extract  # 提取 i18n 字符串
```

### Docker
```bash
cd deploy && cp .env.example .env  # 首次配置
make docker-up    # 启动 Docker 容器
make docker-down  # 停止 Docker 容器
make docker-logs  # 查看日志
```

## 高层架构

### 技术栈
- **后端**: Python 3.12+, FastAPI, LangGraph, deepagents 框架
- **前端**: React 19, Vite 6, TailwindCSS 3.4
- **存储**: MongoDB（主数据库）+ Redis（缓存/发布订阅）+ 可选 PostgreSQL
- **包管理**: 后端用 `uv`，前端用 `npm`

### Agent 系统核心

Agent 系统基于 **deepagents** 框架，每个 Agent 本质上是一个 **LangGraph CompiledGraph**：

1. 流式请求进入 → 创建 `Presenter`
2. `Presenter` 注入到 `config.configurable["presenter"]`
3. 图节点从 config 中取出 presenter，调用 `present_*` 方法输出 SSE 事件
4. `astream_events` 捕获 LLM/Tool 事件并转为 SSE 格式推送前端

注册新 Agent 使用装饰器：
```python
@register_agent("my-agent")
class MyAgent(BaseGraphAgent):
    def build_graph(self, builder: GraphBuilder): ...
```

Agent 在 `src/agents/__init__.py` 的 `discover_agents()` 中通过导入触发注册。

### 后端目录结构

```
src/
├── agents/          # Agent 实现（core 基类、fast_agent、search_agent 等）
├── api/             # FastAPI 路由层（27 个路由模块）
│   ├── routes/      # 各功能路由（auth, chat, mcp, skills, model 等）
│   ├── admin/       # 管理员 API
│   └── agent/       # Agent 配置与模型管理
├── infra/           # 基础设施服务层（各功能模块独立封装）
│   ├── agent/       # Agent 事件处理
│   ├── auth/        # JWT、OAuth、RBAC（35+ 权限，15 个权限组）
│   ├── backend/     # LLM 后端抽象层
│   ├── llm/         # LLM 集成（OpenAI、Anthropic、Gemini、Kimi）
│   ├── model/       # 模型管理（加密存储 + Redis pub/sub 热更新）
│   ├── mcp/         # MCP 协议集成
│   ├── session/     # 会话管理（MongoDB + Redis 双写）
│   ├── settings/    # 配置存储与 pub/sub 同步
│   ├── skill/       # 技能系统（文件系统 + MongoDB 备份）
│   ├── sandbox/     # 代码沙箱（Daytona / E2B）
│   ├── storage/     # 存储适配器（MongoDB、Redis、PostgreSQL、S3/OSS/MinIO/COS）
│   ├── task/        # 任务管理（并发控制、心跳、pub/sub 通知）
│   ├── tool/        # 工具注册与 MCP 客户端
│   └── websocket/   # WebSocket 与限流
└── kernel/          # 核心 schema、全局配置（settings）、类型定义
```

### 关键设计模式

- **模型热更新**: 模型配置通过 Redis pub/sub 实时同步，无需重启
- **会话双写**: 会话数据同时写入 Redis（缓存）和 MongoDB（持久化）
- **RBAC**: 35+ 细粒度权限，前端 UI 根据用户权限动态显示功能
- **多渠道**: 通过 `channel` 模块支持 Feishu 等外部平台接入，可扩展
- **MCP**: 支持系统级和用户级 MCP 配置，API key 加密存储

### 前端架构

前端使用 **React 19 + Vite 6**，采用"Glass Design System"（glass-shell/glass-card 统一样式规范）。支持 5 种语言（中/英/日/韩/俄），响应式布局覆盖移动端、平板、桌面。

i18n 使用 `react-i18next`，翻译文件提取命令：`npm run i18n:extract`。
