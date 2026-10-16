# 配置保存即激活边界（HA 连接与持续发布热切换）- 产品需求文档

## Overview
- **Summary**: 把 EMHASS Web 服务中的"配置保存"定义为一个完整、原子的**激活边界**：管理员不重启服务，通过配置页更换 Home Assistant 地址/令牌，或启用/停用持续发布（continual_publish）后，只有当新配置完成校验、可恢复持久化、所需 HA 连接与持续发布资源全部就绪并接管时，保存成功响应才返回；接管失败时旧配置继续完整服务。
- **Purpose**: 消除当前"保存成功但运行时仍旧"的不一致：全局 WebSocket 客户端一旦建立便忽略后续传入的 URL/令牌；持续发布后台任务在首次 action 时捕获整套 `input_data_dict`（含 rh、time_zone、freq）后永不更新、无法停止；secrets 保存后 params.pkl 不重建；切换中途崩溃会留下半新半旧的配置。
- **Target Users**: 通过 EMHASS 配置页（`/configuration`、`/set-config`）管理运行中实例的管理员/用户；以及依赖持续发布传感器的自动化。

## Goals
- 保存成功 == 新配置已在同一进程内完整激活：params、params_secrets、全局 HA 连接（WS 代际）、持续发布任务四者一致，无新旧混读。
- 新连接（WS 连不上/鉴权失败，或 REST 探测失败）建立失败时，拒绝本次激活，旧连接、旧发布任务、旧文件全部继续服务。
- 旧连接在新连接可用后有序关闭，允许已进入的 action 用其进入时的旧配置跑完。
- 持续发布的启停、时区、频率、目标（rh/实体）严格跟随活动配置；禁用后不残留后台任务及其 HTTP/WS 资源；旧任务停不掉时绝不启动第二个。
- 切换阶段任意时刻崩溃（写文件中断、进程被杀），重启只恢复最后一次**完整激活**的配置。
- 默认 REST 模式、默认不持续发布、只改普通优化参数、现有配置字段/成功响应/服务关闭清理行为保持不变。

## Non-Goals
- 不改动优化数学模型、求解器、forecast 获取逻辑（`optimization.py`、`forecast.py` 零改动或仅无关导入）。
- 不做多 worker/多进程间的激活状态同步（现状全局 WS 本就是进程内单例；默认 `WEB_CONCURRENCY=1`）。多 worker 部署的跨进程一致性不在本次范围。
- 不改变 CLI 入口（`command_line.py` 的 perfect/dayahead/mpc/publish 入口签名与语义）。
- 不改变 InfluxDB / VictoriaMetrics 的连接方式（本就是每请求 HTTP，无持久资源）；HA 地址/令牌变化的校验以 HA REST/WS 为准。
- 不新增第三方依赖；不改动 `param_definitions.json`（无新增参数）。
- 不新增/变更前端页面字段；前端保存调用方式不变。

## Background & Context

当前实现（基线提交 @237d0fe）：

- `POST /set-config`（[parameter_set](file:///f:/swe/098853/project-03/src/emhass/web_server.py#L416-L526)）：直接截断写 `config.json` 与 `params.pkl`，不触碰运行时连接与发布任务。
- `POST /configuration`（[configuration](file:///f:/swe/098853/project-03/src/emhass/web_server.py#L236-L305)）：把 hass_url/long_lived_token 等 secrets 写入 `secrets_emhass.yaml` 并更新全局 `params_secrets`，但不重建 params.pkl，action 仍读旧值直到重启。
- 全局 WS（[connection_manager.py](file:///f:/swe/098853/project-03/src/emhass/connection_manager.py)）：模块级单例 `_global_client`，[get_websocket_client](file:///f:/swe/098853/project-03/src/emhass/connection_manager.py#L50-L81) 对已存在客户端一律复用，调用方传入的 URL/令牌被忽略。
- 持续发布（[continual_publish](file:///f:/swe/098853/project-03/src/emhass/command_line.py#L4192-L4228)）：`while True` 任务，由 [action_call](file:///f:/swe/098853/project-03/src/emhass/web_server.py#L873-L894) 在首个带 `continual_publish: true` 的 action 时懒启动，把该 action 的 `rh`（含旧 URL/令牌/时区/HTTP session）移交给任务；任务永不停止、不随配置更新，关闭时也无显式回收（其 rh 的 HTTP session 不关闭）。
- 每个 action 在进入时从 params.pkl 读取并解析自己的 params 快照（[_load_params_and_runtime](file:///f:/swe/098853/project-03/src/emhass/web_server.py#L529-L574)），天然具备"进入后不受文件更新影响"的隔离性；问题只在全局 WS 复用与发布任务捕获。
- 启动流程 [initialize()](file:///f:/swe/098853/project-03/src/emhass/web_server.py#L1128-L1181)：best-effort 初始化 WS（失败仅告警、继续服务），不启动持续发布。
- 向 HA 发布数据始终走 REST（[post_data](file:///f:/swe/098853/project-03/src/emhass/retrieve_hass.py#L2089)），与 `use_websocket` 无关；WS 仅用于取数。
- `config_path`（默认 `/share/config.json`）、`secrets_path`（默认 `/app/secrets_emhass.yaml`）与 `data_path`（默认 `/data/`）可能位于不同挂载点，不能依赖跨设备 `os.replace`；临时文件必须写在目标文件同目录。

## Functional Requirements

- **FR-1 统一激活入口**：新增进程内运行时管理器（`runtime_manager.py`），`/set-config` 与 `/configuration` 的保存都经由同一个 `activate()` 串行激活流程（单一激活锁，并发保存依次完成且不产生混合状态）。
- **FR-2 先校验、再持久化（可恢复）**：激活依次为 ①内存校验（build_params 与端点既有校验，非法即 400，不落盘）→ ②暂存新文件 + 写 pending 标记 + 备份旧文件 → ③准备新连接 → ④切换发布资源 → ⑤提交落盘并切换内存活动状态 → ⑥成功响应。成功响应返回时全部步骤已完成。
- **FR-3 新连接准备（提交前、非破坏）**：
  - 新配置 `use_websocket=true` 且活动中无同身份（URL+令牌）客户端：创建并完成 connect+auth（沿用 15s 超时），成功前不替换活动客户端。
  - REST 模式且 hass_url/令牌身份变化：用候选配置构建一次性 `RetrieveHass` 走现有 REST `get_ha_config()` 探测；返回失败即激活失败。无令牌的 local/supervisor 模式按现有语义跳过探测（视为就绪）。
  - 仅修改普通优化参数（身份、模式、continual_publish 均未变）时不发起任何网络探测。
- **FR-4 连接代际切换**：`connection_manager` 变为身份感知：同身份复用/断线重连，身份不同则建立新一代；旧代在新代可用后进入退役关闭。`RetrieveHass` 对其取得的 WS 客户端做一次 retain、在 `close()` 时 release；旧代引用计数归零立即关闭，否则在宽限期（30s）后强制关闭。关闭全局连接（服务停止）时立即关闭所有代际。
- **FR-5 持续发布生命周期归管理器所有**：发布管理器持有唯一任务与自建 rh（由活动 params 构造，仅含 rh + retrieve_hass_conf 的精简上下文，沿用现有 `continual_publish` 循环与 metadata 动态调频行为）。激活时按新活动配置 reconcile：启用则保证恰好一个任务以新 rh/时区/频率运行；停用则 cancel 并在有界时间内确认退出、关闭其 rh。`action_call` 不再懒启动/移交 rh，其 rh 在请求结束时一律关闭。
- **FR-6 启动即 reconcile**：启动时先做崩溃恢复（见 FR-7），再按最后完整激活的配置建立活动状态；若该配置启用 continual_publish，则启动发布任务（实体目录为空时循环空转，行为等同现有循环的空目录保护）。启动期 WS 初始化保持 best-effort 现状。
- **FR-7 崩溃可恢复持久化**：暂存写目标同目录的 `*.tmp.<gen>`，pending 标记与回滚备份位于 `data_path/.activation/`；提交按固定顺序 `os.replace` 落盘。启动恢复：存在 pending 标记时，用备份逐文件还原（备份不存在的目标文件删除），使磁盘回到最后一次完整激活；无标记则正常启动。提交前失败清理暂存与标记，活动文件不动。
- **FR-8 失败语义**：
  - `/set-config`：校验失败保持 400；激活阶段失败（连不上 HA、发布任务停不掉、提交落盘失败）返回 503，响应体沿用现有字符串列表形状，活动配置与文件保持旧状态。成功仍为 200 `{}`。
  - `/configuration`：保持现有响应（渲染 configuration.html，200），激活失败仅记录错误日志，磁盘与运行时不变。
- **FR-9 在途动作隔离**：已在执行的 action 继续使用其进入时解析的 params 与已取得的旧代 WS；切换后新进入的 action 一律读新 params.pkl、取得新代 WS。发布任务切换不依赖在途 action。

## Non-Functional Requirements

- **NFR-1 无泄漏**：禁用持续发布、激活失败、服务关闭三条路径下，不存在遗留 asyncio 任务或未关闭的 aiohttp session / websocket。
- **NFR-2 可测**：激活、恢复、连接代际、发布 reconcile 均可在不依赖真实 HA 的前提下用 pytest + mock 验证；新增行为全部有测试。
- **NFR-3 手术式**：不重构无关模块；`command_line.py::continual_publish` 循环逻辑尽量零改动；遵守仓库 ruff 规则与现有代码风格；不引入新依赖。
- **NFR-4 零配置默认不坏**：默认 REST、默认 continual_publish=false 的实例在所有变更后行为与现状逐字节一致（端点响应、日志关键行为、after_serving 清理）。
- **NFR-5 性能**：普通参数保存不引入额外网络往返与任务重启；激活锁持有期间不阻塞与保存无关的 action 请求（锁仅串行化激活流程）。

## Constraints
- **Technical**: Python 版本以 `pyproject.toml` 为准；Quart + Uvicorn，asyncio；跨平台（开发 Windows / 生产 Linux 容器），文件原子性用"同目录临时文件 + os.replace"；`aiofiles` 异步写。
- **Business**: 遵守 AGENTS.md 五条不变量（action_logs 行格式、logger handler、两套日志、param_definitions.json 仅增、opt_res 列与单位）；不新增参数，不改默认值。
- **Dependencies**: 仅现有依赖（aiohttp、websockets、aiofiles、orjson、pandas）。

## Assumptions
- 单 worker 部署（`WEB_CONCURRENCY` 默认 1，与现有全局单例相同的前提）；多 worker 不在范围。
- "新连接建立"在 REST 模式下解释为对 HA 的一次鉴权配置探测（复用 `RetrieveHass.get_ha_config()` REST 路径：2xx 即就绪）；无令牌 local 模式沿用现有跳过逻辑。
- `/set-config` 激活失败使用 503（旧配置仍可用）属于此前不存在的新失败模式，不违反"成功响应不变"；`/configuration` 严格保持 200 HTML 现状。
- 启用 continual_publish 的配置在进程重启后自动恢复发布（这是"恢复最后一次完整激活配置"的一部分；现状"等首个 action 才启动"的行为仅对启用场景改变，禁用默认场景不变）。
- 旧代 WS 宽限期常量取 30s（数据取回均发生在 action 开头，远超覆盖在途取数所需），并以 rh retain/release 引用计数为优先关闭信号。
- 提交落盘顺序：secrets（如有）→ config.json（如有）→ params.pkl；pending 标记在提交完成后删除，崩溃恢复只认标记不认顺序。

## Acceptance Criteria

### AC-1: 保存成功即完整激活（rule）
- **Type**: `rule`
- **Given**: 服务以配置 A 运行（params.pkl/secrets/config.json 均为 A，WS 客户端身份为 A，发布状态为 A）。
- **When**: 管理员调用 `/set-config` 保存配置 B（含不同的 HA 地址、或 use_websocket 变化、或 continual_publish 启停），并收到 200。
- **Then**: 响应返回后，磁盘三文件均为 B 内容；进程内活动 params/secrets 为 B；新 action 使用 B；全局活动 WS 为 B 身份；持续发布任务与 B 一致（启停/时区/频率/目标）。
- **Pass Condition**: 上述每一项均可在测试中观测成立，且成功响应不早于任何一项完成。
- **Evidence**: `tests/test_runtime_manager.py`、`tests/test_web_server.py` 激活路径测试；代码中响应返回点位于提交与切换完成之后。

### AC-2: 新连接失败则旧配置完整服务（rule）
- **Type**: `rule`
- **Given**: 配置 A 正在服务，WS/REST 与持续发布均以 A 工作。
- **When**: 保存指向不可达/鉴权失败 HA 的配置 B（WS 模式 startup 失败，或 REST 模式 get_ha_config 返回 False）。
- **Then**: `/set-config` 返回 503；磁盘文件仍为 A（无 B 内容、无残留 tmp/pending）；活动 WS 仍为 A 且可用；旧发布任务（如启用）仍在运行；随后普通 action 仍以 A 成功执行。
- **Pass Condition**: 失败后全部 A 状态保持项逐项可验证；候选新客户端已被关闭、无额外连接残留。
- **Evidence**: 连接失败注入测试（mock startup 异常 / REST False），断言文件内容、活动客户端身份、任务数、后续 action 行为。

### AC-3: 旧 WS 连接在新连接可用后有序关闭（rule）
- **Type**: `rule`
- **Given**: 配置 A 的 WS 客户端正在被一个在途 action 使用（retain 中）。
- **When**: 配置 B 激活成功（B 的 WS 已 connect+auth）。
- **Then**: 在途 action 仍持有并使用 A 客户端完成取数；B 客户端成为活动客户端并服务新 action；A 客户端在该 rh `close()`（release）后才 shutdown，最迟不超过 30s 宽限期；任何时刻不存在"新连接未就绪先关旧连接"。
- **Pass Condition**: 引用计数为 0 立即关闭、未归零时宽限期内不关闭（可注入短宽限时间验证）；shutdown 顺序断言。
- **Evidence**: `tests/test_connection_manager.py` 新增身份切换/退役测试；`tests/test_retrieve_hass.py` retain/release 测试。

### AC-4: 在途 action 不混读、后续 action 不读旧（rule）
- **Type**: `rule`
- **Given**: 一个长 action 已进入并持有 A 的 params 与 rh。
- **When**: 激活 B 完成；随后另一个 action 进入。
- **Then**: 长 action 全程仅使用 A 的 params/连接（观测其 rh.hass_url、rh.params、WS 客户端身份均为 A）；新 action 从 params.pkl 取得 B 并经新代连接取数；不存在同一 action 内 A/B 混用。
- **Pass Condition**: 两 action 并发交错测试中各自观测身份一致。
- **Evidence**: `tests/test_web_server.py` 并发交错用例（mock set_input_data_dict 记录快照）。

### AC-5: 持续发布随活动配置切换且禁用无残留（rule）
- **Type**: `rule`
- **Given**: 配置 A 启用 continual_publish，发布任务正以 A 的 rh、time_zone、初始 freq 运行。
- **When**: 保存配置 B（场景：仅改时区/时间步长；或由启用改禁用；或由禁用改启用）。
- **Then**: 启用→启用：旧任务取消并确认退出、旧 rh 的 aiohttp session 关闭，恰好一个新任务以 B 的 rh/time_zone/freq 启动，任务总数恒为 1；启用→禁用：无发布任务存活，其 rh session 已关闭；禁用→启用：出现恰好一个任务。循环内 metadata 动态调频行为保持。
- **Pass Condition**: 任务计数、rh.session 关闭状态、新任务上下文身份可断言；`asyncio.all_tasks()` 中无遗留循环。
- **Evidence**: `tests/test_runtime_manager.py` publish reconcile 三场景测试；`tests/test_command_line_utils.py` 既有 continual_publish 循环测试保持通过。

### AC-6: 旧发布任务无法停止时不启动第二个任务（rule）
- **Type**: `rule`
- **Given**: A 的发布任务因阻塞在不可取消调用（注入：cancel 后在有界超时内不退出）。
- **When**: 保存启用发布的配置 B。
- **Then**: 激活失败（`/set-config` 503）；不创建 B 的发布任务；A 任务仍是唯一发布任务且继续运行；磁盘与活动配置保持 A；候选新连接（若有）被关闭。
- **Pass Condition**: 任务总数仍为 1 且为旧任务；无新 rh。
- **Evidence**: 注入"停不掉"任务（stop 超时）的激活失败测试。

### AC-7: 切换阶段崩溃后重启只恢复最后一次完整激活（rule）
- **Type**: `rule`
- **Given**: A 为最后一次完整激活的磁盘状态。
- **When**: 在以下时刻杀死进程并重启：①暂存写入中；②连接准备中；③提交 os.replace 序列的任意中间位置（人为预置"部分替换 + pending 标记"现场）。
- **Then**: 启动恢复后 config.json / secrets / params.pkl 内容逐项等于 A；tmp 与 pending 被清理；不存在 B 部分生效（params 为 B 而 config 为 A 等组合）；服务以 A 正常启动。无 pending 标记时启动路径与现状一致。
- **Pass Condition**: 三类崩溃现场注入后，逐文件字节/内容断言为 A；首次 action 以 A 成功。
- **Evidence**: `tests/test_runtime_manager.py` 恢复矩阵测试。

### AC-8: 序列化激活，无混合状态（rule）
- **Type**: `rule`
- **Given**: 两个保存请求几乎同时到达（B 与 C）。
- **When**: 两者均成功。
- **Then**: 最终磁盘与活动状态等于最后完成者的完整一套配置（不可能出现 B 的 secrets + C 的 params）；任一失败者不影响成功者；激活期间文件状态只有"A 完整"或"B 完整"。
- **Pass Condition**: 并发保存测试中文件三元组始终同源、活动状态一致。
- **Evidence**: 并发激活测试（asyncio.gather 两个 activate，交错点注入）。

### AC-9: 默认 REST / 仅改普通参数 / 现有契约不变（rule）
- **Type**: `rule`
- **Given**: 默认配置（REST、continual_publish=false）。
- **When**: 保存仅含普通优化参数（如电池容量、costfun 相关字段）的变更。
- **Then**: 不发起 HA 网络探测、不创建 WS 客户端、不创建任何发布任务；`/set-config` 成功响应为 200 `{}`、`/configuration` 为 200 HTML；after_serving 现有日志与 `close_global_connection` 调用行为保持；默认零配置实例可正常启动并完成一次优化。
- **Pass Condition**: 网络调用零次、任务数为 0、响应形状断言；既有测试全绿。
- **Evidence**: `tests/test_web_server.py` 现有用例保持通过；新增"普通参数保存无副作用"测试；`pytest tests/` 全绿；ruff check 通过。

### AC-10: 变更手术性与资源卫生质量（rubric）
- **Type**: `rubric`
- **Dimension**: 代码手术性与资源生命周期卫生
- **Scale**: 1-5
- **Anchors**: 1 = 大范围重构、无关模块被改动、存在任务/连接泄漏路径；3 = 核心诉求实现但有冗余抽象或个别资源路径靠 GC 回收；5 = 新增逻辑集中于运行时管理器与连接管理器的小而清晰扩展，在途/失败/关闭每条路径的任务与连接都有确定性收尾，无 bonus 功能。
- **Pass Threshold**: >= 4
- **Evidence**: 独立审查阅读 diff、模块改动行数分布、失败/关闭路径走查表。
