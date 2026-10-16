# 配置保存即激活边界 - 实施计划

## Task 1: connection_manager 身份感知与代际退役
- **Status**: `completed`
- **Completion Evidence**:
  - TR-1.1: 既有 16 用例零修改通过（`pytest tests/test_connection_manager.py` → 16 passed）。
  - TR-1.2/1.3/1.4/1.5: 新增 7 用例（身份切换立即退役、retain/release、30s 宽限强制关闭（注入 0.01s）、新代失败旧代存活、prepare/discard/activate、同身份复用、retire_active+全局关闭）全部通过（合计 23 passed）。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 扩展 [connection_manager.py](file:///f:/swe/098853/project-03/src/emhass/connection_manager.py)：活动客户端持有"身份"（hass_url、token 归一化后）与引用计数。
  - `get_websocket_client(url, token, logger=None)` 保持签名与现有契约：无客户端→创建；同身份→复用/断线重连；**身份不同→创建并启动新一代，旧代进入退役流程**；启动失败时活动槽重置为 None（保持现有测试断言）。
  - 新增 `prepare_client(url, token, logger)`：创建并 startup 一个客户端但**不安装**为活动代，返回持有对象；`discard_client(holder)` 关闭候选；`activate_prepared(holder)`：原子安装为活动代，旧代转入退役。
  - 新增 `retain(client)` / `release(client)`：退役代引用为 0 时立即 shutdown；否则由后台看护任务在宽限期（常量 `RETIRE_GRACE_SECONDS=30`）后强制 shutdown；关闭异常吞掉并记录（与现状一致）。
  - 新增 `retire_active()`：供"WS 被禁用"使用，活动代转退役、活动槽置空。
  - `close_global_connection()` 立即关闭活动代与所有退役代（服务关闭路径，不等待宽限）；`is_connected()` 反映活动代。
  - 保留模块级 `_global_client` 名称（指向活动客户端或 None），现有测试不修改即通过。
- **Acceptance Criteria Addressed**: AC-2, AC-3
- **Test Requirements**:
  - `rule` TR-1.1: 现有 `tests/test_connection_manager.py` 全部用例零修改通过（证据：pytest 输出）。
  - `rule` TR-1.2: 不同身份调用时返回新客户端、旧客户端 shutdown 被延迟到 release 之后；无 retain 时在注入的短宽限期常量下关闭（证据：新用例断言调用顺序与时刻）。
  - `rule` TR-1.3: `prepare_client` 不改变活动槽与 `is_connected()`；`discard_client` 关闭候选且活动状态不变（证据：新用例）。
  - `rule` TR-1.4: `close_global_connection` 后活动代与退役代均被 shutdown、`_global_client is None`（证据：新用例）。
  - `rule` TR-1.5: 身份不同且新客户端 startup 失败时抛异常，旧活动代仍存在且可用（证据：新用例）。

## Task 2: RetrieveHass 对 WS 客户端的 retain/release 挂钩
- **Status**: `completed`
- **Completion Evidence**:
  - TR-2.1: `test_ws_client_retained_once_released_on_close`（两次取数 retain 一次、close 恰好 release 一次、close 幂等）通过。
  - TR-2.2: `test_rest_mode_close_does_not_touch_ws_manager` 通过。
  - TR-2.3: `tests/test_retrieve_hass.py` 全量 78 用例通过（76 既有 + 2 新增）。
- **Priority**: high
- **Depends On**: Task 1
- **Description**:
  - 在 [retrieve_hass.py](file:///f:/swe/098853/project-03/src/emhass/retrieve_hass.py) 中以最小改动挂钩引用计数：构造时增加 `self._ws_retained = False`（仅 ws 模式相关）；新增私有 async 辅助（约 6-10 行）统一取客户端并对"首次取得的客户端"retain 一次；`get_ha_config_websocket` 与 `get_data_websocket` 两处改走该辅助。
  - `close()` 在关闭 HTTP session 的同时，若 ws 模式且持有已 retain 的客户端则 `release`（幂等，异常不外溢）。
  - 不改取数/回退 REST/发布逻辑，不改构造函数签名。
- **Acceptance Criteria Addressed**: AC-3, AC-4
- **Test Requirements**:
  - `rule` TR-2.1: 一个 rh 生命周期内多次取数只 retain 一次、`close()` 恰好 release 一次（证据：mock manager 调用计数断言）。
  - `rule` TR-2.2: REST 模式 rh 的 `close()` 不触碰 WS 管理器（证据：断言无 manager 调用）。
  - `rule` TR-2.3: `tests/test_retrieve_hass.py` 全部既有用例通过（证据：pytest 输出）。

## Task 3: 激活持久化原语（stage / commit / recover）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-3.1: `test_stage_then_commit_promotes_everything` 通过（暂存不改活动文件、commit 后三文件更新、暂存区与同目录 tmp 全清）。
  - TR-3.2: `test_recover_restores_partial_commit_with_pending_marker`、`test_recover_removes_absent_destinations` 通过（部分替换+标记→还原 A；首次激活崩溃→删除 B 新文件）。
  - TR-3.3: `test_recover_without_marker_leaves_destinations_alone` 通过。
  - TR-3.4: `test_commit_failure_rolls_back_and_raises` 通过（commit 中途异常→尽力还原+保留 marker 供重启幂等恢复）。共 7 用例全绿。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在新文件 `src/emhass/runtime_manager.py` 中实现纯文件原语（暂不引入网络/任务逻辑）：
    - 工作目录 `data_path/.activation/`，含 `pending.json`、`staging/`、`rollback/`；pending 内容记录 generation、目标文件清单（dest 绝对路径、是否原已存在、临时文件与回滚文件相对名）。
    - `stage(file_specs)`：对每个目标（secrets/config/params 子集）先把现有文件字节复制到 rollback（不存在则记录 absent），再把新内容写到**目标同目录**的 `<name>.tmp.<gen>`（跨挂载安全，aiofiles 写 + flush），最后写 pending.json。
    - `commit()`：按固定顺序（secrets → config.json → params.pkl）`os.replace` 临时文件到目标，完成后删除 pending 与 rollback/staging；中途替换失败抛 `ActivationCommitError` 并立即用 rollback 做尽力还原。
    - `abort_discard()`：删除临时文件与 pending（rollback 不动活动文件）。
    - `recover_pending(logger)`：启动时调用——存在 pending 则逐文件还原（有备份→replace 还原；absent→删除目标），清理所有 tmp/暂存目录；无 pending 仅做孤儿 tmp/目录清理。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-3.1: stage 不改目标文件内容；commit 后目标为新内容且暂存区清空（证据：临时目录 + 字节断言）。
  - `rule` TR-3.2: 预置"部分 os.replace 已发生 + pending 存在 + rollback 完整"现场，`recover_pending` 后每个目标还原为旧内容；absent 项的目标被删除（证据：恢复矩阵逐文件断言，含 secrets/config/params 三文件 8 种组合中的代表性组合）。
  - `rule` TR-3.3: 无 pending 时 `recover_pending` 不动任何目标文件（证据：普通启动场景用例）。
  - `rule` TR-3.4: commit 中途注入异常后，函数抛出且活动文件被尽力还原为旧内容（证据：mock os.replace 第三次调用抛错）。

## Task 4: PublishManager（持续发布唯一属主）
- **Status**: `completed`
- **Completion Evidence**:
  - TR-4.1: `test_reconcile_enabled_to_enabled_swaps_runner`（新 rh 携带新 URL/时区/5min 频率、任务恒 1 个、旧 rh 关闭）通过。
  - TR-4.2: `test_disable_stops_task_and_closes_rh`、`test_enable_starts_when_disabled` 通过；任务名 `emhass-continual-publish` 便于 all_tasks 扫描。
  - TR-4.3: `test_unstoppable_task_blocks_new_start` 通过（基于 asyncio.wait 的有界停止：吞掉 cancel 的任务保活并抛 PublishStopError，无第二个 rh/任务）。
  - TR-4.4: test_command_line_utils continual/publish_json 既有用例 4 个通过；`continual_publish` 循环零改动。runtime_manager ruff 零告警。共 12 用例全绿。
- **Priority**: high
- **Depends On**: None
- **Description**:
  - 在 `src/emhass/runtime_manager.py` 中实现 `PublishManager`：
    - `build_context(params, emhass_conf, logger)`：用 `utils.get_yaml_parse` 得到 retrieve_hass_conf，直接构造 `RetrieveHass(url, token, optimization_time_step, time_zone, params, emhass_conf, logger)`，组装精简上下文 `{"rh": rh, "retrieve_hass_conf": retrieve_hass_conf}`（`continual_publish`/`publish_json` 实际只访问这两个键）。
    - `start(params)`：断言无运行中任务；用 `asyncio.create_task` 运行包装协程：`try: await continual_publish(ctx, entity_path, logger) except CancelledError: pass finally: await rh.close()`；保存 task 与 rh 引用。
    - `stop(timeout=10s)`：cancel + `asyncio.wait_for` 等待退出；正常退出/取消后 rh session 确认关闭；超时抛 `PublishStopError`，不杀任务、不吞异常。
    - `reconcile(params)`：按 `retrieve_hass_conf.get("continual_publish", False)` 收敛——运行中且应停用→stop；运行中且仍启用→stop 旧 + start 新（任何切换都重建以刷新时区/频率/目标/rh）；未运行且应启用→start；未运行且应停用→no-op。提供"只停旧不启新"的分步接口供 activate 编排失败回滚。
    - `shutdown()`：服务关闭路径，stop（超时也强制走到 rh.close 收尾）。
    - 查询接口：运行状态、当前任务、当前 rh，供测试与诊断。
  - `command_line.py::continual_publish` 循环体零改动（其 CancelledError 透传语义已满足停止要求）。
- **Acceptance Criteria Addressed**: AC-5, AC-6
- **Test Requirements**:
  - `rule` TR-4.1: 启用→启用 reconcile：旧任务被取消并退出、旧 rh.session 已关闭，恰好一个新任务运行且其 rh/time_zone/初始 freq 来自新 params（证据：任务身份与上下文断言、asyncio 任务计数）。
  - `rule` TR-4.2: 启用→禁用：无该循环任务残留（`all_tasks` 扫描）、rh 关闭；禁用→启用：恰好一个任务（证据：三场景用例）。
  - `rule` TR-4.3: stop 注入超时（任务忽略 cancel）→ 抛 `PublishStopError`，旧任务仍存活、未创建任何新任务/rh（证据：AC-6 对应用例）。
  - `rule` TR-4.4: `tests/test_command_line_utils.py` 中 continual_publish 相关既有用例全部通过（证据：pytest 输出）。

## Task 5: RuntimeManager.activate 编排与活动状态
- **Status**: `completed`
- **Completion Evidence**:
  - TR-5.1: `test_full_activation_ws_identity_and_publish_swap`（prepare→stop→commit→activate→start 顺序与新身份落盘/回调）通过。
  - TR-5.2: `test_ws_prepare_failure_keeps_old_config_and_publisher`、`test_rest_probe_failure_keeps_old_config`（连接失败 503 类错误、活动状态 A、无暂存残留、旧发布不动、探测 rh 已关闭）通过。
  - TR-5.3: `test_unstoppable_publish_aborts_without_new_runner` 通过。
  - TR-5.4: `test_concurrent_activations_end_in_one_consistent_generation`（锁串行、在途时文件仍 A、最终三文件同源）通过。
  - TR-5.5: `test_ordinary_params_save_has_no_connection_or_publish_side_effects`（探测/WS/发布零调用）通过。
  - TR-5.6: `test_secrets_patch_does_not_rewrite_config_file` 通过。runtime_manager + 测试 ruff 全过；共 21 用例全绿。
- **Priority**: high
- **Depends On**: Task 1, Task 3, Task 4
- **Description**:
  - 在 `src/emhass/runtime_manager.py` 实现 `RuntimeManager`：
    - 活动状态 `ActiveConfig(params, secrets, config, generation)`；单一 `asyncio.Lock` 串行激活；模块级单例由 web_server 初始化时构造（传入 emhass_conf、logger 取值器、entity_path 取值器）。
    - `activate(config=None, secrets_patch=None)` 流程（全程持锁）：
      1. 合并候选：secrets = 活动 secrets 更新 patch；config = 传入 config 或活动 config；`build_params` 构建候选 params（失败返回既有 500/400 语义由端点处理，管理器抛 `ActivationValidationError`）；补上 optim_conf.costfun/logging_level 与现状一致的处理（沿用初始化值，不改变现有行为）。
      2. 判定差异：`conn_identity_changed`（hass_url/token）、`ws_mode_changed`（use_websocket）、`publish_enabled` 候选值；三者皆无且无 secrets patch → 轻量路径：stage+commit params/config/secrets（实际只有变化的文件）、替换活动状态，无任何网络与发布重启。
      3. stage（FR-7）；随后准备连接：候选 ws 开启→`prepare_client`（失败/超时→abort+抛 `ActivationConnectionError`）；候选 REST 且身份变化→一次性 rh 调 REST `get_ha_config()`（False→关闭候选连接、abort、抛连接错误；无令牌 local 模式跳过）。
      4. 发布切换准备：若发布在运行，先 `stop`（`PublishStopError`→关闭候选连接、abort、抛出，旧任务保持运行）。
      5. commit（FR-7；失败→重启旧发布（若此前在运行）、关闭候选连接、抛 `ActivationCommitError`）。
      6. 切换内存：更新活动 ActiveConfig；经回调更新 `web_server.params_secrets`；WS：候选→`activate_prepared`；ws 关闭→`retire_active`；身份未变→不动；REST 无全局资源。
      7. 发布：候选启用→`start`（start 失败属本地异常，执行回滚：还原文件、恢复活动状态、重启旧发布、抛错——测试以注入覆盖）。
      8. 返回成功摘要（供日志，不进响应体）。
    - `startup_recover()`：recover_pending → 返回；`set_active(params, secrets, config)`；`bootstrap_publish()` 与 `shutdown()`。
    - 所有异常类型集中定义（`ActivationError` 基类带 kind），错误消息为英文短句、经 app.logger 记录。
- **Acceptance Criteria Addressed**: AC-1, AC-2, AC-6, AC-8, AC-9
- **Test Requirements**:
  - `rule` TR-5.1: 全成功路径结束后：活动状态=候选、文件已提交、WS 代际已换、发布按候选运行，且成功返回发生在全部步骤之后（证据：用 spy 记录调用完成顺序）。
  - `rule` TR-5.2: WS startup 异常 / REST 探测 False 两条失败路径：活动状态与文件保持旧值、无 pending/tmp、旧发布仍在运行、候选客户端已 shutdown（证据：AC-2 用例）。
  - `rule` TR-5.3: publish stop 超时路径：无新任务、活动状态与文件为旧、`/set-config` 对应错误可映射（证据：AC-6 用例）。
  - `rule` TR-5.4: 并发两个 activate（gather）后三文件同源且等于后完成者；过程中任意 await 点观测文件三元组只有"全 A"或"全 B"（证据：并发交错用例 + 内容指纹）。
  - `rule` TR-5.5: 普通优化参数激活路径中 prepare_client/探测/publish start/stop 调用次数均为 0（证据：mock 调用计数断言）。
  - `rule` TR-5.6: `/configuration` 风格的 secrets_patch 激活后，重建的 params 中 retrieve_hass_conf.hass_url/token 为新值，且活动 config.json 不被重写（证据：断言）。

## Task 6: web_server 端点与生命周期接线
- **Status**: `completed`
- **Completion Evidence**:
  - TR-6.1: `test_parameter_set_activation_failure_returns_503`（503 + 字符串列表）；既有 heat_topology/curve 400 与 200 用例零修改通过（62 passed）。
  - TR-6.2: `test_configuration_secrets_activate_on_post`、`test_configuration_still_renders_when_activation_fails`（成功/失败均 200 HTML，失败走日志）通过。
  - TR-6.3: `test_action_with_publish_enabled_does_not_start_background_task`（不 add_background_task、rh 关闭）通过。
  - TR-6.4: `test_after_serving_stops_continual_publish` + 既有 after_serving 两分支用例通过。
  - TR-6.5: `test_concurrent_actions_use_their_entry_time_params`（A/B 各自快照、各自 rh 关闭）通过。web_server ruff 全过。
- **Priority**: high
- **Depends On**: Task 2, Task 5
- **Description**:
  - [web_server.py](file:///f:/swe/098853/project-03/src/emhass/web_server.py) 手术式接线：
    - `/set-config`：保留全部既有校验（heat_topology、inverter curve、build_params 前置）；把现有"截断写 config.json + 二次 build_params 写 params.pkl"两段替换为 `runtime.activate(config=merged_config)`；捕获 `ActivationError` → `make_response([str(exc)], 503)`；成功仍返回 `({}, 200)`；500 类路径语义不变。
    - `/configuration` POST：表单→secrets_patch（保持 `***` 掩码跳过、只更新提交键、先读旧 secrets 合并的现有逻辑）；调用 `runtime.activate(secrets_patch=patch)`；失败仅 `app.logger.error`；无论成败继续渲染 configuration.html（200），行为与现状一致。
    - `initialize()`：路径/secrets/config 构建之后、connections 初始化之前调用 `runtime.startup_recover()` 与 `set_active(params, params_secrets, config)`；初始化末尾按活动配置 `bootstrap_publish()`（启用才启动）；WS 初始化保持 best-effort（失败告警、继续服务）。
    - `after_serving()`：在现有 WS 关闭逻辑前增加 `runtime.shutdown()`（停发布、收 rh）；保留现有 is_connected/close_global_connection 分支与日志文本。
    - `action_call`：删除懒启动块与 `rh_handed_to_thread` 移交逻辑，`finally` 中对存在 `"rh"` 的 input_data_dict 一律 `await rh.close()`；其余分发逻辑不动。
    - 提供测试用重置钩子（如 `runtime` 单例的 init/reset 函数），供测试套件隔离。
- **Acceptance Criteria Addressed**: AC-1, AC-4, AC-5, AC-9
- **Test Requirements**:
  - `rule` TR-6.1: `/set-config` 激活成功 200 `{}`；管理器抛 `ActivationError` 时响应 503 且 body 为字符串列表；既有 heat_topology/curve 400 用例零修改通过（证据：pytest 输出）。
  - `rule` TR-6.2: `/configuration` 在激活成功与失败时均返回 200 且渲染 configuration.html；失败时 params.pkl/secrets 文件未变（证据：端点测试）。
  - `rule` TR-6.3: `action_call` 不再创建发布任务；带 `continual_publish: true` 的 mocked action 请求后 `PublishManager` 任务数为 0（管理器未启用时）且请求 rh 被 close（证据：web_server 测试）。
  - `rule` TR-6.4: `after_serving` 现有两条分支用例保持通过，并额外断言发布任务被停止（证据：pytest 输出+新断言）。
  - `rule` TR-6.5: 在途 action 与新进入 action 的交错测试：各自 rh 身份（URL/token/WS 代）与进入时配置一致（证据：AC-4 用例）。

## Task 7: 全量回归、lint 与新增集成测试
- **Status**: `completed`
- **Completion Evidence**:
  - TR-7.1: `uv run pytest tests/` → **1667 passed, 32 xfailed, 370 subtests passed, 0 failed**（含新增 runtime_manager 22 用例）；UI DOM 测试 `node --test tests/ui/*.cjs` → 66 passed。
  - TR-7.2: `uvx ruff check .` → All checks passed（含 B017/I001 修正）。
  - TR-7.3: `TestActivationEndToEnd.test_activation_play` 通过（真实 build_params/associations/defaults + 临时多目录：启用发布→换 URL（探测成功，runner 重建）→坏 URL 被拒旧配置继续→禁用发布无残留→启动恢复检查）。
  - TR-7.4（rubric，自评 5/5）：
    - **diff 分布**（EOL 已归一为仓库 blob 的 LF，numstat 为真实改动）：connection_manager +231/-36；retrieve_hass +39/-11；web_server +68/-57；新增 runtime_manager（479 行）；测试：connection_manager +163/-2、retrieve_hass +32、web_server +144/-2、inverter fixture +33/-13、新增 test_runtime_manager 785 行。optimization.py/forecast.py/command_line.py/param_definitions.json 零改动。
    - **失败/关闭路径走查表**：①WS prepare 失败→discard 候选+abort，旧代在位；②REST 探测 False→关探测 rh+abort；③发布 stop 超时（asyncio.wait 有界）→不启动新任务、abort、旧任务继续；④commit 异常→文件回滚+保留 pending+重启旧发布/恢复旧 WS（best-effort）；⑤publish start 失败→反向 stage/commit 回滚全套；⑥进程崩溃→pending 标记启动恢复（已存在→还原，缺失→清理孤儿）；⑦after_serving→先停发布收 rh，再关所有 WS 代际。
    - 无 bonus 功能：未新增参数/依赖/前端字段；默认 REST、默认不发布、普通参数保存零网络/零任务副作用均有用例锁定。

## Issue I-1: rh 必须钉住首次取得的 WS 代际（F-01，Review R1，high）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 1, Task 2
- **Discovered By**: Review R1
- **Description**:
  - 旧 rh 在激活后第二次取数仍按身份调用 get_websocket_client，会新建旧身份 A2、回退活动槽并关闭新代 B（实证）。
  - 修复：retrieve_hass `_get_active_ws_client` 一旦 retain 即复用同一 client；钉住期间断连只对该 client reconnect（失败回退 REST），永不重新解析身份。
- **Acceptance Criteria Addressed**: AC-1, AC-3, AC-4
- **Test Requirements**:
  - `rule` TR-I-1.1: 新增 `test_ws_generation_is_pinned_for_rh_lifetime`（A retain→B 激活→二次取数仍 A、无第三个连接、B 保持活动、close 后 A 关 B 不关）通过。
  - `rule` TR-I-1.2: `test_get_data_websocket` 断连场景改为 pinned reconnect 失败→REST 回退，通过。
- **Completion Evidence**: 两测试通过；tests/test_retrieve_hass.py 全量 80 用例通过。

## Issue I-2: 崩溃恢复必须先于 secrets 读取；manager 在路径校验后构建（F-02/F-12，Review R1，med）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 6
- **Discovered By**: Review R1
- **Description**:
  - initialize 原本先 build_secrets 后 recover，secrets 已 promote 的崩溃现场会用 B 的 yaml 重建出混合 params。
  - 修复：`_setup_paths` 后立即 `ActivationStore.recover_pending()`，再 build_secrets；RuntimeManager 移到 `_validate_data_path` 之后构造。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-I-2.1: `test_recovery_before_build_secrets_restores_old_identity`（secrets 已 promote+marker→recover→build_secrets 得 A 身份）通过。
- **Completion Evidence**: 用例通过；initialize 顺序经走查确认。

## Issue I-3: costfun/logging_level 热切换不得被旧活动态覆盖（F-03，Review R1，med）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Discovered By**: Review R1
- **Description**: 删除 activate 中把 previous 的 costfun/logging_level 强制写回候选的代码块，与基线 /set-config 行为一致。
- **Acceptance Criteria Addressed**: AC-8, AC-9
- **Test Requirements**:
  - `rule` TR-I-3.1: `test_costfun_change_takes_effect_immediately`（活动态与 params.pkl 立即为新值）通过。

## Issue I-4: secrets 文件只持久化文件作用域键（F-04，Review R1，med）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5, Task 6
- **Discovered By**: Review R1
- **Description**:
  - activate 增加 `secrets_file`（yaml 精确内容）；端点传入"磁盘原 yaml ∪ 表单键"；内存合并 secrets 仍含 ENV/options 全量。回滚按 previous.secrets_file 还原；提交顺序改为 secrets→config→params。
- **Acceptance Criteria Addressed**: AC-1, AC-7
- **Test Requirements**:
  - `rule` TR-I-4.1: `test_secrets_yaml_excludes_env_derived_values`（ENV 派生键不落盘）通过；端点 kwargs 断言通过。

## Issue I-5: WS 同身份保存不得发起网络（F-05，Review R1，med）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: Task 5
- **Discovered By**: Review R1
- **Description**: prepare 条件收紧为 `ws_will and (identity_changed or not ws_was)`；同身份断连交给 action 路径懒重连。
- **Acceptance Criteria Addressed**: AC-9
- **Test Requirements**:
  - `rule` TR-I-5.1: `test_ws_same_identity_ordinary_save_makes_no_connection`（prepare/activate/retire 零调用）通过。

## Issue I-6: 关闭无条件化、异常包装、取消清理、备份原子化、死代码（F-06/F-07/F-08/F-09/F-10，Review R1，low~med）
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: Task 1, Task 4, Task 5
- **Discovered By**: Review R1
- **Description**:
  - after_serving 无条件 close_global_connection（含退役宽限代际）；测试同步更新。
  - stage OSError 包装 ActivationCommitError；探测构建异常包装 ActivationValidationError；提交后交换步骤整体 try/except BaseException 失败回滚；准备阶段取消/异常统一 discard 候选+abort。
  - rollback 备份改为临时文件+os.replace 原子就位。
  - 删除生产无调用方的 PublishManager.reconcile；shutdown 持锁。
- **Acceptance Criteria Addressed**: AC-2, AC-5, AC-7, AC-10
- **Test Requirements**:
  - `rule` TR-I-6.1: after_serving 两分支均断言 close 调用；全量回归无新增挂起/泄漏任务；ruff 全过。
- **Completion Evidence**: 见 Task 7 全量结果。

## Issue I-7: advisory 项处理决定
- **Status**: `completed`
- **Priority**: low
- **Discovered By**: Review R1
- **Description**: F-11（锁内同步小文件 IO）接受——文件均为 KB 级配置，暂不引入线程池；不阻塞 NFR-5。

## Issue I-8: 恢复/回滚跨挂载安全（N-00，Review R2，high）
- **Status**: `completed`
- **Priority**: high
- **Discovered By**: Review R2
- **Description**: rollback 位于 data_path，目标可能在 /app、/share 挂载；恢复/进程内回滚改用 promote_file（copyfile 到目标同目录临时文件→同目录 os.replace）；逐文件 OSError 计数并保留 marker 抛 ActivationCommitError，杜绝静默混合启动。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-I-8.1: `test_recovery_works_when_rollback_is_on_another_mount`（注入跨父目录 EXDEV，仍完整还原 A）通过。

## Issue I-9: options.json data_path 重定向前置于恢复（N-01，Review R2，med）
- **Status**: `completed`
- **Priority**: high
- **Discovered By**: Review R2
- **Description**: 新增 `_apply_options_data_path`，在 initialize 恢复前应用 options.json 的 data_path 重定向（build_secrets 幂等再应用）；RuntimeManager 本就在路径终态后构造。
- **Acceptance Criteria Addressed**: AC-7
- **Test Requirements**:
  - `rule` TR-I-9.1: `test_apply_options_data_path_redirects_before_recovery`（自定义/default/缺失/损坏 4 情形）通过。

## Issue I-10: WS 初始化失败不阻断发布 bootstrap（N-02，Review R2，low）
- **Status**: `completed`
- **Priority**: medium
- **Discovered By**: Review R2
- **Description**: initialize 中 _initialize_connections 包 try/except 告警后继续，bootstrap_publish 始终执行（发布走 REST，不依赖 WS 就绪）。
- **Acceptance Criteria Addressed**: AC-5

## Issue I-11: boot 代追踪 secrets 文件存在性，提交后失败可删/还原 yaml（N-03，Review R2，low）
- **Status**: `completed`
- **Priority**: medium
- **Discovered By**: Review R2
- **Description**: ActiveConfig 增加 secrets_file_existed；initialize 读 yaml（存在则内容+True，缺失则 None+False）；_restore_previous 按存在性还原内容或直接 unlink。
- **Acceptance Criteria Addressed**: AC-2, AC-7
- **Test Requirements**:
  - `rule` TR-I-11.1: `test_post_commit_failure_removes_secrets_file_absent_at_boot`、`test_post_commit_failure_restores_previous_secrets_file` 通过。

## Issue I-12: 代际 served 检查与日志文案（N-04/N-08，Review R2，advisory）
- **Status**: `completed`
- **Priority**: low
- **Discovered By**: Review R2
- **Description**: connection_manager 增加 is_generation_served；rh 钉住代际断连时若已被强制关闭则不重开无主 socket（抛错→REST 回退），被追踪则同代 reconnect；after_serving 恢复原日志文案（仍无条件关闭）。N-05（探测超时）、N-06（换槽微窗）、N-07（幂等双关）接受为 advisory。
- **Test Requirements**:
  - `rule` TR-I-12.1: 钉住代际"追踪中断连 reconnect/已退役拒重开/正常复用"三分支测试通过。

## Issue I-13: secrets 追踪跨激活链传播与 restore 残留清理（R3-N1/N2，Review R3，low）
- **Status**: `completed`
- **Priority**: medium
- **Discovered By**: Review R3
- **Description**:
  - R3-N1：非 secrets 激活构造新 ActiveConfig 时把 secrets_file_existed 硬置 True/None，导致"boot→普通 set-config→secrets 激活提交后失败"链回滚时 yaml 残留 B。修复：无 secrets_patch 的激活传播 previous 代的 secrets_file/secrets_file_existed。
  - R3-N2（advisory）：recover_pending 对每个已知目标 glob 清理 `*.restore.*` 孤儿临时文件。R3-N3（served/reconnect TOCTOU 微窗）接受为 advisory。
- **Acceptance Criteria Addressed**: AC-2, AC-7
- **Test Requirements**:
  - `rule` TR-I-13.1: `test_chained_rollback_after_intermediate_setconfig_absent_yaml`、`..._restores_yaml`（链式回滚删除/还原 yaml）通过。
  - `rule` TR-I-13.2: `test_recovery_removes_orphan_restore_temp` 与 EXDEV 测试的孤儿断言通过。

## Task 7（原始描述存档）
- **Priority**: high
- **Depends On**: Task 6
- **Description**:
  - 补齐跨模块集成测试：完整一次"REST 默认启动 → set-config 换 URL（探测成功）→ 新 action 用新 URL → 再停用/启用发布 → 模拟崩溃现场 → recover 后启动"剧本（全部 mock HA）。
  - 运行 `pytest tests/` 全量通过；`uvx ruff check .` 零新增告警；Windows 本地可运行（os.replace 同目录临时文件策略已覆盖跨盘）。
  - 自查 AGENTS.md 不变量：action_logs 行格式未动；未加 logger handler；param_definitions.json 未动；opt_res 列未动；默认 REST/未启用发布行为不变。
- **Acceptance Criteria Addressed**: AC-1..AC-10
- **Test Requirements**:
  - `rule` TR-7.1: `pytest tests/` 全绿（证据：完整输出摘要，含既有与新增用例数）。
  - `rule` TR-7.2: `uvx ruff check .` 通过（证据：命令输出）。
  - `rule` TR-7.3: 端到端剧本测试通过，覆盖激活成功、失败回退、发布切换、崩溃恢复四段（证据：新集成用例）。
  - `rubric` TR-7.4: 手术性与资源卫生走查；维度同 AC-10；1-5；锚点 1=无关模块被改/有泄漏路径，3=诉求达成但有冗余，5=改动集中且每条失败/关闭路径资源收尾确定；阈值 >=4；证据：diff 文件分布清单与失败路径走查表（写在完成证据中）。
