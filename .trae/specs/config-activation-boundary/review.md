# 配置保存即激活边界 - Independent Review

- [x] CP-R1: 保存成功即完整激活（AC-1）
  - **Type**: `rule`
  - **Covers**: AC-1, TR-5.1, TR-6.1, TR-7.3
  - **Evidence**: R4 pass。activate 仅在 commit+内存切换+WS 代际+publish start 全部完成后返回（runtime_manager.py:651-706；web_server.py:519-525）；test_full_activation_ws_identity_and_publish_swap、E2E test_activation_play 实测。

- [x] CP-R2: 新连接失败时旧配置完整服务（AC-2）
  - **Type**: `rule`
  - **Covers**: AC-2, TR-1.5, TR-5.2, TR-6.1, I-11/I-13
  - **Evidence**: R1/R4 pass。准备全部在 commit 前，失败 discard 候选+abort；提交后失败经 _restore_previous（含跨代 secrets 链）回到旧代；/set-config 映射 503。

- [x] CP-R3: 旧 WS 连接有序关闭（AC-3）
  - **Type**: `rule`
  - **Covers**: AC-3, TR-1.2, TR-1.4, TR-2.1, I-1, I-12
  - **Evidence**: R1/R4 pass。refs=0 立即关、非零 30s 宽限、新代可用后才退役旧代、close_global_connection 收全部代际；23 个 connection_manager 测试 + 钉住三分支测试。

- [x] CP-R4: 在途/后续 action 配置隔离（AC-4）
  - **Type**: `rule`
  - **Covers**: AC-4, TR-2.1, TR-6.5, I-1
  - **Evidence**: R1 发现的 rh 二次取数回退代际（F-01）已修复并实证；每 action 读 params.pkl 快照、rh 钉住代际；并发 action 交错测试通过。

- [x] CP-R5: 持续发布随配置切换且禁用无残留（AC-5）
  - **Type**: `rule`
  - **Covers**: AC-5, TR-4.1/4.2, TR-6.3/6.4, I-10
  - **Evidence**: R1/R4 pass。任务恒 1、新 rh 携带新 URL/tz/freq、禁用 all_tasks 无残留、rh session 关闭；continual_publish 循环零改动；WS 初始化失败不阻断发布 bootstrap（I-10）。

- [x] CP-R6: 旧发布停不掉不启第二个任务（AC-6）
  - **Type**: `rule`
  - **Covers**: AC-6, TR-4.3, TR-5.3
  - **Evidence**: R1/R4 pass。asyncio.wait 有界停止，超时 PublishStopError、旧任务保活、不建 B 的 rh/任务，commit 前 abort。

- [x] CP-R7: 崩溃后只恢复最后完整激活（AC-7）
  - **Type**: `rule`
  - **Covers**: AC-7, TR-3.x, I-2/I-8/I-9/I-11/I-13
  - **Evidence**: R2 的跨挂载 EXDEV（N-00）、options data_path 错位（N-01）、boot secrets 追踪（N-03）全部修复；promote_file 跨挂载安全、marker 先写、备份原子、孤儿 restore 清理、recover 先于 build_secrets；矩阵+EXDEV+链式测试。

- [x] CP-R8: 并发激活无混合状态（AC-8）
  - **Type**: `rule`
  - **Covers**: AC-8, TR-5.4
  - **Evidence**: R1/R4 pass。单 asyncio.Lock 串行；B/C 并发最终三文件同源等于最后完成者。

- [x] CP-R9: 默认 REST / 普通参数 / 既有契约不变（AC-9）
  - **Type**: `rule`
  - **Covers**: AC-9, TR-5.5, TR-6.x, TR-7.1/7.2, I-3/I-5
  - **Evidence**: R1/R4 pass。普通参数零网络/零任务（WS 同身份亦不建连）；200 {}/200 HTML/400/500/503 契约逐一核对；after_serving 日志文案恢复且无条件关连接；全量 1679 passed、ruff 全过。

- [x] CP-U1: 手术性与资源卫生（AC-10）
  - **Type**: `rubric`
  - **Covers**: AC-10, TR-7.4, 全部 issues
  - **Scale**: 1-5
  - **Anchors**: 1 = 大范围重构/无关模块被改/有泄漏路径；3 = 诉求达成但有冗余或靠 GC 收尾；5 = 改动集中、每条失败/关闭路径确定性收尾、无 bonus 功能
  - **Pass Threshold**: >= 4
  - **Evidence**: R4 评分 **4/5**。src 侧仅 3 改 1 新（connection_manager +242/-35、retrieve_hass +55/-11、web_server +136/-62、新增 runtime_manager 827 行）；optimization.py/forecast.py/command_line.py/param_definitions.json 零改动、无新依赖/参数/前端字段；扣分为两个接受的 low advisory（纠正性回滚期间双崩溃的极端收敛、runner 仅在 tz/freq/identity/启停变化时重建——均符合 spec 明文范围）。

## Review History

### Review R1
- **Result**: fail
- **Evidence**: 1 high（F-01 rh 二次取数回退 WS 代际，动态实证）+ 5 med（F-02 恢复顺序、F-03 costfun 覆盖、F-04 ENV secrets 固化、F-05 同身份建连、F-06 关闭漏退役代际）+ advisories（F-07 异常包装、F-08 取消清理、F-09 备份原子/顺序、F-10 死代码/锁、F-11 接受、F-12 manager 构造时机）。修复记录见 tasks.md Issue I-1..I-7；1672 passed。

### Review R2
- **Result**: fail
- **Evidence**: R1 项 9 fixed/3 partial；新发现 N-00(high，跨挂载恢复 EXDEV，注入实证)、N-01(med，options data_path 恢复错位，动态实证)、N-02(low，WS 初始化失败阻断发布)、N-03(low，boot 不追踪 secrets_file)；N-04..N-08 advisory。修复记录见 I-8..I-12；1676 passed。

### Review R3
- **Result**: fail（范围窄）
- **Evidence**: N-00..N-08 全部 fixed；新发现 R3-N1(low，secrets 追踪未跨激活链传播导致链式回滚 yaml 残留，双变体动态实证)、R3-N2(advisory，restore 孤儿临时文件)、R3-N3(advisory 接受，served/reconnect TOCTOU 微窗)。修复记录见 I-13；新增 3 回归测试。

### Review R4
- **Result**: **pass — 无阻塞，可交付**
- **Evidence**: R3-N1 fixed（传播 previous 代 secrets_file/existed；absent/present 链式回归测试通过，多代链推演成立）；R3-N2 清理有效；R3-N3 接受合理。AC-1..AC-9 全部 pass、CP-U1 4/5。290 个相关测试通过；全量 `uv run pytest tests/` = **1679 passed, 32 xfailed, 370 subtests, 0 failed**；`uvx ruff check .` All checks passed。独立审查者亲跑复核并签字。
- **Residual advisories（接受，不阻塞）**:
  1. 纠正性回滚期间再次被杀（双故障、超出 AC-7 崩溃矩阵；磁盘三文件仍自洽）。
  2. 发布 runner 仅在 spec 明列的启停/身份/tz/freq 变化时重建；method_ts_round 等标量保存后沿用到下次相关变更（相对基线"永不更新"无回退，spec Goal 未要求）。
