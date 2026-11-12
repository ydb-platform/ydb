# Тесты с неэффективной нагрузкой на CPU (CI)

Зафиксировано по метрикам `report` (`ru_utime+ru_stime` / duration чанка) и `cgroup`
(`resources_monitor` `kind=test` → `cpu_used_cores`) + разбору кода.

Источник: [PR-check 41071947594](https://storage.yandexcloud.net/ydb-gh-logs/ydb-platform/ydb/PR-check/41071947594/ya-x86-64/try_1/tests_metrics/dashboard.html)
(сортировка по `cgroup_max / report_max`).

`REQUIREMENTS(cpu)` — слот планировщика, **не** квота на потоки внутри процесса.

Два разных артефакта ratio:
- **25–30×** — часто report = только клиент, cgroup = клиент+кластер (recipe).
- **1.1–3×** — обычно peak vs mean на уже учтённой работе (оба счётчика видят burn).

Caveats учёта:
- Dashboard `recommended_cpu` часто **MEDIUM/SMALL-cap → 4/1**, даже если `base_tier` 16–48 (groupinfo, actors/util/benchmark).
- В `resources_monitor` пути `library/*` без префикса `ydb/` (report — с `ydb/`).
- «Hot wall» ниже = чанк с max **report cores** (не max `cores×dur`).

---

## Сводная таблица (41071947594)

| Suite | ya | SIZE | hot wall | REPORT p50/p95/max | CGROUP p50/p95/max | ratio | вердикт | sum wall | chunks | RAM max | note |
|-------|---:|:-----|---------:|--------------------|--------------------|------:|:--------|---------:|-------:|--------:|:-----|
| `tests/functional/kqp/kqp_indexes` | ∅ | MEDIUM | 150s | 0.35/0.35/0.35 | 4.1/7.3/10.6 | **30.2×** | да | 150s | 1 | 8.9G | recipe |
| `tests/functional/kqp/kqp_query_svc` | ∅ | MEDIUM | 45s | 0.39/0.39/0.39 | 2.4/9.3/10.2 | **25.8×** | да | 45s | 1 | 11.8G | recipe |
| `library/actors/core/ut` | 4 | MEDIUM | 45s | 0.55/3.7/5.3 | 1.7/7.4/19.8 | **3.8×** | да | 255s | 10 | 0.2G | MEDIUM cap |
| `library/nmslib/test/run_test_integr` | ∅ | MEDIUM | 144s | 3.5/3.5/3.5 | 3.1/6.4/11.7 | **3.4×** | да | 144s | 1 | 0.4G | |
| `library/actors/util/ut` | ∅ | SMALL | 15s | 4.4/4.4/4.4 | 2.5/11.0/12.2 | **2.8×** | нет | 15s | 1 | 0.1G | SMALL cap |
| `tests/functional/nbs` | 4 | MEDIUM | 151s | 2.9/4.4/4.5 | 2.5/8.9/12.5 | **2.7×** | да | 356s | 3 | 11.9G | MEDIUM cap |
| `tests/functional/sqs/migration/topic_creation/large` | ∅ | SMALL | 60s | 0.93/4.4/4.9 | 3.1/8.8/12.0 | **2.5×** | partial | 248s | 10 | 11.7G | SMALL cap |
| `tests/functional/sqs/large` | 4 | MEDIUM | 62s | 0.91/4.3/4.6 | 3.2/8.0/10.2 | **2.2×** | partial | 249s | 10 | 11.6G | MEDIUM cap |
| `tests/stress/topic/tests` | 4 | MEDIUM | 220s | 0.98/4.4/4.5 | 3.8/7.9/9.3 | **2.1×** | partial | 931s | 10 | 12.7G | MEDIUM cap |
| `tests/stress/topic_kafka/tests` | 4 | MEDIUM | 113s | 4.2/4.2/4.2 | 6.3/8.1/8.3 | **2.0×** | partial | 113s | 1 | 3.6G | MEDIUM cap |
| `tests/functional/sqs/migration/compatibility/multinode` | ∅ | SMALL | 41s | 0.92/3.0/3.4 | 2.8/5.3/6.2 | **1.8×** | partial | 457s | 60 | 8.9G | SPLIT 60 |
| `tests/functional/sqs/multinode` | 4 | MEDIUM | 38s | 0.92/3.0/3.4 | 2.8/5.4/5.9 | **1.7×** | partial | 631s | 60 | 8.9G | SPLIT 60 |
| `tests/functional/sqs/migration/topic_creation/multinode` | ∅ | SMALL | 37s | 0.93/3.1/3.6 | 2.9/5.5/6.1 | **1.7×** | partial | 454s | 60 | 8.9G | SPLIT 60 |
| `tools/stress_tool/ut_sector_map` | ∅ | MEDIUM | 14s | 3.4/3.4/3.4 | 3.4/5.5/5.8 | **1.7×** | partial | 14s | 1 | 0.8G | |
| `library/actors/util/benchmark` | ∅ | SMALL | 21s | 9.5/9.5/9.5 | 10.2/15.6/16.0 | **1.7×** | нет | 21s | 1 | 1.1G | SMALL cap |
| `tests/stress/reconfig_state_storage_workload/tests` | 4 | MEDIUM | 176s | 7.3/7.8/7.8 | 9.3/11.3/13.0 | **1.7×** | нет | 526s | 4 | 11.2G | MEDIUM cap |
| `tests/functional/sqs/migration/finished/multinode` | ∅ | SMALL | 37s | 0.91/2.9/3.8 | 2.9/5.5/6.1 | **1.6×** | partial | 453s | 60 | 8.9G | SPLIT 60 |
| `core/kqp/ut/query` | 2 | MEDIUM | 409s | 1.0/1.2/3.9 | 1.1/5.0/5.5 | **1.4×** | partial | 1531s | 50 | 5.1G | |
| `core/util/ut` | 4 | MEDIUM | 21s | 0.85/3.8/5.7 | 1.0/8.0/8.1 | **1.4×** | partial | 77s | 10 | 0.3G | MEDIUM cap |
| `core/tx/conveyor_composite/ut` | 4 | MEDIUM | 82s | 0.62/1.3/12.6 | 10.4/16.8/17.4 | **1.4×** | partial | 190s | 60 | 3.8G | |
| `tests/stress/scheme_board/pile_promotion/tests` | 4 | MEDIUM | 203s | 13.6/13.6/13.6 | 14.6/17.4/17.6 | **1.3×** | нет | 203s | 1 | 20.8G | MEDIUM cap |
| `library/actors/interconnect/ut` | 4 | MEDIUM | ~9–46s | 0.90/6.5/8.9 | 1.0/6.8/10.8 | **1.2×** | нет | 183s | 8 | 1.1G | MEDIUM cap |
| `core/blobstorage/groupinfo/ut` | 4 | MEDIUM | 18s (ch9) | 0.50/20.8/37.1 | 1.0/43.5/43.9 | **1.2×** | да* | 49s | 10 | 0.8G | MEDIUM cap→4 при base 48 |
| `core/blobstorage/dsproxy/ut_ftol` | 4 | MEDIUM | 101s | 4.3/5.0/5.1 | 4.8/5.5/5.6 | **1.1×** | нет | 874s | 24 | 0.6G | |

Пути в таблице относительно `ydb/`. `да*` = ratio мал, но абсолют ~40c из‑за `hardware_concurrency()`.

### Приоритет фикса (по `cgroup_max × hot_wall`, только да/partial)

| # | impact | вердикт | Suite | что чинить в первую очередь |
|--:|-------:|:--------|-------|-----------------------------|
| 1 | ~2845 | да | `library/actors/core/ut` | вынести/облегчить benchmark+performance |
| 2 | ~2244 | partial | `core/kqp/ut/query` | `ManyPartitions*` 1k–5k → десятки |
| 3 | ~2056 | partial | `tests/stress/topic/tests` | кап partitions/consumers (MEDIUM ≤cpu:4); развести 50K-сценарий |
| 4 | ~1885 | да | `tests/functional/nbs` | session cluster / меньше erasure / io_depth |
| 5 | ~1693 | да | `library/nmslib/.../run_test_integr` | сузить `threadQty` matrix в PR |
| 6 | ~1582 | да | `tests/functional/kqp/kqp_indexes` | LocalExecutor / Sleep / erasure |
| 7 | ~1422 | partial | `core/tx/conveyor_composite/ut` | меньше tasks / setup под workers |
| 8 | ~933 | partial | `tests/stress/topic_kafka/tests` | cpu по peak; teardown сразу |
| 9 | ~781 | да* | `core/blobstorage/groupinfo/ut` ch9 | cap worker threads |
| 10 | ~715/635 | partial | SQS large + migration large | poll + shared cluster |
| 11 | ~455 | да | `tests/functional/kqp/kqp_query_svc` | `UNIFORM_PARTITIONS` |
| 12 | ~220–255 | partial | SQS multinode ×4 | `SPLIT≈N`, cpu:4, shared cluster |

`pile_promotion` / `reconfig_state_storage` дают большой impact, но это честный stress — не в очереди «плохо построены».

---

## Классы проблем

| Класс | Симптом | Примеры | Лечение |
|-------|---------|---------|---------|
| Recipe blind spot | report ≪ cgroup (25–30×) | kqp_indexes, kqp_query_svc | слот/оптимизация по cgroup; урезать кластер/нагрузку |
| `hardware_concurrency()` fan-out | абсолют ~Ncores на 64-core runner | groupinfo partlayout, nmslib | hard cap (4–8) |
| Per-test cluster boot | пик на каждом кейсе/param | nbs, SQS multinode/large | session-scoped cluster |
| Over-SPLIT | SPLIT≫числа кейсов, пустые чанки | SQS multinode (`60`) | `SPLIT ≈ N tests` |
| Idle/sleep tail | mean ≪ peak, ratio 1.5–2× | topic, topic_kafka, SQS poll `sleep(5)` | короткий poll; teardown; не мешать light/heavy |
| Oversized scenario | wall от схемы, не от assert | kqp query 4096/5k partitions; conveyor 1e6 | урезать до минимального assert |
| Perf в unit target | бенч рядом с UT | actors/core, actors/util | отдельный target / LARGE / меньше args |
| Slot under-provision | `cpu:N` ≪ cgroup peak | conveyor, sector_map, topic_kafka | поднять слот **после** урезания пика, или явно зафиксировать |

---

## Плохо построены → да (чинить)

### 1. `ydb/tests/functional/kqp/kqp_indexes` — ~30×, wall ~150s

- **Почему:** `ydb_recipe` + `YDB_ERASURE=block_4-2` + `Sleep(10s)` + `LocalExecutor` **12**×**5000** delete/upsert/select. Report ~0.35c (клиент ждёт RPC); cgroup 3–10c (кластер).
- **Код:** `main.cpp` → `ConsistentIndexRead::InteractiveTx`
- **Сделать:** урезать concurrency/итерации (напр. 4×500–1000); убрать/сократить Sleep; меньший erasure если сценарий не про него; слот по cgroup peak, не по report клиента.

### 2. `ydb/tests/functional/kqp/kqp_query_svc` — ~26×, wall ~45s

- **Почему:** тот же recipe/`block_4-2` + `UNIFORM_PARTITIONS = 4096` + стрим 100k строк. Report ~0.39c; cgroup пик ~10c на create/describe.
- **Код:** `NodeIdDescribe::HasDistribution`, `KqpQueryService::ReplyPartLimitProxyNode`
- **Сделать:** десятки/сотни партиций вместо 4096; проверить нужность полного `block_4-2`; слот по cgroup peak.

### 3. `ydb/library/nmslib/test/run_test_integr` — ~3.4×, wall ~144s

- **Почему:** ~65 index-сценариев; `multiThread` + цикл `threadQty = 1..MAX_THREAD_QTY` (повтор одних кейсов).
- **Код:** `ydb/library/nmslib/test/test_integr/test_integr.cc`
- **Сделать:** в PR оставить 1–2 значения `threadQty`; полный матричный прогон → LARGE/nightly; не дефолтить `hardware_concurrency()/2` на 64-core runner.

### 4. `ydb/library/actors/core/ut` — ~3.8×, hot ~45s @5.3c report / cgroup max ~20c

- **Почему:** `benchmark_ut` / `performance_ut` (QuickSort 1M, actor/KV pools) в том же FORK, что и обычные тесты. Пик in-process. Ch4 длинный (~144s @~1.8c) — не путать с hot-по-cores.
- **Код:** `benchmark_ut.cpp`, `performance_ut.cpp`
- **Сделать:** вынести perf в отдельный target / LARGE; ограничить размер/итерации в PR.

### 5. `ydb/tests/functional/nbs` — ~2.75×, hot ~151s, cgroup max ~12.5c

- **Почему:** каждый тест (function-scoped) поднимает **новый** кластер `MIRROR_3_DC` (9 static + dynamic) + DDisk pool; load-actor `IoDepth: 32` на 10–20s при `sleep(duration+2)`. Пик 9–12.5c при `cpu:4`. F*-фикстуры уже умеют session-cluster — этот target нет.
- **Код:** `ydb/tests/functional/nbs/common.py`, `test_nbs.py`, `ya.make` (`cpu:4`)
- **Сделать:** `Erasure.NONE` + меньше nodes; урезать io_depth/duration; `FORK_SUBTESTS`+`SPLIT` оставить вместе (SPLIT без FORK → configure fail). Полный session-share без FORK — только если укладываемся в MEDIUM timeout.

### 6. `ydb/core/blobstorage/groupinfo/ut` ch9 (partlayout) — ratio ~1.2×, абсолют ~40c

- **Почему:** `TCheckQueue` спавнит `std::thread::hardware_concurrency()` воркеров (~64 на CI) + полный перебор part-layout. Report и cgroup оба ~37–44c (честный учёт). Dashboard: `base_tier=48; MEDIUM cap → 4`.
- **Код:** `blobstorage_groupinfo_partlayout_ut.cpp`
- **Сделать:** cap воркеров (напр. 4 под `cpu:4`), не `hardware_concurrency()`.
- **ch1** `BelongsToSubgroupBenchmark` (~22s @ ~1c) — ок, не трогать.

---

## Частично плохо → partial

### SQS large / migration topic_creation large — ~2.2–2.5×, ~60s

- Один сценарий (`test_leader_start_inflight.py`): `BLOCK_4_2` (8 нод), fork по 4 params, poll `sleep(5)` пока лидеры не съедут на node0/node1. Пик ~10–12c на leader storm; mean ~5c ≈ report.
- **Сделать:** poll 0.5s с deadline; один `setup_class` cluster на params (без fork-перезапуска); не дублировать тот же кейс в двух targets без нужды.

### SQS multinode family (plain + migration ×3) — ~1.6–1.8×

- Общий `migration/suites/multinode.inc`: `BLOCK_4_2`, `FORK`, **`SPLIT_FACTOR(60)`** при ~11–15 кейсах → куча пустых чанков; каждый кейс сам бутит 8 нод ради коротких HTTP + `sleep(0.3–0.5)`.
- `topic_creation/multinode` и `finished/multinode` override **`cpu:2`** при peak ~6c.
- **Сделать:** `SPLIT_FACTOR` ≈ числу кейсов (~11–15); один cluster на chunk; вернуть `cpu:4` на override’ах.

### `ydb/tests/stress/topic/tests` — ~2.1×, ~220s

- 1 node, реальный stress; 20 producers / 50 consumer threads / **200 partitions**; 3 сценария × 60s. Hot ~6–7c ~145s, потом тихий хвост 50K byte_rate (~1c) раздувает mean. Ratio = peak/mean.
- **Сделать:** кап partitions/consumer_threads (MEDIUM слот ≤ `cpu:4`); лёгкий 50K-сценарий не в одном 60s-слоте с 10M.

### `ydb/tests/stress/topic_kafka/tests` — ~2.0×, ~113s

- Steady ~6.5–8.3c минуту, потом idle ~35s при живом кластере. Mean≈report; ratio от idle tail.
- **Сделать:** слот ≤ `cpu:4` (MEDIUM); рвать cluster сразу после `join()`; кап нагрузки если peak всё ещё выше.

### `ydb/core/kqp/ut/query` — ~1.4×, hot ch ~409s

- In-process `TKikimrRunner`; report видит сервер (~3.9c, cgroup max ~5.5). Hot: `ManyPartitions*` с **1000–5000** шардов (`kqp_limits_ut.cpp`).
- **Сделать:** десятки партиций достаточно для assert multi-task; wall режется схемой, не ratio.

### `ydb/core/util/ut` — ~1.4×

- `lf_stack_ut` `DoConcurrent`: таймер 5s × workers 4/8. Peak 8 / mean 5.7 ≈ 1.41.
- **Сделать:** iteration limit вместо длинного timed spin.

### `ydb/core/tx/conveyor_composite/ut` — ~1.4×, hot ~82s @12–17c

- `BuildActorSystemSetup(64, …)` + тесты с **16.4** workers и **1e6** tasks (`ut_simple.cpp` UniformDistribution). Peak≈workers; mean ниже из‑за poll Sleep.
- **Сделать:** размер actor system ≈ workers; урезать `GetTasksCount`; слот не `cpu:1` при 16 workers.

### `ydb/tools/stress_tool/ut_sector_map` — ~1.7×, ~14s

- Timed PDisk load 1–2s + actor pools ~10 threads; `cpu:1` при peak ~4–6c.
- **Сделать:** слот по peak; укоротить duration если assert только `throughput > 0`.

---

## Не «плохо построены» (честная/ожидаемая нагрузка)

| Suite | Ratio | Комментарий | Опционально |
|-------|------:|-------------|-------------|
| `actors/util/ut` | ~2.8× | Короткий MPMC/alloc spike | Урезать Performance-прогоны |
| `actors/util/benchmark` | ~1.7× | Явный бенч 1/4/**16** producers; 16/9.5≈ratio | Убрать arg `16` если мешает CI |
| `reconfig_state_storage_workload` | ~1.7× | 8-node `BLOCK_4_2`, duration 120s; report≈cgroup | Убавить DEBUG-логи |
| `scheme_board/pile_promotion` | ~1.3× | 16 nodes, duration 180s, LARGE; честный burn ~14–17c, RAM ~21G | Слот/`cpu` vs peak отдельно |
| `actors/interconnect/ut` | ~1.2× | V2 load: 5 nodes × 4 threads; in-process | — |
| `dsproxy/ut_ftol` | ~1.1× | ~5c steady, wall от 100k Put; ratio шум | Урезать 100k если нужен wall |
| `groupinfo/ut` ch1 | ~1.05× | BelongsToSubgroupBenchmark ~1c | Оставить |

---

## Общий harness: накладные работы «на все тесты»

Да — часть CPU/wall уходит не в assert сценария, а в **общие дефолты** поднятия кластера/рантайма. Три стека:

| Стек | Где | Сколько потребителей | Когда платим |
|------|-----|----------------------|--------------|
| `ydb_recipe` | `ydb/public/tools/ydb_recipe` + `cmds.deploy` | ~38 `ya.make` | **1× на chunk/process** (USE_RECIPE) |
| Python `KiKiMR` / `KikimrConfigGenerator` | `ydb/tests/library/harness/` | ~130+ конфигов, ~200 импортов | setup_class / fixture / иногда **каждый тест** |
| C++ `TKikimrRunner` / `TServer` | `ydb/core/kqp/ut/common`, `ydb/core/testlib` | ~244 файла | часто **на тест** (in-process) |

### Что включено по умолчанию (и часто не нужно сценарию)

**Erasure → число нод** (`KikimrConfigGenerator`, `nodes = rings × min_fail_domains`):

| Erasure | Ноды | Где часто |
|---------|-----:|-----------|
| `NONE` (дефолт harness) | 1 | recipe без `YDB_ERASURE` |
| `block_4-2` | **8** | ~11 recipe ENV + ~33 py/ya; SQS multinode/large; kqp_indexes/query_svc |
| `mirror_3_dc` | **9** | ~11 recipe ENV + ~27 py; nbs, stress/* |

**На каждой ноде** (`harness/resources/default_yaml.yml`):
- actor pools **System2 + User3 + Batch2 + IO1 + IC1 ≈ 9 потоков/ноду** → 8 нод ≈ 72 idle+service потока ещё до нагрузки;
- `pqconfig.enabled: true` всегда (`enable_pq=True` в `KikimrConfigGenerator`; в recipe CLI флаг есть, но в `deploy()` **не прокидывается**);
- gRPC services: `nbs`, `yq`, `pq*`, `datastreams`, `clickhouse_internal`, `analytics`, `export/import`, … — полный список даже для «просто KQP»;
- `federated_query_config` в yaml (enabled false, но конфиг/акторы вокруг); system tablets на всех static nodes.

**Уже сделано правильно:**
- `recipe.inc` / `harness_dep.inc`: `YDB_TEST_DISABLE_MLOCK=1` (нет `mlockall` в тестах);
- `enable_drain_on_shutdown = False` (быстрее stop);
- in-memory PDisks по умолчанию (исключение: compatibility fixtures → **file** PDisks);
- log default `NOTICE` в recipe yaml (но см. DEBUG/TRACE в отдельных fixtures ниже).

**Почти не используется:**
- `YDB_TINY_MODE` / `--tiny-mode` — только ~1 тест; `SetTinyMode()` выключает лишь SelfPing + MemoryTracker (пулы **не** режет: yaml явный → auto-config не включается).

**In-process KQP/UT (платится на каждый `Y_UNIT_TEST`):**
- `TKikimrSettings::WithSampleTables = true` → `CreateSampleTables` (TwoShard/EightShard/Logs/BatchUpload, ~23 шарда + insert) даже если тест таблицы не читает;
- `MakeActorSystemSetup`: ~8 real threads + scheduler spin threshold 100;
- `WaitRootIsUp`: busy-loop `Ls` без sleep до ответа SchemeShard;
- полный bootstrap tablets (Coordinator/Mediator/SchemeShard/Hive/BSController/Console/…).

**Прочие всегда-on мелочи harness:**
- `add_storage_pool` default **`num_groups=2`** даже при `NONE` (лишняя dynamic group на каждый cluster);
- `KiKiMRNode.start` вызывает `ydbd -V` на каждую ноду; ноды стартуют **сериально**;
- `use_log_files=True`, recipe не передаёт `dont_use_log_files`;
- `ydb_cluster` fixture: DEBUG на FLAT_TX_SCHEMESHARD / TX_DATASHARD / CHANGE_EXCHANGE;
- SQS base: `SQS: TRACE`;
- compatibility fixtures: default `MIRROR_3_DC` + file PDisks;
- recipe `deploy()`: `YDB_KAFKA_PROXY_PORT` default `"auto"` → **Kafka proxy всегда** на stock `ydb_recipe`; `pdisk_store_path=ydb_working_dir` → **выключает** in-memory PDisks (file-backed 64GiB SectorMap size);
- `TServer::SetupLocalService` / `SetupProxies`: column-shard limiters/conveyors, PQ tracker/cache/deferred-publish, metadata provider refresh **1s** — без opt-in, даже если тест про один DataShard;
- shared waiters: client `__retry_sleep_seconds=10`, `wait_for_matcher` step 1s (на success path реже, но дорого на retry).

### Где накладные × умножаются

1. **`FORK_SUBTESTS` × module-scoped `ydb_cluster`** — fixture «module», но fork = новый process → полный cluster **на каждый тест** (tenants, topic, secrets, serverless, audit, …). Крупнейший множитель Python.
2. **FORK / SPLIT × recipe/`setup_class`** — SQS `SPLIT_FACTOR(60)` при ~11 кейсах; ~60 KQP `ut/**/ya.make` с `FORK_SUBTESTS` (in-process cluster и так per-test, но fork ещё платит process/sanitizer).
3. **Function-scoped cluster** — nbs (не F*): новый `MIRROR_3_DC` на тест; compatibility/stress `scope=function`.
4. **Тяжёлый erasure «на всякий случай»** — kqp_indexes/query_svc/`block_4-2` (не дефолт harness — уже `NONE`).
5. **In-process UT** — sample tables + real threads + полный tablet/gRPC/PQ boot на **каждый** case.
6. **SQS `setup_method`** — на каждый тест `node.start()` «если умер».

### Что резать в shared-коде (широкий impact)

| # | Изменение | Где | Кого заденет | Эффект |
|--:|-----------|-----|--------------|--------|
| 1 | **`WithSampleTables=false` по умолчанию** (opt-in где нужны) | `kqp_ut_common.h/.cpp` | сотни KQP UT | главный wall cut in-process |
| 2 | Прокинуть `enable_pq=` из recipe CLI; дефолт **False** | `cmds/__init__.py`, `KikimrConfigGenerator` | все recipe + py harness | меньше PQ на boot |
| 3 | Урезать default gRPC + `dont_use_log_files` / без `ydbd -V` | `default_yaml.yml`, recipe, `KiKiMRNode` | все KiKiMR/recipe | меньше сервисов/IO логов |
| 4 | CI actor pools 1/1/1/1/1; spin 0; реальный `tiny_mode` | `default_yaml.yml`, `test_runtime.cpp`, `SetTinyMode` | multi-node + UT | меньше idle потоков |
| 5 | `num_groups=1` по умолчанию | `kikimr_runner.py` `add_storage_pool` | все harness/recipe | −1 BS group на boot |
| 6 | Не ставить `block_4-2` / `mirror_3_dc` без сценария про erasure | suite `ya.make` / py | kqp_*, stress, nbs | 1 нода вместо 8–9 |
| 7 | Не `FORK_SUBTESTS` на suite с module `ydb_cluster`; session/class-scoped cluster | fixtures + ya.make | tenants/topic/secrets/… | убрать N полных boot |
| 8 | `SPLIT_FACTOR ≈ N tests` | SQS multinode `.inc` | 4 suite | меньше пустых boot |
| 9 | `WaitRootIsUp` без busy-spin; opt-in тяжёлых `SetupLocalService`/gRPC/PQ | `test_client.cpp` | много UT | быстрее cold start |
| 10 | Убрать DEBUG/TRACE из shared fixtures | `fixtures/__init__.py`, SQS base | py functional | меньше log CPU |
| 11 | Recipe: Kafka off by default; не форсить file PDisks через `pdisk_store_path` | `cmds/__init__.py` | все `ydb_recipe` | −Kafka + in-memory disks |

Не путать с per-suite «плохо построенными» тестами выше: harness — это **база**, на которой они ещё и сами оверсайзят нагрузку.

---

## Общие правила

1. Recipe / внешний `ydbd`: низкий report + высокий cgroup → слот и оптимизация по **cgroup peak**, не по `median_cores` дашборда.
2. Ratio 1.1–2× при совпадении report cpu-sec с cgroup → смотреть **peak vs mean** и idle/sleep хвосты, не «магию учёта».
3. Не масштабировать от `hardware_concurrency()` на 64-core runner.
4. Не бутить N-node кластер на каждый forked param, если хватает session-scoped cluster.
5. `SPLIT_FACTOR` ≈ числу реальных кейсов, не «с запасом 60».
6. Перед снижением `REQUIREMENTS(cpu)` — cgroup peak и critical path wall.
7. `SIZE(MEDIUM/SMALL)` cap на dashboard ≠ реальная нагрузка: смотреть `recommended_cpu_base` / cgroup max.
8. Для `SIZE(MEDIUM)` в ya.make **не ставить `cpu` > 4** (и для SMALL — >1): слот режется политикой SIZE; чинить пик нагрузкой, не `cpu:8`.
9. Не поднимать слот «поверх» плохого теста: сначала урезать пик, потом выставлять `cpu` под новый peak (в пределах SIZE).
9. Shared harness: не включать PQ/полный gRPC/8–9 нод «по умолчанию», если сценарий этого не требует.

---

## Как перепроверить

```bash
# метрики прогона (пример)
# dashboard: .../PR-check/<run>/ya-x86-64/try_1/tests_metrics/dashboard.html
# сырьё: trace_chunks.csv + resources_monitor.jsonl (kind=test)

# локально один сюит
./ya make --build relwithdebinfo -tA <folder> -F '*filter*'
```

Сравнивать: report cores чанка vs cgroup `cpu_used_cores` timeline; для recipe — не верить низкому report.

---

## Статус

**Да (по impact):**
- [ ] actors/core: вынести/облегчить benchmark+performance
- [ ] nbs: session cluster / меньше erasure / io_depth
- [ ] nmslib: сузить threadQty matrix в PR
- [ ] kqp_indexes: LocalExecutor / sleep / erasure
- [ ] groupinfo partlayout: cap worker threads
- [ ] kqp_query_svc: UNIFORM_PARTITIONS

**Partial:**
- [ ] kqp/ut/query ManyPartitions: меньше шардов
- [ ] stress/topic: кап partitions (MEDIUM ≤cpu:4); развести лёгкий сценарий
- [ ] conveyor_composite: меньше tasks / setup под workers
- [ ] stress/topic_kafka: ≤cpu:4 + teardown сразу
- [ ] SQS large + migration large: poll + shared cluster
- [ ] SQS multinode family: SPLIT≈N, cpu:4, shared cluster
- [ ] util/ut lf_stack: iteration limit
- [ ] ut_sector_map: слот по peak

**Harness (широкий impact):**
- [ ] KQP UT: `WithSampleTables=false` by default
- [ ] recipe: прокинуть `enable_pq`; дефолт False
- [ ] `default_yaml`: минимальный gRPC + `dont_use_log_files` / без `ydbd -V`
- [ ] `num_groups=1` default; CI actor pools / реальный `tiny_mode`
- [ ] убрать лишний `block_4-2`/`mirror_3_dc` где сценарий не про erasure
- [ ] не FORK’ать module `ydb_cluster`; session-scoped (nbs и др.)
- [ ] `WaitRootIsUp` без busy-spin; убрать DEBUG/TRACE из shared fixtures
- [ ] recipe: Kafka off; in-memory PDisks не сбрасывать через working_dir
