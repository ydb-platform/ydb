# Nemesis App

Приложение для инъекции неисправностей (fault injection) в YDB кластер.

## Режимы работы

- **orchestrator** — планирование chaos-сценариев, диспетчеризация на агенты, UI, liveness/safety wardens на оркестраторе.
- **agent** — запуск nemesis runner’ов на хосте, локальные safety-проверки по логам; состояние процессов с оркестратора опрашивается по HTTP.

## Конфигурация

Переменные окружения (или аргументы CLI для `install` / `run`, см. `nemesis --help`):

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `NEMESIS_TYPE` | Режим: `orchestrator` или `agent` | `orchestrator` |
| `APP_HOST` | Адрес привязки HTTP | `::` |
| `APP_PORT` | Порт приложения | `31434` |
| `MON_PORT` | Порт мониторинга (warden / health) | `8765` |
| `STATIC_LOCATION` | Каталог статики UI (оркестратор) | `static` |
| `YAML_CONFIG_LOCATION` | Путь к `cluster.yaml` | — |
| `NEMESIS_INSTALL_ROOT` | Корень установки на удалённых хостах (`rsync`, `ExecStart` в unit) | `/Berkanavt/nemesis` |
| `KIKIMR_LOGS_DIRECTORY` | Каталог логов Kikimr для safety wardens на агенте | `/Berkanavt/kikimr/logs/` |

Для установки с нестандартными путями:

```bash
./nemesis install --yaml-config-location /path/to/cluster.yaml \
  --install-root /opt/nemesis \
  --kikimr-logs-directory /var/log/kikimr/
```

## Установка на кластер и запуск

```bash
# Из директории с бинарником nemesis
./nemesis --yaml-config-location /your/path/to/config.yaml install
```

Первый хост из `cluster.yaml` становится orchestrator, остальные — агентами.

Порт по умолчанию: **31434** (настраивается через `APP_PORT`).

## Остановка сервисов

```bash
./nemesis stop
```

## Структура `internal/`

- **Общее** (и orchestrator, и agent): `config.py`, `models.py`, `event_loop.py`, `nemesis/catalog.py`, `nemesis/chaos_dispatch.py`.
- **`internal/agent/`** — только агент: `agent_warden_checker.py`, `nemesis/runner.py` (`NemesisManager`).
- **`internal/orchestrator/`** — только оркестратор: `install.py`, `orchestrator_warden_checker.py`, `nemesis/` (расписание, `chaos_state`, планировщики). Состояние оркестратора (hosts, healthcheck, chaos store) живёт в `routers/orchestrator_router.py`.

## UI и API-модели

Статические ответы для UI описаны датаклассами в `internal/models.py` (`ProcessInfo`, `ProcessTypeRow`, `WardenCheckReport`, и т.д.). Эндпоинты возвращают те же поля, что и раньше, в виде JSON.

## Результаты запусков на агенте

Завершение и логи процессов на агенте **не пушатся** на оркестратор. Состояние снимается опросом с оркестратора: `GET /api/hosts/processes` (агрегирует `GET /api/processes` по хостам).

## Логирование nemesis runner’ов

Сообщения исполняемых на агенте nemesis пишутся в логгер `ydb.tests.stability.nemesis.execution`. `NemesisManager` на время выполнения вешает на него потоковый handler для сбора логов в UI; **корневой логгер не трогается**.

## UI в браузере

`http://<orchestrator_host>:31434/static/index.html`

---

## Расширение: свой nemesis

Реестр и UI-группы: **`internal/nemesis/catalog.py`** (`NEMESIS_TYPES`, `NEMESIS_UI_GROUPS`).

### Как выполняется nemesis

1. **Оркестратор** по расписанию или вручную вызывает планировщик (`ChaosOrchestratorStore` → `NemesisPlannerBase`), получает список `DispatchCommand`.
2. Команды уходят на агенты: **HTTP `POST /api/processes`** с телом `{ type, action, payload }` (см. `internal/orchestrator/nemesis/schedule_loop.py`, `chaos_dispatch.py`).
3. **Агент** в `routers/agent_router.py` берёт `runner` из `NEMESIS_TYPES[type]` и запускает **`inject_fault` / `extract_fault`** в потоке через `NemesisManager` (`internal/agent/nemesis/runner.py`).

Тело сценария всегда на **агенте**; оркестратор только планирует **какой** тип, **на каком** хосте и **какой** payload.

### Регистрация типа

1. Добавьте класс актора, наследник **`MonitoredAgentActor`** (как `NetworkNemesis` / `KillNodeNemesis`): реализуйте **`inject_fault`** и **`extract_fault`**, при необходимости читайте `payload` из dispatch.
2. Заведите **строковый id** процесса (константа, как `NETWORK_NEMESIS` в `network_planner.py`).
3. В **`NEMESIS_TYPES`** добавьте запись:
   - **`runner`**: экземпляр актора;
   - **`schedule`**: интервал по умолчанию для UI (секунды);
   - **`ui_group`**: id группы в **`NEMESIS_UI_GROUPS`** (описание для `/api/process_types/grouped`; неизвестная группа попадёт под «Other», если не добавить описание);
   - **`planner_cls`**: класс планировщика **или отсутствие ключа** (см. ниже).

### Без своего планировщика

**Не указывайте `planner_cls`** в записи `NEMESIS_TYPES`. Тогда `build_all_planners()` подставит **`DefaultRandomHostPlanner`** (`internal/orchestrator/nemesis/default_planner.py`):

- на каждый тик расписания выбирается **случайный** хост из кластера и шлётся **inject** с **пустым** `PAYLOAD_INJECT`;
- при **выключении** расписания **extract по списку затронутых хостов не планируется** (планировщик никого не «помнит»);
- ручной inject/extract из UI по-прежнему уходит на выбранный хост.

Этого достаточно, если сценарий **без памяти между тиками** и **без массового extract** при отключении расписания (например, одноразовый удар по случайной ноде с пустым payload, если актор сам всё делает локально).

### Со своим планировщиком

Нужен, если требуется, например:

- вести **множество затронутых хостов** и при **отключении** расписания сделать **extract на всех**;
- на одном тике **несколько** команд или **своя** логика выбора хостов (не «один случайный»);
- **разные** `PAYLOAD_INJECT` / `PAYLOAD_EXTRACT` (как у сетевого nemesis).

Шаги:

1. Подкласс **`NemesisPlannerBase`** (`internal/orchestrator/nemesis/nemesis_planner_base.py`): задайте **`nemesis_type`**, **`PAYLOAD_INJECT`**, **`PAYLOAD_EXTRACT`**, реализуйте **`scheduled_tick`**, **`_drain_tracked_hosts`**, **`_register_inject`**, **`_register_extract`** (ориентир — `network_planner.py`, `kill_node_planner.py`).
2. В **`NEMESIS_TYPES`** укажите **`planner_cls`: ВашPlanner`** (класс, не экземпляр — его создаёт `build_all_planners()`).

### Кратко: когда обходиться без планировщика

| Нужно | Достаточно `DefaultRandomHostPlanner` (без `planner_cls`) |
|--------|--------------------------------------------------------|
| Один inject на случайный хост за тик, payload не важен / фиксирован в акторе | Да |
| Помнить «кого задели» и при выключении расписания сделать extract всем | Нет, свой planner |
| Нестандартный выбор хостов / несколько команд за тик | Нет, свой planner |

---

## Расширение: liveness и safety checks

Каталоги: **`internal/agent/agent_warden_catalog.py`** (агент: **`collect_agent_safety_warden_pairs`**, фабрики в **`ydb/tests/library/wardens/logs.py`** и т.п.), **`internal/orchestrator/orchestrator_warden_catalog.py`** (оркестратор). Список проверок до запуска в UI/API не отдаётся — строки появляются в **`GET /api/hosts/warden/results`** после **`Run Checks`**.

### Где что выполняется

| Категория | Где исполняется | Как попадает в отчёт |
|-----------|-----------------|----------------------|
| **Liveness** | Только **оркестратор**: подпроцесс `nemesis liveness` (набор из `ORCHESTRATOR_LIVENESS_CHECKS`, исполнение `run_orchestrator_liveness_cli_batch` в `orchestrator_warden_execution.py`) | `_orchestrator` в `GET /api/hosts/warden/results` |
| **Safety (agent)** | Каждый **агент** локально (`AgentWardenChecker`, фоновый asyncio + `run_in_executor`, проверки параллельно) | По каждому хосту в том же JSON |
| **Safety (orchestrator)** | **Оркестратор** (`OrchestratorWardenChecker`): кортежи **`ORCHESTRATOR_CLUSTER_SAFETY_CHECKS`** и **`ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS`** (агрегация — `unified_agent_verify_failed_aggregated.py`) | В `_orchestrator.safety_checks` |

Агенты **liveness не запускают** (в отчёте по хосту блок liveness пустой).

### Добавить liveness check

1. В **`internal/orchestrator/orchestrator_warden_catalog.py`** добавьте элемент в кортеж **`ORCHESTRATOR_LIVENESS_CHECKS`**: **`OrchestratorLivenessCheck`** с **`name`**, **`description`**, **`build=lambda c: ...`** (как у агента).
2. Команда **`nemesis liveness`** вызывает **`run_orchestrator_liveness_cli_batch`** — дублировать список не нужно.

Исполнение: бинарь на оркестраторе вызывает `nemesis liveness`, внутри — тот же каталог.

### Добавить safety check

Зависит от **location** (`agent` / `orchestrator`).

**Agent (`location: "agent"`)** — проверка с доступом к **локальным** логам / dmesg и т.п.:

1. В **`internal/agent/agent_warden_catalog.py`** расширьте **`collect_agent_safety_warden_pairs`**: добавляйте пары **`(slot_name, warden)`** с уникальной строкой **`slot_name`** (см. **`_agent_safety_slot_name`**). Либо добавьте вызов новой фабрики по образцу **`kikimr_start_logs_safety_warden_factory`** / **`kikimr_grep_dmesg_safety_warden_factory`**.

**Orchestrator (`location: "orchestrator"`)** — логика на оркестраторе (кластер и/или агрегация по агентам):

1. Локальная по кластеру: в **`orchestrator_warden_catalog.py`** — элемент в кортеже **`ORCHESTRATOR_CLUSTER_SAFETY_CHECKS`** с **`build=lambda c: ...`**. Сбор пар **`collect_orchestrator_cluster_safety_warden_pairs`** и запуск через **`build_safety_runs_from_pairs`** (**`internal/safety_warden_execution.py`**, тот же путь, что на агенте).
2. Агрегированная: элемент в **`ORCHESTRATOR_AGGREGATED_SAFETY_CHECKS`** с **`agent_source_class_name`** и **`impl`** (**`aggregate(...)`**; ожидание — **`OrchestratorWardenChecker._wait_for_agent_safety_completion_async`**). Вызов — **`run_orchestrator_aggregated_safety`**.

Для новых агрегаторов: в **`safety_checks`** ищите строку по **`name`** (точное совпадение или первый токен — см. **`UnifiedAgentVerifyFailedAggregated._row_matches_class`**).
