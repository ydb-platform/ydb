# Nemesis App

Приложение для инъекции неисправностей (fault injection) в YDB кластер.

## Режимы работы

- **master** (orchestrator) — планирование chaos-сценариев, диспетчеризация на агенты, UI, liveness/safety wardens с мастера.
- **agent** — запуск nemesis runner’ов на хосте, локальные safety-проверки по логам; состояние процессов с мастера опрашивается по HTTP.

## Конфигурация

Переменные окружения (или аргументы CLI для `install` / `run`, см. `nemesis --help`):

| Параметр | Описание | По умолчанию |
|----------|----------|--------------|
| `NEMESIS_TYPE` | Режим: `master` или `agent` | `master` |
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

- **Общее** (и master, и agent): `config.py`, `models.py`, `event_loop.py`, `nemesis/catalog.py`, `nemesis/chaos_dispatch.py`.
- **`internal/agent/`** — только агент: `agent_warden_checker.py`, `nemesis/runner.py` (`NemesisManager`).
- **`internal/master/`** — только оркестратор: `install.py`, `orchestrator_warden_checker.py`, `nemesis/` (расписание, `chaos_state`, планировщики). Состояние оркестратора (hosts, healthcheck, chaos store) живёт в `routers/orchestrator_router.py`.

## UI и API-модели

Статические ответы для UI описаны датаклассами в `internal/models.py` (`ProcessInfo`, `ProcessTypeRow`, `WardenCheckReport`, и т.д.). Эндпоинты возвращают те же поля, что и раньше, в виде JSON.

## Результаты запусков на агенте

Завершение и логи процессов на агенте **не пушатся** на мастер. Состояние снимается опросом с оркестратора: `GET /api/hosts/processes` (агрегирует `GET /api/processes` по хостам).

## Логирование nemesis runner’ов

Сообщения исполняемых на агенте nemesis пишутся в логгер `ydb.tests.stability.nemesis.execution`. `NemesisManager` на время выполнения вешает на него потоковый handler для сбора логов в UI; **корневой логгер не трогается**.

## UI в браузере

`http://<orchestrator_host>:31434/static/index.html`
