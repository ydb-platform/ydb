# Секция конфигурации `log_config`

Секция `log_config` управляет тем, как серверный процесс {{ ydb-short-name }} обрабатывает и управляет своими логами. Она позволяет настраивать уровни логирования для различных компонентов, а также глобальные форматы логов и методы вывода.

{% note info %}

Этот документ описывает конфигурацию прикладного и системного логирования. Для информации о логировании для целей аудита и обеспечения безопасности, см. [{#T}](../../security/audit-log.md).

{% endnote %}

## Обзор

Логирование является критически важной частью системы [наблюдаемости](../observability/index.md) {{ ydb-short-name }}. Секция `log_config` позволяет настраивать различные аспекты логирования, включая:

- уровень логирования по умолчанию;
- уровни логирования для конкретных компонентов;
- формат вывода логов;
- ротацию и хранение логов;
- интеграцию с системными логами или внешними сервисами логирования.

## Параметры конфигурации

| Параметр | Тип | Значение по умолчанию | Описание |
| --- | --- | --- | --- |
| `default_level` | uint32 | 5 (NOTICE) | Уровень логирования по умолчанию для всех компонентов. |
| `default_sampling_level` | uint32 | 7 (DEBUG) | Уровень семплирования по умолчанию для всех компонентов. |
| `default_sampling_rate` | uint32 | 0 | Частота семплирования по умолчанию для всех компонентов. |
| `sys_log` | bool | false | Включить системное логирование через syslog. |
| `sys_log_to_stderr` | bool | false | Копировать логи в stderr в дополнение к системному логу. |
| `format` | string | "full" | Формат вывода логов. Возможные значения: "full", "short", "json". |
| `cluster_name` | string | — | Имя кластера для включения в записи логов. |
| `allow_drop_entries` | bool | true | Разрешить отбрасывание записей логов, если система логирования перегружена. |
| `use_local_timestamps` | bool | false | Использовать локальный часовой пояс для временных меток логов (по умолчанию используется UTC). |
| `backend_file_name` | string | — | Имя файла для вывода логов. Если указано, логи записываются в этот файл. |
| `sys_log_service` | string | — | Имя сервиса для syslog. |
| `time_threshold_ms` | uint64 | 1000 | Пороговое значение времени для операций логирования в миллисекундах. |
| `ignore_unknown_components` | bool | true | Игнорировать запросы логирования от неизвестных компонентов. |
| `tenant_name` | string | — | Имя базы данных для включения в записи логов. |
| `entry` | array | [] | Массив конфигураций логирования для конкретных компонентов. |
| `uaclient_config` | object | — | Конфигурация для клиента Unified Agent. |

### Объекты Entry

Поле `entry` содержит массив объектов со следующей структурой:

| Параметр | Тип | Описание |
| --- | --- | --- |
| `component` | string | Имя компонента (должно быть закодировано в base64 при указании в YAML). |
| `level` | uint32 | Уровень логирования для этого компонента. |
| `sampling_level` | uint32 | Уровень семплирования для этого компонента. |
| `sampling_rate` | uint32 | Частота семплирования для этого компонента. |

### Объект UAClientConfig

Поле `uaclient_config` настраивает интеграцию с [Unified Agent](https://yandex.cloud/ru/docs/monitoring/concepts/data-collection/unified-agent/):

| Параметр | Тип | Значение по умолчанию | Описание |
| --- | --- | --- | --- |
| `uri` | string | — | URI сервера Unified Agent. |
| `shared_secret_key` | string | — | Общий секретный ключ для аутентификации. |
| `max_inflight_bytes` | uint64 | 100000000 | Максимальное количество байт в передаче. |
| `grpc_reconnect_delay_ms` | uint64 | — | Задержка между попытками переподключения в миллисекундах. |
| `grpc_send_delay_ms` | uint64 | — | Задержка между попытками отправки в миллисекундах. |
| `grpc_max_message_size` | uint64 | — | Максимальный размер сообщения gRPC. |
| `client_log_file` | string | — | Файл логов для самого клиента UA. |
| `client_log_priority` | uint32 | — | Приоритет логирования для клиента UA. |
| `log_name` | string | — | Имя лога для метаданных сессии. |

## Уровни логирования {#log-levels}

{{ ydb-short-name }} использует следующие уровни логирования, перечисленные от наивысшей к наименьшей серьезности:

| Уровень | Числовое значение | Описание |
| --- | --- | --- |
| EMERG | 0 | Возможен сбой системы (например, отказ кластера). |
| ALERT | 1 | Возможна деградация системы, компоненты системы могут выйти из строя. |
| CRIT | 2 | Критическое состояние. |
| ERROR | 3 | Некритическая ошибка. |
| WARN | 4 | Предупреждение, на которое следует отреагировать и исправить, если оно не временное. |
| NOTICE | 5 | Произошло событие, существенное для системы или пользователя. |
| INFO | 6 | Отладочная информация для сбора статистики. |
| DEBUG | 7 | Отладочная информация для разработчиков. |
| TRACE | 8 | Очень подробная отладочная информация. |

## Примеры

### Базовая конфигурация

```yaml
log_config:
  default_level: 5  # NOTICE
  sys_log: true
  format: "full"
  backend_file_name: "/var/log/ydb/ydb.log"
```

### Настройка уровней логирования для отдельных компонентов

```yaml
log_config:
  default_level: 5  # NOTICE
  entry:
    - component: U0NIRU1FU0hBUkQ=  # Base64 для "SCHEMESHARD"
      level: 7  # DEBUG
    - component: VEFCTEVUX01BSU4=  # Base64 для "TABLET_MAIN"
      level: 6  # INFO
  backend_file_name: "/var/log/ydb/ydb.log"
```

### Формат JSON с интеграцией Unified Agent

```yaml
log_config:
  default_level: 5  # NOTICE
  format: "json"
  cluster_name: "production-cluster"
  sys_log: false
  uaclient_config:
    uri: "[fd53::1]:16400"
    grpc_max_message_size: 4194304
    log_name: "ydb_logs"
```

### Полный пример

```yaml
log_config:
  default_level: 5  # NOTICE
  default_sampling_level: 7  # DEBUG
  default_sampling_rate: 10
  sys_log: true
  sys_log_to_stderr: true
  format: "json"
  cluster_name: "production-cluster"
  allow_drop_entries: true
  use_local_timestamps: false
  backend_file_name: "/var/log/ydb/ydb.log"
  sys_log_service: "ydb"
  time_threshold_ms: 2000
  ignore_unknown_components: true
  tenant_name: "main"
  entry:
    - component: U0NIRU1FU0hBUkQ=  # Base64 для "SCHEMESHARD"
      level: 7  # DEBUG
    - component: VEFCTEVUX01BSU4=  # Base64 для "TABLET_MAIN"
      level: 6  # INFO
    - component: QkxPQlNUT1JBR0U=  # Base64 для "BLOBSTORAGE"
      level: 4  # WARN
  uaclient_config:
    uri: "[fd53::1]:16400"
    grpc_max_message_size: 4194304
    log_name: "ydb_logs"
```

## Примечания

- При указании имён компонентов в массиве `entry` имена компонентов должны быть закодированы в base64.
- Уровни логирования указываются в конфигурации как числовые значения, а не строки. Используйте [таблицу выше](#log-levels) для сопоставления между числовыми значениями и их значениями.
- Если указан параметр `backend_file_name`, логи записываются в этот файл. Если параметр `sys_log` имеет значение true, логи отправляются в системный регистратор.
- Параметр `format` определяет, как форматируются записи логов. Формат "full" включает всю доступную информацию, "short" предоставляет более компактный формат, а "json" выводит логи в формате JSON для более простого разбора.

## Смотрите также

- [{#T}](../observability/index.md)
- [{#T}](../observability/metrics/index.md)
- [{#T}](../observability/tracing/setup.md)
- [{#T}](../../security/audit-log.md)