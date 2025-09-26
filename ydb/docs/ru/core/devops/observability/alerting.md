# Алертинг в {{ ydb-short-name }}

## {{ ydb-short-name }} Prometheus Alerting Rules

Prometheus Alerting Rules — это набор правил, написанных в формате YAML, которые определяют условия для генерации оповещений (алертов). Эти правила основаны на языке запросов PromQL (Prometheus Query Language) и позволяют автоматически выявлять проблемы на основе собранных метрик. Например, вы можете настроить правило, которое сработает, если CPU нагрузка превысит 90% или если диск заполнен более чем на 80%.

- **Где они хранятся?** Правила обычно определяются в отдельных файлах (например, rules.yml) и загружаются в конфигурацию Prometheus.
- **Как они оцениваются?** Prometheus сервер периодически (по умолчанию каждые 1-2 минуты) вычисляет выражения в правилах. Если условие истинно в течение заданного времени (параметр for), генерируется алерт.
- **Состояния алерта:**
    **Pending** (ожидание): Условие истинно, но ещё не прошло время for.
    **Firing** (активный): Условие истинно и прошло время for.
    **Resolved** (решён): Условие больше не истинно.

Более подробно об особенности системы и стуктуре правил смотрите в [официальной документации](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/).

### 1. {{ ydb-short-name }} Health Check

**Описание:** Правило отслеживает общее состояние здоровья {{ ydb-short-name }} кластера. Срабатывает, когда статус здоровья отличается от `GOOD`, что может указывать на проблемы с доступностью или производительностью базы данных.
**Что делать:** Это общий алерт системы самодиагностики, в сообщении будет указана причина, по которой сработал алерт.

```yaml
- alert: YDBHealthCheck
  expr: ydb_healthcheck{STATUS!="GOOD"}
  for: 1m
  labels:
    severity: '{{ if or (eq $labels.STATUS "RED") (eq $labels.STATUS "DEGRADED") }} critical {{ else }} warning {{ end }}'
    component: ydb
    database: "{{ $labels.DATABASE }}"
    type: "{{ $labels.TYPE }}"
    instance: "{{ $labels.instance }}"
  annotations:
    summary: "YDB Health Issue in {{ $labels.DATABASE }} (Status: {{ $labels.STATUS }})"
    description: |
      Health check failed for YDB.
      - Status: {{ $labels.STATUS }}
      - Message: {{ $labels.MESSAGE }}
      - Type: {{ $labels.TYPE }}
      - Database: {{ $labels.DATABASE }}
      - Domain: {{ $labels.DOMAIN }}
      - Instance: {{ $labels.instance }}
      This issue has been active for more than 1 minute.
```

**Пример сработавшего алерта:**

```text
description: Health check failed for YDB.
  - Status: RED
  - Message: VDisk is not available
  - Type: VDISK
  - Database: /Root
  - Domain: Root
  - Instance: myt0-7514.search.yandex.net
  This issue has been active for more than 1 minute.
summary: YDB Health Issue in /Root (Status: RED)
```

### 2. {{ ydb-short-name }} ExecPool High Utilization

**Описание:** Правило контролирует загрузку [пулов](../concepts/glossary#resource-pool) в {{ ydb-short-name }}. Срабатывает при превышении 90% утилизации, что может привести к деградации производительности и увеличению времени отклика запросов.
**Что делать:** Проанализировать нагрузку. В сообщении алерта будет указано название пула, в котором превышена нагрузка.

```yaml
- alert: YDBExecPoolHighUtilization
  expr: |
    (
      sum by (instance, execpool) (
        rate(utils_ElapsedMicrosec[1m])
      ) / 1000000
    ) > 0.9
  for: 1m
  labels:
    severity: critical
    component: ydb
    subsystem: execpool
    instance: "{{ $labels.instance }}"
    execpool: "{{ $labels.execpool }}"
  annotations:
    summary: "YDB ExecPool high utilization on {{ $labels.instance }}"
    description: |
      ExecPool {{ $labels.execpool }} on host {{ $labels.instance }}
      is loaded at {{ $value | humanizePercentage }}.
      This may lead to performance degradation.
      - Instance: {{ $labels.instance }}
      - ExecPool: {{ $labels.execpool }}
      - Current utilization: {{ $value | humanizePercentage }}
```

### 3. {{ ydb-short-name }} Authentication Ticket Errors

**Описание:** Правило отслеживает ошибки аутентификации в {{ ydb-short-name }}. Срабатывает при появлении более 2 ошибок, что может указывать на проблемы с системой безопасности или неправильную конфигурацию.
**Что делать:** Искать в [логах](./logging.md) ошибки аутентификации и разбираться в причинах.

```yaml
- alert: YDBAuthTicketErrors
  expr: auth_TicketsErrors > 2
  for: 1m
  labels:
    severity: critical
    component: ydb
    subsystem: auth
    instance: "{{ $labels.instance }}"
  annotations:
    summary: "YDB authentication ticket errors on {{ $labels.instance }}"
    description: |
      Authentication errors detected in YDB.
      This may indicate security issues or misconfiguration.
      - Current error count: {{ $value }}
      - Instance: {{ $labels.instance }}
      - Host: {{ $labels.host }}
```

### 4. {{ ydb-short-name }} Storage Usage Warning

**Описание:** Предупреждающее правило о высоком использовании дискового пространства. Срабатывает при заполнении хранилища более чем на 80%.
**Что делать:** Найти причину превышения базой ожидаемого размера. Удалить ненужные записи (например, старые логи) или увеличить лимит на размер базы, если это требуется.

```yaml
- alert: YDBStorageUsageWarning
  expr: |
    (ydb_resources_storage_used_bytes / ydb_resources_storage_limit_bytes) * 100 > 80
    and ydb_resources_storage_limit_bytes > 0
  for: 5m
  labels:
    severity: warning
    component: ydb
    subsystem: storage
    database: "{{ $labels.database }}"
  annotations:
    summary: "High storage usage in YDB database {{ $labels.database }}"
    description: |
      Storage usage is above warning threshold.
      Consider cleaning up old data or increasing storage capacity.
      - Database: {{ $labels.database }}
      - Current usage: {{ printf "%.2f" $value }}%
      - Threshold: 80%
      - Duration: more than 5 minutes
```

### 5. {{ ydb-short-name }} Storage Usage Critical

**Описание:** Критическое правило о заполнении дискового пространства. Срабатывает при использовании более 90% хранилища.
**Что делать:** Найти причину превышения базой ожидаемого размера. Удалить ненужные записи (например, старые логи) или увеличить лимит на размер базы, если это требуется.

```yaml
- alert: YDBStorageUsageCritical
  expr: |
    (ydb_resources_storage_used_bytes / ydb_resources_storage_limit_bytes) * 100 > 90
    and ydb_resources_storage_limit_bytes > 0
  for: 5m
  labels:
    severity: critical
    component: ydb
    subsystem: storage
    database: "{{ $labels.database }}"
  annotations:
    summary: "Critical storage usage in YDB database {{ $labels.database }}"
    description: |
      Storage usage is critically high. Immediate action required!
      Database may stop accepting writes soon.
      - Database: {{ $labels.database }}
      - Current usage: {{ printf "%.2f" $value }}%
      - Threshold: 90%
      - Duration: more than 5 minutes
```

## Полный конфигурационный файл

```yaml
groups:
- name: ydb-rules
  rules:
  # Здесь размещаются все правила из секций выше
```

## Рекомендации по настройке

- **Пороговые значения:** Настройте пороги в соответствии с вашими SLA и характеристиками нагрузки.
- **Время ожидания (`for`):** Увеличьте параметр `for` для менее критичных алертов, чтобы избежать ложных срабатываний.
- **Severity levels:** Используйте `warning` для предупреждений и `critical` для ситуаций, требующих немедленного вмешательства.
