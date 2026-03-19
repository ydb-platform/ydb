# Настройка TTL для метрик в мониторинге {{ ydb-short-name }}

## Проблема

Без правильной настройки TTL (Time To Live) для метрик мониторинга {{ ydb-short-name }} происходит неконтролируемое накопление данных, что приводит к:

- **Переполнению шардов мониторинга** - превышение лимитов хранения данных
- **Увеличению стоимости хранения** - рост расходов на инфраструктуру мониторинга
- **Снижению производительности** - замедление работы систем мониторинга из-за большого объема данных
- **Нарушению SLA** - возможные сбои в работе систем алертинга

В Yandex Monitoring по умолчанию TTL для метрик отключен, что требует явной настройки для предотвращения проблем.

## Решение

### Настройка TTL через Monium

Для настройки TTL метрик в Yandex Monitoring необходимо использовать интерфейс Monium:

1. **Откройте интерфейс Monium** и перейдите на страницу сервиса, кластера или шарда
2. **Укажите значение TTL** в параметре "TTL метрик"
3. **Сохраните настройки** - автоматическое удаление запустится в течение 10 минут

#### Иерархия настроек TTL

Настройки TTL переопределяются в следующем порядке (от менее строгих к более строгим):
- Сервис → Кластер → Шард

### Рекомендуемые значения TTL

Для различных типов метрик рекомендуются следующие значения TTL:

| Тип метрик | Рекомендуемый TTL | Обоснование |
|------------|-------------------|-------------|
| Операционные метрики (CPU, память, сеть) | 30 дней | Достаточно для анализа трендов и диагностики |
| Бизнес-метрики | 90-180 дней | Для долгосрочного анализа и отчетности |
| Отладочные метрики | 7-14 дней | Временные данные для отладки |
| Метрики высокой детализации | 3-7 дней | Высокочастотные данные с быстрым устареванием |

### Конфигурация в {{ ydb-short-name }}

Для настройки параметров хранения метрик в конфигурации {{ ydb-short-name }} используются следующие параметры:

```yaml
# Пример конфигурации YDB с настройками мониторинга
monitoring_config:
  retention_storage_interval: "PT24H"  # Интервал хранения метрик (24 часа)
  storage_retention_period: "P30D"     # Период хранения данных (30 дней)
```

## Примеры кода

### Хороший пример: Правильная настройка TTL

```yaml
# Конфигурация Prometheus для YDB с оптимальными настройками TTL
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "ydb_alerts.yml"

scrape_configs:
  - job_name: 'ydb-cluster'
    static_configs:
      - targets: ['ydb-node-1:8765', 'ydb-node-2:8765', 'ydb-node-3:8765']
    metrics_path: '/counters/counters=utils/prometheus'
    scrape_interval: 30s
    # Настройки хранения метрик
    metric_relabel_configs:
      - source_labels: [__name__]
        regex: '.*'
        action: keep
    # TTL на уровне Prometheus
    remote_write:
      - url: "http://monitoring-write:8428/api/v1/write"
        write_relabel_configs:
          - regex: "ydb_.*"
            action: keep
```

### Плохой пример: Отсутствие настройки TTL

```yaml
# ❌ НЕПРАВИЛЬНО: Отсутствие настроек TTL приводит к накоплению данных
global:
  scrape_interval: 5s  # Слишком частый сбор
  evaluation_interval: 5s

scrape_configs:
  - job_name: 'ydb-all-metrics'
    static_configs:
      - targets: ['ydb-cluster:8765']
    metrics_path: '/counters/prometheus'  # Сбор всех метрик без фильтрации
    scrape_interval: 5s  # Избыточная частота
    # Отсутствуют настройки retention
```

### Хороший пример: Настройка через Terraform

```hcl
# Настройка YDB базы с мониторингом через Terraform
resource "ycp_ydb_database" "production_db" {
  name        = "production-ydb"
  location_id = "ru-central1"
  
  serverless_database {}

  monitoring_config {
    enabled = true
    # Настройки TTL для метрик
    retention_storage_interval = "PT24H"
    storage_retention_period   = "P30D"
  }

  lifecycle {
    ignore_changes = [
      backup_config,
      monitoring_config,
      serverless_database,
    ]
  }
}
```

### Плохой пример: Игнорирование лимитов хранения

```yaml
# ❌ ОПАСНО: Отсутствие лимитов может привести к переполнению
monitoring_config:
  enabled: true
  # Отсутствуют настройки retention_storage_interval
  # Отсутствуют настройки storage_retention_period
  # Метрики будут накапливаться бесконечно
```

## Практические рекомендации

### 1. Мониторинг использования хранилища

Настройте алерты на использование дискового пространства:

```yaml
# Алерт на превышение лимита хранилища
- alert: YDBStorageUtilizationHigh
  expr: resources_storage_used_bytes / resources_storage_limit_bytes > 0.75
  for: 5m
  labels:
    severity: warning
  annotations:
    description: "YDB storage utilization is above 75%"
    summary: "High storage usage detected"
```

### 2. Регулярный аудит метрик

Проводите регулярный аудит собираемых метрик:

```bash
# Проверка актуальных метрик
curl -s http://ydb-node:8765/counters/prometheus | grep -v "^#" | wc -l

# Анализ самых объемных метрик
curl -s http://ydb-node:8765/counters/prometheus | \
  grep -v "^#" | cut -d' ' -f1 | sort | uniq -c | sort -nr | head -10
```

### 3. Оптимизация частоты сбора

Настройте оптимальную частоту сбора метрик:

```yaml
# Оптимальные интервалы для разных типов метрик
scrape_configs:
  - job_name: 'ydb-critical'
    targets: ['ydb-node:8765']
    metrics_path: '/counters/counters=utils/prometheus'
    scrape_interval: 15s  # Критичные метрики
    
  - job_name: 'ydb-business'
    targets: ['ydb-node:8765']
    metrics_path: '/counters/counters=business/prometheus'
    scrape_interval: 60s  # Бизнес-метрики
```

## Заключение

Правильная настройка TTL для метрик мониторинга {{ ydb-short-name }} - критически важная задача для обеспечения стабильной работы инфраструктуры. Регулярный аудит настроек TTL и мониторинг использования хранилища помогут предотвратить проблемы с переполнением и обеспечить эффективное использование ресурсов.
