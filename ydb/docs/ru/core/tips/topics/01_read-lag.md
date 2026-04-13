# Мониторинг лага чтения из топиков

## Проблема

Мониторинг лага чтения позволяет отслеживать, насколько консьюмер отстает от продюсера, и своевременно выявлять проблемы с производительностью. Без надлежащего мониторинга можно столкнуться с:

- Потерей данных при удалении данных из топика по retention
- Переполнением дисков кластера при использовании important consumer-а
- Перегрузкой приложения при попытке "догнать" поток после длительного простоя

## Решение

### Ключевые метрики для мониторинга

{{ ydb-short-name }} предоставляет несколько метрик для мониторинга лага чтения:

- `topic.partition.committed_lag_messages_max` - максимальное по всем партициям количество невычитанных сообщений
- `topic.partition.read.idle_milliseconds_max` - максимальное по всем партициям время, когда из партиции ничего не читали

### Настройка мониторинга

#### Хороший пример: Комплексный мониторинг

```yaml
# Пример алерта для мониторинга лага чтения
alert_rules:
  - alert: TopicCommitLagHigh
    expr: |
      topic.partition.committed_lag_messages_max{topic="my-topic", consumer="my-consumer"} > 1000
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "Высокий лаг коммитов в топике {{ $labels.topic }}"
      description: "Лаг коммитов превышает 5 минут для консьюмера {{ $labels.consumer }}"

  - alert: TopicConsumerStalled
    expr: |
      topic.read.messages{topic="my-topic", consumer="my-consumer"} == 0
      and
      topic.partition.read.idle_milliseconds_max{topic="my-topic", consumer="my-consumer"} > 60000
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "Консьюмер {{ $labels.consumer }} остановил чтение"
      description: "Сообщения не читаются более 1 минуты"
```

#### Плохой пример: Недостаточный мониторинг

```yaml
# ❌ Плохо: Отслеживается только одна метрика без контекста
alert_rules:
  - alert: SimpleLagAlert
    expr: topic.read.lag_messages > 1000
    # Не учитывает топик и консьюмер
    # Нет проверки на активность чтения
    # Нет проверки чтения без коммитов
```

### Использование {{ ydb-short-name }} CLI для теста производительности чтения

#### Хороший пример: Комплексная проверка

```bash
# Получение статистики по топику
ydb -e $ENDPOINT -d $DATABASE topic stat get my-topic

# Мониторинг лага в реальном времени
ydb -e $ENDPOINT -d $DATABASE workload topic run read \
  --topic my-topic \
  --consumers 1 \
  --seconds 60 \
  --print-timestamp
```

#### Плохой пример: Поверхностная проверка

```bash
# ❌ Плохо: Проверка без контекста и агрегации
ydb topic read my-topic --limit 10
# Не показывает лаг, не учитывает партиции
```

### Рекомендации по настройке алертов

1. **Мониторинг коммитов**: Отслеживайте `topic.partition.committed_lag_messages_max` для обнаружения остановки коммитов. Особенно важно для important consumer-ов
2. **Проверка отсутствия чтения**: Установите пороги для `topic.partition.read.idle_milliseconds_max`, чтобы поймать ситуацию, когда читатель перестал приходить за новыми данными
