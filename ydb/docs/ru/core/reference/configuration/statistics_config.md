# statistics_config

{% if feature_flags.enable_column_statistics == false %}
{% note warning "Экспериментальная функциональность" %}

Функциональность находится в разработке и по умолчанию выключена флагом [`enable_column_statistics`](feature_flags.md). Её поведение может измениться без сохранения обратной совместимости, а сама функциональность может так и не стать общедоступной. Включение флага может привести к ошибкам, нестабильной работе кластера или невозможности сменить версию YDB. Не включайте флаг в production-кластерах. [Подробнее](feature_flags.md#experimental).

{% endnote %}
{% endif %}

Секция `statistics_config` настраивает сбор статистики столбцов для [стоимостного оптимизатора](../../concepts/query_execution/optimizer.md#statistics).

## Параметры конфигурации

| Параметр | Тип | По умолчанию | Описание |
|:---------|:----|:-------------|:---------|
| `enable_background_column_stats_collection` | bool | `false` | Фоновый сбор статистики включён |

## Фоновый сбор {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` включает автоматический запуск `ANALYZE` для пользовательских колоночных таблиц. По умолчанию фоновый сбор выключен.

Повторный сбор планируется через 24 часа после начала предыдущего `ANALYZE`. Этот интервал фиксирован и не настраивается. Если уже выполняется сбор статистики для другой таблицы, новый запуск ждёт его завершения.

## Пример конфигурации

Включить фоновый сбор статистики:

```yaml
statistics_config:
  enable_background_column_stats_collection: true
```

## См. также

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
