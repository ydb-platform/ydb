# Инвалидация блокировок транзакций

{{ ydb-short-name }} использует [optimistic concurrency control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) для обнаружения конфликтов с другими выполняющимися транзакциями. Если проверка блокировок в стадии коммита обнаруживает конфликтующие изменения, транзакция откатывается и должна быть выполнена заново. В этом случае {{ ydb-short-name }} возвращает ошибку `transaction locks invalidated`. Повторное выполнение значительного количества транзакций может замедлить ваше приложение.

{% note info %}

{{ ydb-short-name }} SDK предоставляет встроенный механизм обработки временных ошибок. Подробнее см. [{#T}](../../../reference/ydb-sdk/error_handling.md).

{% endnote %}


## Диагностика

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/transaction-lock-invalidation.md) %}

## Рекомендации

Примите во внимание следующие рекомендации:

- Чем дольше длится транзакция, тем выше вероятность возникновения ошибки `transaction locks invalidated`.

    По возможности избегайте [интерактивных транзакций](../../../concepts/glossary.md#interactive-transaction). Лучшим подходом является использование одного YQL-запроса с командами `BEGIN;` и `COMMIT;` для выбора данных, обновления данных и выполнения коммита транзакции.

<<<<<<< HEAD
    Если без интерактивных транзакций не обойтись, выполняйте коммит транзакции в последнем запросе.
=======
### Анализ через системные представления

Для анализа конфликтов блокировок доступны следующие системные представления:

#### Анализ на уровне запросов
>>>>>>> 4ed6bd91bf6 (Docs: Add more sys_view about broken locks (#39965))

- Проанализируйте диапазон первичных ключей, в которых происходят конфликтующие изменения, и попытайтесь изменить логику приложения, чтобы уменьшить количество конфликтов.

<<<<<<< HEAD
    Например, если одна строка с общим балансовым значением часто обновляется, разделите эту строку на сто строк и рассчитайте общий баланс как сумму этих строк. Это значительно сократит количество ошибок `transaction locks invalidated`.
=======
```sql
SELECT QueryText, LocksBrokenAsBreaker, LocksBrokenAsVictim
FROM `.sys/query_metrics_one_minute`
WHERE LocksBrokenAsBreaker > 0 OR LocksBrokenAsVictim > 0
ORDER BY LocksBrokenAsBreaker + LocksBrokenAsVictim DESC;
```

| Колонка | Описание |
|:--------|:---------|
| `LocksBrokenAsBreaker` | Сколько раз этот запрос сломал чужие блокировки |
| `LocksBrokenAsVictim` | Сколько раз блокировки этого запроса были сняты |

Запросы с высоким `LocksBrokenAsBreaker` — нарушители: именно они вызывают откаты других транзакций. Запросы с высоким `LocksBrokenAsVictim` — жертвы.

#### Анализ на уровне партиций

Для анализа сломанных блокировок на уровне партиций таблиц используйте следующие системные представления:

* [`.sys/partition_stats`](../../../dev/system-views.md#partitions) — текущая статистика по партициям, содержит кумулятивное поле `LocksBroken`
* [`.sys/top_partitions_by_tli_one_minute`](../../../dev/system-views.md#top-tli-partitions) — топ-10 партиций с ненулевым числом сломанных блокировок за минутный интервал
* [`.sys/top_partitions_by_tli_one_hour`](../../../dev/system-views.md#top-tli-partitions) — топ-10 партиций с ненулевым числом сломанных блокировок за часовой интервал

Пример запроса для поиска партиций с наибольшим числом сломанных блокировок:

```sql
SELECT
    Path,
    SUM(LocksBroken) as TotalLocksBroken
FROM `.sys/partition_stats`
GROUP BY Path
ORDER BY TotalLocksBroken DESC
LIMIT 10;
```

Пример запроса для просмотра истории сломанных блокировок по партициям:

```sql
SELECT
    IntervalEnd,
    LocksBroken,
    Path
FROM `.sys/top_partitions_by_tli_one_hour`
WHERE IntervalEnd BETWEEN Timestamp("2000-01-01T00:00:00Z") AND Timestamp("2099-12-31T00:00:00Z")
ORDER BY IntervalEnd DESC, LocksBroken DESC
LIMIT 100;
```
>>>>>>> 4ed6bd91bf6 (Docs: Add more sys_view about broken locks (#39965))
