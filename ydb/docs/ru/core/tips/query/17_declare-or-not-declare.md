# `DECLARE` or not `DECLARE`

Инструкция YQL `DECLARE` позволяет заранее объявить переменную и ее тип в запросе:

```sql
DECLARE $id AS Uint64;
```

Это позволяет YQL сохранять strict режим работы с запросами и исключить множество неоднозначностей в интерпретации запросов.

До версии `25.1` {{ ydb-short-name }} нельзя было писать запросы с параметрами, не объявляя их явно через `DECLARE`.

Начиная с версии {{ ydb-short-name }} `25.1` нет необходимости явно писать `DECLARE` для всех запросов с параметрами.

## Проблема

Если в запросе явно описан `DECLARE $list AS List<SomeType>`, а в переданных параметрах запроса по логике приложения передается `EmptyList`, то на запрос вернется ошибка, т.к. переданный параметр не соответствует декларированному.

```go
var ids []uint64
// logic to collect ids
db.Query().QueryResultSet(ctx, `
	DECLARE $ids AS List<Uint64>;
	SELECT * FROM t WHERE id IN $ids;
`, query.WithParameters(
	ydb.ParamsFromMap(map[string]any{
		"$ids": ids, // EmptyList or List<Uint64>
	}),
))
```

## Решение

Убрать из запроса явный `DECLARE`, тогда ошибка несовпадения типов пропадет


{% cut "Плохой пример" %}
```yql
DECLARE $ids AS List<Uint64>;

SELECT * FROM t WHERE id IN $ids; -- $ids как параметр имеет тип EmptyList
```
{% endcut %}

{% cut "Хороший пример" %}
```yql
SELECT * FROM t WHERE id IN $ids; -- $ids как параметр имеет тип EmptyList
```
{% endcut %}
