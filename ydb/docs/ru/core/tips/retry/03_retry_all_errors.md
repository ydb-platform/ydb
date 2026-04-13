# Обработка ошибок с правильной стратегией повторов

## Проблема

[Встроенные ретраеры](../../recipes/ydb-sdk/retry.md) в SDK различают типы ошибок по таблице [{#T}](../../reference/ydb-sdk/ydb-status-codes.md): например, [`OVERLOADED`](../../reference/ydb-sdk/ydb-status-codes.md#overloaded) обычно требует повторов с длинной задержкой, а [`PRECONDITION_FAILED`](../../reference/ydb-sdk/ydb-status-codes.md#precondition-failed) повторять бессмысленно — запрос не станет успешным без изменения условий.

## Решение

Использовать ретраеры в составе SDK (см. [{#T}](../../recipes/ydb-sdk/retry.md))


{% cut "Плохой пример" %}
```go
for {
	var rowCount int
	err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		res, err := s.Query(ctx, queryText)
		if err != nil {
			return err
		}
		defer res.Close(ctx)
		for {
			resultSet, err := res.NextResultSet(ctx)
			if err != nil {
				break
			}
			for {
				_, err := resultSet.NextRow(ctx)
				if err != nil {
					break
				}
				rowCount++
			}
		}
		return nil
	})
	if err == nil {
		fmt.Printf("Query executed successfully, rows: %d\n", rowCount)
		return nil
	}

	fmt.Printf("Error occurred: %v, retrying...\n", err)
	time.Sleep(time.Second) // Бесконечный ретрай - плохо!
}
```
{% endcut %}

{% cut "Хороший пример" %}
```go
var rowCount int
err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
	res, err := s.Query(ctx, queryText)
	if err != nil {
		return err
	}
	defer res.Close(ctx)
	for {
		resultSet, err := res.NextResultSet(ctx)
		if err != nil {
			break
		}
		for {
			_, err := resultSet.NextRow(ctx)
			if err != nil {
				break
			}
			rowCount++
		}
	}
	return nil
})
if err == nil {
	fmt.Printf("Query executed successfully, rows: %d\n", rowCount)
	return nil
}
```
{% endcut %}
