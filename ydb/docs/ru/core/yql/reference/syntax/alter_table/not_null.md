# Выставление и сброс ограничения `NOT NULL`

## Выставление `NOT NULL`

`SET NOT NULL` устанавливает ограничение `NOT NULL` для указанной колонки.

Например, следующий запрос установит ограничение `NOT NULL` для колонки `column_name` в таблице `table_name`:

```yql
ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;
```

### Примечания

* `SET NOT NULL` выполняется как фоновая операция и может занять длительное время: перед установкой ограничения YDB проверяет таблицу на наличие `NULL`-значений в указанной колонке.
* SQL-операция выполняется синхронно и ожидает завершения. При этом создаётся фоновая операция для наблюдаемости.
* За ходом выполнения операций можно следить с помощью [CLI-команды](../../../../reference/ydb-cli/operation-list.md) `ydb operation list setnotnull`. Также доступны команды, позволяющие [получить статус конкретной операции](../../../../reference/ydb-cli/operation-get.md), [отменить операцию](../../../../reference/ydb-cli/operation-cancel.md) или [удалить запись о завершённой операции](../../../../reference/ydb-cli/operation-forget.md).
* После запуска операции `SET NOT NULL` и до её завершения в указанную колонку нельзя записывать `NULL`-значения. При попытке записать такие значения, вы получите ошибку с текстом "\textit{`SET NOT NULL` operation is currently in progress for this column}".


## Сброс `NOT NULL`

`DROP NOT NULL` снимает ограничение `NOT NULL` с указанной колонки.

Например, следующий запрос снимет ограничение `NOT NULL` с колонки `column_name` в таблице `table_name`:

```yql
ALTER TABLE table_name ALTER COLUMN column_name DROP NOT NULL;
```

{% note warning %}

Операции `SET NOT NULL` и `DROP NOT NULL` поддерживаются только для [строковых таблиц](../../../../concepts/glossary.md#row-oriented-table).

{% endnote %}
