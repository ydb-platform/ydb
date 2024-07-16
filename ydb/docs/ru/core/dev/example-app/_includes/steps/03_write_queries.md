## Запись данных {#write-queries}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note_main.md) %}

Выполняется запись данных в созданные строковые таблицы с использованием команды [`UPSERT`](../../../../yql/reference/syntax/upsert_into.md) языка запросов [YQL](../../../../yql/reference/index.md). Применяется режим передачи запроса на изменение данных с автоматическим подтверждением транзакции в одном запросе к серверу.
