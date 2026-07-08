# Совместимость {{ ydb-short-name }} с PostgreSQL

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

Совместимость с PostgreSQL — это экспериментальный механизм выполнения SQL-запросов в диалекте PostgreSQL на инфраструктуре {{ ydb-short-name }}. Он позволял отправлять запросы в синтаксисе PostgreSQL через инструменты {{ ydb-short-name }} (с маркером `--!syntax_pg`) или через сетевой протокол PostgreSQL (pgwire).

**Эта функциональность удалена.** {{ ydb-short-name }} больше не принимает маркер `--!syntax_pg`, не предоставляет сетевой протокол PostgreSQL и не содержит утилиту `ydb tools pg-convert`.

Для миграции приложений с PostgreSQL на {{ ydb-short-name }} перепишите запросы на [YQL](../yql/reference/index.md) или используйте [конвертер SQL-диалектов](../integrations/sql-translation/sql-dialect-converter.md). Для импорта данных см. [миграцию данных](../integrations/data-migration/index.md) и [импорт из файлов](../reference/ydb-cli/export-import/import-file.md).

Остальные страницы в этом разделе описывают конструкции PostgreSQL, которые поддерживались ранее, и сохранены для справки.

В этом разделе описываются:

- [{#T}](connect.md)
- [{#T}](interoperability.md)
- [{#T}](functions.md)
- [{#T}](import.md)
