## mysql2ydb {#about}

**mysql2ydb** — Go-утилита из [mysql-ydb-importer](https://github.com/ydb-platform/mysql-ydb-importer) для переноса MySQL «один к одному»: те же имена таблиц и колонок, постраничное чтение, возобновление после сбоя.

### Системные требования

| Компонент | Требование |
| --- | --- |
| Go | Версия из `go.mod` репозитория (для сборки) |
| ОС | Linux, macOS — там, где есть сеть до MySQL и {{ ydb-short-name }} |
| MySQL | У таблиц должен быть `PRIMARY KEY` (или схема в YDB подготовлена вручную + `-data-only`) |
| Конфиг | `~/.my.cnf` или флаг `-mysql` |

### Сборка

```bash
git clone https://github.com/ydb-platform/mysql-ydb-importer.git
cd mysql-ydb-importer
go build -o mysql2ydb ./cmd/mysql2ydb
```

Подробнее: [импорт из MySQL](../../../../integrations/data-migration/import-mysql.md).
