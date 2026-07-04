## dbt {#about}

**dbt** (data build tool) — инструмент для SQL-трансформаций: модели описываются как `SELECT`, зависимости строятся автоматически, результат **материализуется** в таблицы хранилища.

Для {{ ydb-short-name }} используется коннектор **[dbt-ydb](https://github.com/ydb-platform/dbt-ydb)** (`pip install dbt-ydb`). dbt **не подключается к исходной СУБД напрямую** — источник должен быть доступен в YQL (обычно через External Data Source из раздела «Федеративные запросы»).

### Системные требования

| Компонент | Требование |
| --- | --- |
| Python | **3.10+** |
| dbt | **dbt Core 1.8+** (не dbt Fusion 2.0) |
| pip | Установка `dbt-ydb` |
| Кластер {{ ydb-short-name }} | Рабочий endpoint, права на создание таблиц |

### Установка

```bash
pip install dbt-ydb
dbt --version
```

Профиль подключения к {{ ydb-short-name }} — в `~/.dbt/profiles.yml`. Пример:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: ydb
      host: localhost
      port: 2136
      database: /local
      schema: mydb
```

Проверка: `dbt debug`.

Подробнее: [интеграция dbt](../../../../integrations/migration/dbt.md).

### Ограничения для миграции

* Для полной миграции используйте materialization `table` или `incremental`.
* **Seeds** (CSV) — только для небольших справочников, не для больших баз.
