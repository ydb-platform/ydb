# Изменение дополнительных параметров таблиц

Большинство параметров строковых и колоночных таблиц в {{ ydb-short-name }}, приведенных на странице [описания таблицы]({{ concept_table }}), можно изменить командой ```ALTER```.

В общем случае команда для изменения любого параметра таблицы выглядит следующим образом:

```sql
ALTER TABLE table_name SET (key = value);
```

```key``` — имя параметра, ```value``` — его новое значение.

Список дополнительных параметров, которые можно изменить для таблиц:

* `AUTO_PARTITIONING_BY_SIZE` — автоматическое партиционирование таблицы:
    ```sql
    ALTER TABLE series SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
    ```
* `AUTO_PARTITIONING_PARTITION_SIZE_MB` — размер партиции таблицы:    
    ```sql
    ALTER TABLE series SET (AUTO_PARTITIONING_PARTITION_SIZE_MB = 512);
    ```
* `TTL` — время жизни записи в таблицы:
    ```sql
    ALTER TABLE series SET (TTL = Interval("PT0S") ON expire_at);
    ```

## Сброс дополнительных параметров таблицы {#additional-reset}

Некоторые параметры таблиц в {{ ydb-short-name }}, приведенные на странице [описания таблицы]({{ concept_table }}), можно сбросить командой ```ALTER```. Команда для сброса параметра таблиц выглядит следующим образом:

```sql
ALTER TABLE table_name RESET (key);
```

```key``` — имя параметра.

Например, такая команда сбросит (удалит) настройки TTL для строковых или колоночных таблиц:

```sql
ALTER TABLE series RESET (TTL);
```

Сброс параметров `AUTO_PARTITIONING_BY_SIZE` и `AUTO_PARTITIONING_PARTITION_SIZE_MB` не поддерживается для колоночных таблиц.