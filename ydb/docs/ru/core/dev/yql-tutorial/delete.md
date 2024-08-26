# Удаление данных

{% note warning %}

{% include [OLAP_not_allow_text](../../_includes/not_allow_for_olap_text.md) %}

Вместо `DELETE FROM` для удаления данных из колоночных таблиц можно воспользоваться механизмом удаления строк по времени — [TTL](../../concepts/ttl.md). TTL можно задать при [создании](../../yql/reference/syntax/create_table.md) строковой или колоночной таблицы (`CREATE TABLE`) или при их [изменении](../../yql/reference/syntax/alter_table/index.md) (`ALTER TABLE`).

{% endnote %}

Удалите данные из строковой таблицы с помощью оператора [DELETE](../../yql/reference/syntax/delete.md).

{% include [yql-reference-prerequisites](_includes/yql_tutorial_prerequisites.md) %}

```sql
DELETE
FROM episodes
WHERE
    series_id = 2
    AND season_id = 5
    AND episode_id = 12
;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 2 AND season_id = 5;

-- YDB не знает об изменениях, имевших место в начале транзакции,
-- поэтому сначала выполняет чтение. Невозможно выполнить UPDATE или DELETE,
-- если таблица уже была изменена в рамках текущей транзакции. UPDATE ON и
-- DELETE ON позволяют читать, обновлять и удалять несколько строк из одной таблицы
-- в рамках одной транзакции.

$to_delete = (
    SELECT series_id, season_id, episode_id
    FROM episodes
    WHERE series_id = 1 AND season_id = 1 AND episode_id = 2
);

SELECT * FROM episodes WHERE series_id = 1 AND season_id = 1;

DELETE FROM episodes ON
SELECT * FROM $to_delete;

-- Посмотреть результат:
SELECT * FROM episodes WHERE series_id = 1 AND season_id = 1;
```
