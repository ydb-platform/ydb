# Создание таблицы

Создайте таблицы и задайте для них схемы данных с помощью конструкции [CREATE TABLE](../../yql/reference/syntax/create_table/index.md).

{% note info %}

Ключевые слова не чувствительны к регистру и написаны заглавными буквами для наглядности.

{% endnote %}

```sql
CREATE TABLE series         -- series — имя таблицы.
(                           -- Должно быть уникальным в пределах каталога.
    series_id Uint64,
    title Utf8,
    series_info Utf8,
    release_date Uint64,
    PRIMARY KEY (series_id) -- Первичный ключ — это столбец или
                            -- комбинация столбцов, однозначно идентифицирующих
                            -- каждую строку в таблице (может содержать только
                            -- неповторяющиеся значения). Для таблицы может быть
                            -- указан только один первичный ключ. Для каждой таблицы
                            -- YDB указание первичного ключа является обязательным.
);

CREATE TABLE seasons
(
    series_id Uint64,
    season_id Uint64,
    title Utf8,
    first_aired Uint64,
    last_aired Uint64,
    PRIMARY KEY (series_id, season_id)
);

CREATE TABLE episodes
(
    series_id Uint64,
    season_id Uint64,
    episode_id Uint64,
    title Utf8,
    air_date Uint64,
    PRIMARY KEY (series_id, season_id, episode_id)
);

COMMIT;
```
