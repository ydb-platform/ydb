# Создание и изменение групп колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLTP_only_allow_note](../../../../_includes/only_allow_for_oltp_note.md) %}

{% endif %}

Механизм {% if oss == true and backend_name == "YDB" %}[групп](../../../../concepts/datamodel/table.md#column-groups){% else %}групп{% endif %} колонок позволяет увеличить производительность операций неполного чтения строк путем разделения хранения колонок строковой таблицы на несколько групп. Наиболее часто используемый сценарий — организация хранения редко используемых атрибутов в отдельной группе колонок.


## Создание группы колонок

`ADD FAMILY` — создаёт новую группу колонок в строковой таблице. Приведенный ниже код создаст в таблице `series_with_families` группу колонок `family_small`.

```yql
ALTER TABLE series_with_families ADD FAMILY family_small (
    DATA = "ssd",
    COMPRESSION = "off"
);
```

## Изменение групп колонок

При помощи команды `ALTER COLUMN` можно изменить группу колонок для указанной колонки. Приведённый ниже код для колонки `release_date` в таблице `series_with_families` сменит группу колонок на `family_small`.

```yql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

Две предыдущие команды можно объединить в один вызов `ALTER TABLE`. Приведённый ниже код создаст в таблице `series_with_families` группу колонок `family_small` и установит её для колонки `release_date`.

```yql
ALTER TABLE series_with_families
  ADD FAMILY family_small (
      DATA = "ssd",
      COMPRESSION = "off"
  ),
  ALTER COLUMN release_date SET FAMILY family_small;
```

При помощи команды `ALTER FAMILY` можно изменить параметры группы колонок.

### Изменение типа хранилища

Приведённый ниже код для группы колонок `default` в таблице `series_with_families` сменит тип хранилища на `rot`:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "rot";
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}

### Изменение кодека сжатия

Приведённый ниже код для группы колонок `default` в таблице `series_with_families` сменит кодек сжатия на `lz4`:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET COMPRESSION "lz4";
```

### Изменение режима кэширования

При переключении режима кэширования на `in_memory` для существующей таблицы через команду `ALTER TABLE`, все страницы, которые ещё не находятся в памяти, будут подгружены автоматически.

Если для таблицы ранее был активирован режим `in_memory`, а затем через `ALTER TABLE` установлен режим кэширования `regular`, все находящиеся в памяти страницы сохраняются, но впоследствии могут вытесняться из памяти согласно общей политике кэширования.

Приведённый ниже код для группы колонок `default` в таблице `series_with_families` сменит [режим кэширования](../../../../concepts/datamodel/table.md#cache-modes) на `in_memory`:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET CACHE_MODE "in_memory";
```

Могут быть указаны все параметры группы колонок, описанные в команде [`CREATE TABLE`](../create_table/secondary_index.md)