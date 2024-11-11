# Создание и изменение групп колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

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

При помощи команды `ALTER COLUMN` можно изменить группу колонок для указанной колонки. Приведенный ниже код для колонки `release_date` в таблице `series_with_families` сменит группу колонок на `family_small`.

```yql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

Две предыдущие команды можно объединить в один вызов `ALTER TABLE`. Приведенный ниже код создаст в таблице `series_with_families` группу колонок `family_small` и установит её для колонки `release_date`.

```yql
ALTER TABLE series_with_families
  ADD FAMILY family_small (
      DATA = "ssd",
      COMPRESSION = "off"
  ),
  ALTER COLUMN release_date SET FAMILY family_small;
```

При помощи команды `ALTER FAMILY` можно изменить параметры группы колонок. Приведенный ниже код для группы колонок `default` в таблице `series_with_families` сменит тип хранилища на `hdd` (поддерживается только для [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) таблиц.):

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "hdd";
```

Приведенный ниже код для группы колонок `default` в таблице `series_with_families` сменит кодек сжатия на `lz4`:

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET COMPRESSION "lz4";
```

Приведенный ниже код для группы колонок `default` в таблице `series_with_families` сменит уровень сжатия кодека, если он поддерживает различные уровни сжатия (поддерживается только для [колоночных](../../../../concepts/datamodel/table.md#column-oriented-tables) таблиц):

```yql
ALTER TABLE series_with_families ALTER FAMILY default SET COMPRESSION_LEVEL 5;
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}

Могут быть указаны все параметры группы колонок, описанные в команде [`CREATE TABLE`](../create_table/secondary_index.md)