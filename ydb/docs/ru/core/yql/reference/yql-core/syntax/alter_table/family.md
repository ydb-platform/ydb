# Создание и изменение групп колонок

{% if oss == true and backend_name == "YDB" %}

{% include [OLAP_not_allow_note](../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

Механизм [групп](../../../../concepts/datamodel/table.md#column-groups) колонок позволяет увеличить производительность операций неполного чтения строк путем разделения хранения колонок строковой таблицы на несколько групп. Наиболее часто используемый сценарий — организация хранения редко используемых атрибутов в отдельной группе колонок.

## Создание группы колонок
```ADD FAMILY``` — создаёт новую группу колонок в строковой таблице. Приведенный ниже код создаст в таблице ```series_with_families``` группу колонок ```family_small```.

```sql
ALTER TABLE series_with_families ADD FAMILY family_small (
    DATA = "ssd",
    COMPRESSION = "off"
);
```

## Изменение групп колонок

При помощи команды ```ALTER COLUMN``` можно изменить группу колонок для указанной колонки. Приведенный ниже код для колонки ```release_date``` в таблице ```series_with_families``` сменит группу колонок на ```family_small```.

```sql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

Две предыдущие команды можно объединить в один вызов ```ALTER TABLE```. Приведенный ниже код создаст в таблице ```series_with_families``` группу колонок ```family_small``` и установит её для колонки ```release_date```.

```sql
ALTER TABLE series_with_families
	ADD FAMILY family_small (
    	DATA = "ssd",
    	COMPRESSION = "off"
	),
	ALTER COLUMN release_date SET FAMILY family_small;
```

При помощи команды ```ALTER FAMILY``` можно изменить параметры группы колонок. Приведенный ниже код для группы колонок ```default``` в таблице ```series_with_families``` сменит тип хранилища на ```hdd```:

```sql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "hdd";
```

{% note info %}

Доступные типы устройств хранения зависят от конфигурации кластера {{ ydb-short-name }}.

{% endnote %}

Могут быть указаны все параметры группы колонок, описанные в команде [`CREATE TABLE`](../create_table/secondary_index.md)