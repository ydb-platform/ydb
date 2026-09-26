# DROP TABLE

Удаляет указанную таблицу.{% if feature_mapreduce %}  Таблица по имени ищется в базе данных, заданной оператором [USE](use.md).{% endif %}

Если таблицы с таким именем не существует, возвращается ошибка.

Обозначения в блоке синтаксиса описаны в [{#T}](syntax-conventions.md).

## Синтаксис

```yql
DROP TABLE [IF EXISTS] <table_name>
```

### Параметры

* `IF EXISTS` — если таблицы с указанным именем нет, команда не возвращает ошибку.
* `<table_name>` — путь удаляемой таблицы. Допускается запись в виде абсолютного пути или имени в текущей базе данных. В одной команде можно указать только одну таблицу.

{% note info %}

При выборе имени таблицы учитывайте общие [правила именования схемных объектов](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

## Примеры

Удаление таблицы `my_table` в текущей базе данных:

```yql
DROP TABLE `my_table`;
```

Удаление таблицы по абсолютному пути:

```yql
DROP TABLE `/Root/test/my_table`;
```

Если таблица может отсутствовать:

```yql
DROP TABLE IF EXISTS `my_table`;
```

Чтобы удалить несколько таблиц, выполните отдельный оператор `DROP TABLE` для каждой из них:

```yql
DROP TABLE `series`;
DROP TABLE `seasons`;
DROP TABLE `episodes`;
```

## См. также

* [CREATE TABLE](create_table/index.md)
* [ALTER TABLE](alter_table/index.md)
* [TRUNCATE TABLE](truncate-table.md)
* [DROP EXTERNAL TABLE](drop-external-table.md)
