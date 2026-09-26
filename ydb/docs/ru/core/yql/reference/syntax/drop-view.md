# DROP VIEW

`DROP VIEW` удаляет [представление](../../../concepts/datamodel/view.md).

Обозначения в блоке синтаксиса описаны в [{#T}](syntax-conventions.md).

## Синтаксис

```yql
DROP VIEW [IF EXISTS] <view_name>
```

### Параметры

* `IF EXISTS` — если представления с указанным именем нет, команда не возвращает ошибку.
* `<view_name>` — имя удаляемого представления. Допускается запись в виде абсолютного пути или имени в текущей базе данных.

{% note info %}

При выборе имени для представления учитывайте общие [правила именования схемных объектов](../../../concepts/datamodel/cluster-namespace.md#object-naming-rules).

{% endnote %}

## Примеры

Удаление представления `recent_series` в текущей базе данных:

```yql
DROP VIEW recent_series;
```

Удаление представления по абсолютному пути:

```yql
DROP VIEW `/Root/test/recent_series`;
```

Если представление может отсутствовать:

```yql
DROP VIEW IF EXISTS recent_series;
```

## См. также

* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)
