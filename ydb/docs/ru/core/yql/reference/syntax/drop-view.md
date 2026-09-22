# DROP VIEW

`DROP VIEW` удаляет [представление](../../../../concepts/datamodel/view).

## Синтаксис

```yql
DROP VIEW [IF EXISTS] <view_name>
```

### Параметры

* `IF EXISTS` — если представления с указанным именем нет, команда не возвращает ошибку.
* `<view_name>` — имя удаляемого представления.

## Примеры

Следующая команда удалит представление со списком современных сериалов:

```yql
DROP VIEW recent_series;
```

## См. также

* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)
