# DROP VIEW

`DROP VIEW` удаляет [представление](../../../../concepts/datamodel/view).

## Синтаксис

```sql
DROP VIEW <имя>
```

### Параметры

* `имя` - имя представления, подлежащего удалению.

## Примеры

Следующая команда удалит представление со списком современных сериалов:

```sql
DROP VIEW recent_series;
```

## См. также

* [CREATE VIEW](create-view.md)
* [ALTER VIEW](alter-view.md)