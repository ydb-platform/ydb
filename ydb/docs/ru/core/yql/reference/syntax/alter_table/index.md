# ALTER TABLE

При помощи команды `ALTER TABLE` можно изменить состав колонок и дополнительные параметры {% if backend_name == "YDB" and oss == true %}[строковых](../../../../concepts/datamodel/table.md#row-tables) и [колоночных](../../../../concepts/datamodel/table.md#colums-tables) таблиц{% else %}таблиц {% endif %}. В одной команде можно указать несколько действий. В общем случае команда `ALTER TABLE` выглядит так:


```yql
ALTER TABLE table_name action1, action2, ..., actionN;
```

`action` — это любое действие по изменению таблицы, из описанных ниже:

* [Переименование таблицы](rename.md).
* Работа с [колонками](columns.md) строковой и колоночной таблиц.
* Добавление или удаление [потока изменений](changefeed.md).
* Работа с [индексами](indexes.md).
* Работа с [группами колонок](family.md) строковой таблицы.
{% if backend_name == "YDB" and oss == true %}
* Изменение [дополнительных параметров таблиц](set.md).
{% endif %}