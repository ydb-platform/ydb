# Изменение состава колонок

{{ backend_name }} поддерживает возможность добавлять столбцы в {% if backend_name == "YDB" and oss == true %} строковые и колоночные таблицы{% else %} таблицы {% endif %}, а также удалять неключевые колонки из таблиц.

`ADD COLUMN` — добавляет столбец с указанными именем и типом. Приведенный ниже код добавит к таблице `episodes` столбец `views` с типом данных `Uint64`.

```yql
ALTER TABLE episodes ADD COLUMN views Uint64;
```

`DROP COLUMN` — удаляет столбец с указанным именем. Приведенный ниже код удалит столбец `views` из таблицы `episodes`.

```yql
ALTER TABLE episodes DROP COLUMN views;
```