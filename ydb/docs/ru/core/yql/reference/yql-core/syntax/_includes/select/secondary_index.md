# VIEW (INDEX)

{% if backend_name == "YDB" %}

{% include [olap_warning_note](../../../../../_includes/not_allow_for_olap_note.md) %}

{% endif %}

Чтобы сделать запрос `SELECT` по вторичному индексу строковой таблицы, используйте конструкцию:

``` yql
SELECT *
    FROM TableName VIEW IndexName
    WHERE …
```

**Примеры**

* Выбрать все поля из строковой таблицы `series` по индексу `views_index` с условием `views >= someValue`:

  ``` yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id
      FROM series VIEW views_index
      WHERE views >= someValue
  ```

* Сделать [`JOIN`](../../join.md) строковых таблиц `series` и `users` c заданным полем `userName` по индексам `users_index` и `name_index` соответственно:

  ``` yql
  SELECT t1.series_id, t1.title
      FROM series VIEW users_index AS t1
      INNER JOIN users VIEW name_index AS t2
      ON t1.uploaded_user_id == t2.user_id
      WHERE t2.name == userName;
  ```
