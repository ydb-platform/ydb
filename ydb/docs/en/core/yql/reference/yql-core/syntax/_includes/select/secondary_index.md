# VIEW (INDEX)

{% include [olap_warning_note](../../../../../_includes/not_allow_for_olap_note.md) %}

To make a `SELECT` by secondary index of row-oriented table statement, use the following:

```yql
SELECT *
    FROM TableName VIEW IndexName
    WHERE …
```

**Examples**

* Select all the fields from the `series` row-oriented table using the `views_index` index with the `views >=someValue` criteria:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id
      FROM series VIEW views_index
      WHERE views >= someValue
  ```

* [`JOIN`](../../join.md) the `series` and `users` row-oriented tables on the `userName` field using the `users_index` and `name_index` indexes, respectively:

  ```yql
  SELECT t1.series_id, t1.title
      FROM series VIEW users_index AS t1
      INNER JOIN users VIEW name_index AS t2
      ON t1.uploaded_user_id == t2.user_id
      WHERE t2.name == userName;
  ```

