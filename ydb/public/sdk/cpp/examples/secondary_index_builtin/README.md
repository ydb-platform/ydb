# YQL (only version 1) query with secondary index

## Create table

```cl
USE plato;

CREATE TABLE TableName (
    Key1 Type,
    Key2 Type,
    …
    PRIMARY KEY (SomeKey),
    INDEX IndexName1 GLOBAL ON (SomeKey),
    INDEX IndexName2 GLOBAL ON (SomeKey1, SomeKey2, ...)
);
COMMIT;
```
Пример создания таблицы series c полями series_id, title, info, release_date, views, uploaded_user_id, с первичным ключом series_id, и вторичными индексами views_index по полю views и users_index по полю uploaded_user_id.

```cl
USE plato;

CREATE TABLE series (
    series_id Uint64,
    title Utf8,
    info Utf8,
    release_date Datetime,
    views Uint64,
    uploaded_user_id Uint64,
    PRIMARY KEY (series_id),
    INDEX views_index GLOBAL ON (views),
    INDEX users_index GLOBAL ON (uploaded_user_id)
);
COMMIT;
```
## Select 

```cl
SELECT * 
FROM TableName VIEW IndexName
WHERE …
```

Пример SELECT запроса полей series_id, title, views, ... из таблицы series по индексу views_index 
с условием views >= someValue

```cl
SELECT series_id, title, info, release_date, views, uploaded_user_id
FROM series view views_index
WHERE views >= someValue
```

Пример SELECT с JOIN таблиц series и users по индексам users_index и name_index соответственно c заданным username.

```cl
SELECT t1.series_id, t1.title
FROM series VIEW users_index AS t1
INNER JOIN users VIEW name_index AS t2
ON t1.uploaded_user_id == t2.user_id
WHERE t2.name == username;
```
