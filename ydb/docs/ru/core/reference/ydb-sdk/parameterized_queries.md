## Параметризованные запросы

{{ ydb-short-name }} поддерживает и рекомендует к использованию так называемые [параметризованные запросы](https://en.wikipedia.org/wiki/Prepared_statement). В таких запросах данные передаются отдельно от самого тела запроса, а в SQL-запросе используются специальные параметры для обозначения местоположения данных.

Запрос с данными в теле запроса:

```sql
SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = 15 AND sa.season_id = 3
```

Соответствующий ему параметризованный запрос:

```sql
DECLARE $seriesId AS Uint64;
DECLARE $seasonId AS Uint64;

SELECT sa.title AS season_title, sr.title AS series_title
FROM seasons AS sa INNER JOIN series AS sr ON sa.series_id = sr.series_id
WHERE sa.series_id = $seriesId AND sa.season_id = $seasonId
```

Параметризованные запросы записываются в форме шаблона, в котором определенного вида имена заменяются конкретными параметрами при каждом выполнении запроса. Лексемы начинающиеся со знака ```$``` такие, как```$seriesId``` и ```$seasonId``` в запросе выше, используются для обозначения параметров.

Параметризованные запросы обеспечивают следующие преимущества:

* При повторяющихся запросах сервер базы данных имеет возможность кэшировать план запроса для параметризованных запросов. Это радикально снижает потребление CPU и повышает пропускную способность системы;
* Использование параметризованных запросов спасает от уязвимостей вида  [SQL Injection](https://en.wikipedia.org/wiki/SQL_injection).

{{ ydb-short-name }} SDK автоматически кэшируют планы параметризованных запросов по умолчанию, для этого обычно используется настройка ```KeepInCache = true```.
