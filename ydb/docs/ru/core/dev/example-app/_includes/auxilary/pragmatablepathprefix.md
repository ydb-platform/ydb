`PRAGMA TablePathPrefix` добавляет указанный префикс к путям таблиц внутри БД. Работает по принципу объединения путей в файловой системе — поддерживает ссылки на родительский каталог и не требует добавления слеша справа. Например:

```sql
PRAGMA TablePathPrefix = "/cluster/database";
SELECT * FROM episodes;
```

Подробнее о PRAGMA YQL можно прочитать в [документации YQL](../../../../yql/reference/index.md).
