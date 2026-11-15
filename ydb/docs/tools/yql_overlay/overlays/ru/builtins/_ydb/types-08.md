### Сигнатура

```yql
OptionalType(Type)->опциональный Type
```

Добавляет в переданный тип возможность содержать `NULL`.

### Примеры

```yql
SELECT FormatType(OptionalType(DataType("Bool"))); -- Bool?
SELECT FormatType(OptionalType(ParseType("List<String?>"))); -- List<String?>?
```
