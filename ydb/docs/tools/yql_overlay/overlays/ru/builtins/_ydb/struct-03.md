### Сигнатура

```yql
AddMember(struct:Struct<...>, new_key:String, new_value:T) -> Struct<...>
```

Аргументы:

1. struct - исходная структура;
2. new_key - имя нового поля;
3. new_value - значение нового поля.

### Примеры

```yql
$struct = <|a:1|>;
SELECT
  AddMember(
    $struct,
    "b",
    2
  ) AS ab; -- ("a": 1, "b": 2)
```
