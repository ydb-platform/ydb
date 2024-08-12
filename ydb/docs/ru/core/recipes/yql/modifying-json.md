# Изменение JSON с помощью YQL

В памяти YQL работает с неизменяемыми значениями. Таким образом, когда запросу нужно изменить что-то внутри значения JSON, следует думать об этом как о создании нового значения из частей старого.

Данный пример запроса принимает входной JSON названный `$fields`, парсит его, заменяет ключ `a` на 0, удаляет ключ `d` и добавляет ключ `c` со значением 3:

```yql
$fields = '{"a": 1, "b": 2, "d": 4}'j;
$pairs = DictItems(Yson::ConvertToInt64Dict($fields));
$result_pairs = ListExtend(ListNotNull(ListMap($pairs, ($item) -> {
    $item = if ($item.0 == "a", ("a", 0), $item);
    return if ($item.0 == "d", null, $item);
})), [("c", 3)]);
$result_dict = ToDict($result_pairs);
SELECT Yson::SerializeJson(Yson::From($result_dict));
```

## Смотрите также

- [{#T}](../../yql/reference/udf/list/yson.md)
- [{#T}](../../yql/reference/builtins/list.md)
- [{#T}](../../yql/reference/builtins/dict.md)
- [{#T}](accessing-json.md)
