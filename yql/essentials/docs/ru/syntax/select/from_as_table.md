# FROM AS_TABLE

Обращение к именованным выражениям как к таблицам с помощью функции `AS_TABLE`.

`AS_TABLE($variable)` позволяет использовать значение `$variable` в качестве источника данных для запроса. При этом переменная `$variable` должна иметь тип `List<Struct<...>>`, `Optional<Struct<...>>` или быть лямбдой без аргументов, возвращающей `Stream<Struct<...>>`.

При этом, если `$variable` имеет тип `Optional<List<Struct<...>>>`, то будет ошибка запроса, т.к. `Optional` в этом контексте рассматривается как список длины 1. Привести тип `Optional<List<Struct<...>>>` к типу `List<Struct<...>>` можно, например, с помощью функции [Coalesce](../../builtins/basic.md#coalesce) или [Unwrap](../../builtins/basic.md#unwrap).

## Пример

```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

SELECT Key, Value FROM AS_TABLE($data);
```
