### Сигнатура

```yql
TryMember(struct:Struct<...>, key:String, default_value:T) -> T
TryMember(struct:Struct<...>?, key:String, default_value:T) -> T?
```

Аргументы:

1. struct - исходная структура;
2. key - имя поля;
3. default_value - значение по умолчанию если поле отсутствует

{% note info %}

Имя поля (key) не может зависеть от данных или от аргументов лямбды. В этом случае функция TryMember не может быть протипизирована.

{% endnote %}

### Примеры
