## Enum, AsEnum {#enum}

`Enum()` cоздает значение перечисления.

**Сигнатура**
```
Enum(String, Type<Enum<...>>)->Enum<...>
```

Аргументы:

* Строка с именем поля
* Тип перечисления

**Пример**
``` yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

`AsEnum()` создает значение [перечисления](../../../types/containers.md) с одним элементом. Это значение может быть неявно преобразовано к любому перечислению, содержащему такое имя.

**Сигнатура**
```
AsEnum(String)->Enum<'tag'>
```

Аргументы:

* Строка с именем элемента перечисления

**Пример**
``` yql
SELECT
   AsEnum("Foo");
```
