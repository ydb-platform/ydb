## Udf {#udf}

Строит `Callable` по заданному названию функции и опциональным `external user types`, `RunConfig` и `TypeConfig`.

* `Udf(Foo::Bar)` — Функция `Foo::Bar` без дополнительных параметров.
* `Udf(Foo::Bar)(1, 2, 'abc')` — Вызов udf `Foo::Bar`.
* `Udf(Foo::Bar, Int32, @@{"device":"AHCI"}@@ as TypeConfig")(1, 2, 'abc')` — Вызов udf `Foo::Bar` с дополнительным типом `Int32` и указанным `TypeConfig`.
* `Udf(Foo::Bar, "1e9+7" as RunConfig")(1, 'extended' As Precision)` — Вызов udf `Foo::Bar` с указанным `RunConfig` и именоваными параметрами.

### Сигнатуры

```yql
Udf(Callable[, T1, T2, ..., T_N][, V1 as TypeConfig][,V2 as RunConfig]])->Callable
```

Где `T1`, `T2`, и т. д. -- дополнительные (`external`) пользовательские типы.

### Примеры

```yql
$IsoParser = Udf(DateTime2::ParseIso8601);
SELECT $IsoParser("2022-01-01");
```

```yql
SELECT Udf(Unicode::IsUtf)("2022-01-01")
```

```yql
$config = @@{
    "name":"MessageFoo",
    "meta": "..."
}@@;
SELECT Udf(Protobuf::TryParse, $config As TypeConfig)("")
```
