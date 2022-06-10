## Callable {#callable}

Создать вызываемое значение с заданной сигнатурой из лямбда-функции. Обычно используется для того, чтобы размещать вызываемые значения в контейнерах.

**Сигнатура**
```
Callable(Type<Callable<(...)->T>>, lambda)->Callable<(...)->T>
```

Аргументы:

1. Тип;
2. Лямбда-функция.

**Примеры:**
``` yql
$lambda = ($x) -> {
    RETURN CAST($x as String)
};

$callables = AsTuple(
    Callable(Callable<(Int32)->String>, $lambda),
    Callable(Callable<(Bool)->String>, $lambda),
);

SELECT $callables.0(10), $callables.1(true);
```
