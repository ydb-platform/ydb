## Pickle, Unpickle {#pickle}

`Pickle()` и `StablePickle()` сериализуют произвольный объект в последовательность байт, если это возможно. Типовыми несериализуемыми объектами являются Callable и Resource. Формат сериализации не версионируется, допускается использовать в пределах одного запроса. Для типа Dict функция StablePickle предварительно сортирует ключи, а для Pickle порядок элементов словаря в сериализованном представлении не определен.

`Unpickle()` — обратная операция (десериализация), где первым аргументом передается тип данных результата, а вторым — строка с результатом `Pickle()` или `StablePickle()`.

**Сигнатуры**
```
Pickle(T)->String
StablePickle(T)->String
Unpickle(Type<T>, String)->T
```

Примеры:
``` yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0; -- в реальности лучше использовать TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```
