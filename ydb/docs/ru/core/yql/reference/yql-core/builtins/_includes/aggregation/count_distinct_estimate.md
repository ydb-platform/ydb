## CountDistinctEstimate, HyperLogLog и HLL {#countdistinctestimate}

**Сигнатура**
```
CountDistinctEstimate(T)->Uint64?
HyperLogLog(T)->Uint64?
HLL(T)->Uint64?
```

Примерная оценка числа уникальных значений по алгоритму [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Логически делает то же самое, что и [COUNT(DISTINCT ...)](#count), но работает значительно быстрее ценой некоторой погрешности.

Аргументы:

1. Значение для оценки;
2. Точность (от 4 до 18 включительно, по умолчанию 14).

Выбор точности позволяет разменивать дополнительное потребление вычислительных ресурсов и оперативной памяти на уменьшение погрешности.

На данный момент все три функции являются алиасами, но в будущем `CountDistinctEstimate` может начать использовать другой алгоритм.

**Примеры**
``` yql
SELECT
  CountDistinctEstimate(my_column)
FROM my_table;
```

``` yql
SELECT
  HyperLogLog(my_column, 4)
FROM my_table;
```

