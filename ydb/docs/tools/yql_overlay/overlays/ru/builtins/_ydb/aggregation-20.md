1. Можно воспользоваться описанными ниже агрегатными функциями с фиксированными сетками корзин: [LinearHistogram](#linearhistogram) или [LogarithmicHistogram](#linearhistogram).
2. Можно самостоятельно вычислить номер корзины для каждой строки и сделать по нему [GROUP BY](../../syntax/select/group-by.md).

При использовании [фабрики агрегационной функции](../basic.md#aggregationfactory) в качестве первого аргумента [AGGREGATE_BY](#aggregate-by) передается `Tuple` из значения и веса.

### Примеры
