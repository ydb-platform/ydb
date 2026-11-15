### Сигнатура

```yql
ForceSpreadMembers(list_of_tuples:List<Tuple<String, T>>, result_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Список таплов: имя поля, значение поля;
2. Список всех возможных имен полей в структуре.

### Примеры

```yql
SELECT
  ForceSpreadMembers([('a',1),('a',2),('c',100)],['a','b']); -- (a: 2, b: null)
```

## StructUnion, StructIntersection, StructDifference, StructSymmetricDifference

Комбинируют две структуры одним из четырех способов, используя предоставленную функцию для слияния полей с одинаковыми именами:

* `StructUnion` добавляет в результат все поля обеих структур;
* `StructIntersection` — поля, которые есть в обеих структурах;
* `StructDifference` — поля которые есть в left, но которых нет в right;
* `StructSymmetricDifference` — все поля, которые есть только в одной из структур.

### Сигнатуры

```yql
StructUnion(left:Struct<...>, right:Struct<...>[, mergeLambda:(name:String, l:T1?, r:T2?)->T])->Struct<...>
StructIntersection(left:Struct<...>, right:Struct<...>[, mergeLambda:(name:String, l:T1?, r:T2?)->T])->Struct<...>
StructDifference(left:Struct<...>, right:Struct<...>)->Struct<...>
StructSymmetricDifference(left:Struct<...>, right:Struct<...>)->Struct<...>
```

Аргументы:

1. `left` - первая структура;
2. `right` - вторая структура;
3. `mergeLambda` - _(опционально)_ позволяет задать функцию для объединения полей (аргументы: имя поля, `Optional` значение поля в первой структуре, `Optional` значение поля во второй структуре - аргументы принимают значение `Nothing<T?>` в случае отсутствия соответствующего поля); по умолчанию выбирается значение поля из первой структуры, а если в первой отсутствует — из второй.

### Примеры
