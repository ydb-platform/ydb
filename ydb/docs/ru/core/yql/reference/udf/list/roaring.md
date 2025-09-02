# Roaring

## Введение

Битовые множества, также называемые битовыми картами (bitmaps), обычно используются в качестве быстрых структур данных. К сожалению, они могут потреблять слишком много памяти. Чтобы уменьшить занимаемый объём, применяется сжатие.

Roaring bitmaps — это сжатые битовые карты, которые, как правило, превосходят обычные алгоритмы сжатия битовых карт, такие как WAH, EWAH или Concise. В некоторых случаях Roaring bitmaps могут быть в сотни раз быстрее, а также часто обеспечивают значительно лучшее сжатие. Они могут быть даже быстрее, чем несжатые битовые карты.

## Реализация

Работать с Roaring bitmap в {{ ydb-short-name }} можно с помощью набора пользовательских функций UDF в модуле `Roaring`. Они предоставляют возможность работать с 32-битными Roaring bitmap. Для этого данные должны быть сериализованы в формате 32-битных битовых масок, описанном в [спецификации](https://github.com/RoaringBitmap/RoaringFormatSpec?tab=readme-ov-file#standard-32-bit-roaring-bitmap). Это можно сделать с помощью функции, доступной в библиотеке Roaring bitmap. Такие библиотеки есть для многих языков, например, для [Go](https://github.com/RoaringBitmap/roaring).

Сериализованную битовую маску приложение может сохранить в колонке с типом `String`.

Для работы с Roaring bitmap данные из строкового типа необходимо десериализовать в тип [Resource<roaring_bitmap>](../../types/special.md). Для сохранения нужно выполнить обратную операцию. После этого приложение сможет прочитать обновлённую битовую маску из {{ ydb-short-name }} и десериализовать её уже на своей стороне.

## Доступные методы

```yql
Roaring::Deserialize(String{Flags:AutoMap})->Resource<roaring_bitmap>
Roaring::FromUint32List(List<Uint32>{Flags:AutoMap})->Resource<roaring_bitmap>
Roaring::Serialize(Resource<roaring_bitmap>{Flags:AutoMap})->String
Roaring::Uint32List(Resource<roaring_bitmap>{Flags:AutoMap})->List<Uint32>

Roaring::Cardinality(Resource<roaring_bitmap>{Flags:AutoMap})->Uint32

Roaring::Or(Resource<roaring_bitmap>{Flags:AutoMap}, Resource<roaring_bitmap>{Flags:AutoMap})->Resource<roaring_bitmap>
Roaring::OrWithBinary(Resource<roaring_bitmap>{Flags:AutoMap}, String{Flags:AutoMap})->Resource<roaring_bitmap>

Roaring::And(Resource<roaring_bitmap>{Flags:AutoMap}, Resource<roaring_bitmap>{Flags:AutoMap})->Resource<roaring_bitmap>
Roaring::AndWithBinary(Resource<roaring_bitmap>{Flags:AutoMap}, String{Flags:AutoMap})->Resource<roaring_bitmap>

Roaring::AndNot(Resource<roaring_bitmap>{Flags:AutoMap}, Resource<roaring_bitmap>{Flags:AutoMap})->Resource<roaring_bitmap>
Roaring::AndNotWithBinary(Resource<roaring_bitmap>{Flags:AutoMap}, String{Flags:AutoMap})->Resource<roaring_bitmap>

Roaring::RunOptimize(Resource<roaring_bitmap>{Flags:AutoMap})->Resource<roaring_bitmap>
```

## Сериализация и десериализация

Для создания `Resource<roaring_bitmap>` доступны две функции: `Deserialize` и `FromUint32List`. Вторая функция позволяет создавать Roaring bitmap из списков беззнаковых целых чисел, то есть без необходимости использовать библиотечный код Roaring bitmaps для создания бинарного представления.

YDB не сохраняет данные с типом `Resource`, поэтому созданную битовую маску необходимо преобразовать в бинарное представление с помощью метода `Serialize`.

Чтобы использовать полученную битовую маску, например, в условии `WHERE`, предусмотрен метод `Uint32List`, который возвращает список беззнаковых целых чисел из `Resource<roaring_bitmap>`.

## Битовые операции

В настоящий момент поддерживаются три модифицирующие бинарные операции с битовыми масками:

- `Or`
- `And`
- `AndNot`

Модифицирующие операции означают, что они изменяют `Resource<roaring_bitmap>`, переданный в первом аргументе. У каждой из этих операций есть версия с суффиксом `WithBinary`, которая позволяет работать с бинарным представлением, не прибегая к его десериализации в тип `Resource<roaring_bitmap>`. Реализация этих методов всё равно выполняет десериализацию для выполнения операции, но не создаёт промежуточный `Resource`, что позволяет экономить ресурсы.

## Прочие операции

Для получения кардинальности (количества бит, установленных в 1) предусмотрена функция `Cardinality`.

После построения или модификации битовой маски её можно оптимизировать с помощью метода `RunOptimize`. Это связано с тем, что внутренний формат Roaring bitmap может использовать контейнеры с более эффективным представлением для разных последовательностей бит.

## Примеры

```yql
$b = Roaring::FromUint32List(AsList(42));
$b = Roaring::Or($b, Roaring::FromUint32List(AsList(56)));


SELECT Roaring::Uint32List($b) AS `Or`; -- [42, 56]
```


```yql
$b1 = Roaring::FromUint32List(AsList(10, 567, 42));
$b2 = Roaring::FromUint32List(AsList(42));

$b2ser = Roaring::Serialize($b2); -- результат можно сохранить в колонку с типом String

SELECT Roaring::Cardinality(Roaring::AndWithBinary($b1, $b2ser)) AS Cardinality; -- 1

SELECT Roaring::Uint32List(Roaring::And($b1, $b2)) AS `And`; -- [42]
SELECT Roaring::Uint32List(Roaring::AndWithBinary($b1, $b2ser)) AS AndWithBinary; -- [42]
```

```yql
$b1 = Roaring::FromUint32List(AsList(10, 567, 42));
$b2 = Roaring::FromUint32List(AsList(42));

$b2ser = Roaring::Serialize($b2); -- результат можно сохранить в колонку с типом String

SELECT Roaring::Cardinality(Roaring::AndNotWithBinary($b1, $b2ser)) AS Cardinality; -- 2

SELECT Roaring::Uint32List(Roaring::AndNot($b1, $b2)) AS AndNot; -- [10,567]
SELECT Roaring::Uint32List(Roaring::AndNotWithBinary($b1, $b2ser)) AS AndNotWithBinary; -- [10,567]
```
