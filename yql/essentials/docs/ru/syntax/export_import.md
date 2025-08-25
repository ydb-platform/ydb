# EXPORT и IMPORT

Механизм для выноса части запроса в отдельный приложенный файл. Чтобы воспользоваться механизмом, необходимо выставить прагму:

* [PRAGMA Library](pragma/file.md#library) &mdash; помечает приложенный файл как доступный для импорта.

## EXPORT

В `EXPORT $my_symbol1, $my_symbol2, ...;` перечисляется список именованных выражений в библиотеке, доступных для импорта.

## IMPORT

`IMPORT my_library SYMBOLS $my_symbol1, $my_symbol2, ...;` делает перечисленные именованные выражения доступными для использования ниже.

{% note info %}

В библиотеку могут быть вынесены [лямбды](expressions.md#lambda), [действия](action.md), [именованные подзапросы](subquery.md), константы и выражения, но __не подзапросы и не агрегатные функции__.

{% endnote %}

{% note warning %}

Файл, на который ссылается [PRAGMA Library](pragma/file.md#library), должен быть приложен к запросу. __Использовать для этой цели [PRAGMA File](pragma/file.md#file) нельзя__.

{% endnote %}


## Пример

my_lib.sql:

```yql
$Square = ($x) -> { RETURN $x * $x; };
$Sqrt = ($x) -> { RETURN Math::Sqrt($x); };

-- Агрегационные функции, создаваемые с помощью
-- AggregationFactory, удобно выносить в библиотеку
$Agg_sum = AggregationFactory("SUM");
$Agg_max = AggregationFactory("MAX");

EXPORT $Square, $Sqrt, $Agg_sum, $Agg_max;
```

Запрос:

```yql
PRAGMA Library("my_lib.sql");
IMPORT my_lib SYMBOLS $Square, $Sqrt, $Agg_sum, $Agg_max;
SELECT
  $Square(2), -- 4
  $Sqrt(4);   -- 2

SELECT
  AGGREGATE_BY(x, $Agg_sum), -- 5
  AGGREGATE_BY(x, $Agg_max)  -- 3
FROM (
  SELECT 2 AS x
  UNION ALL
  SELECT 3 AS x
)
```
