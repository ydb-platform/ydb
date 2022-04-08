# Вынос части запроса в отдельный файл

Механизм для выноса части запроса в отдельный приложенный файл:

* [PRAGMA Library](../pragma.md#library) помечает приложенный файл как доступный для импорта.
## `Export`
* В `EXPORT $my_symbol1, $my_symbol2, ...;` перечисляется список именованных выражений в библиотеке, доступных для импорта.
## `Import`
* `IMPORT my_library SYMBOLS $my_symbol1, $my_symbol2, ...;` делает перечисленные именованные выражения доступными для использования ниже.

{% note info "Примечание" %}

В библиотеку могут быть вынесены [лямбды](../expressions.md#lambda), [действия](../action.md){% if feature_subquery %}, [именованные подзапросы](../subquery.md){% endif %}, константы и выражения, но __не подзапросы и не агрегатные функции__.

{% endnote %}

{% note warning "Предупреждение" %}

Файл, на который ссылается [PRAGMA Library](../pragma.md#library) должен быть приложен к запросу. __Использовать для этой цели [PRAGMA File](../pragma.md#file) нельзя__.

{% endnote %}


**Примеры:**

my_lib.sql:
``` yql
$Square = ($x) -> { RETURN $x * $x; };
$Sqrt = ($x) -> { RETURN Math::Sqrt($x); };

-- Агрегационные функции, создаваемые с помощью
-- AggregationFactory, удобно выносить в библиотеку
$Agg_sum = AggregationFactory("SUM");
$Agg_max = AggregationFactory("MAX");

EXPORT $Square, $Sqrt, $Agg_sum, $Agg_max;
```

Запрос:
``` yql
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

