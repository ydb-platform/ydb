* Функции, вычисляемые на рамке окна `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` либо `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, реализованы эффективно (не требуют дополнительной памяти и вычисляются на разделе за O(размер раздела)).

* Для рамки окна `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` есть возможность выбрать стратегию выполнения в оперативной памяти, указав [хинт](../../lexer.md#sql-hints) `COMPACT` после ключевого слова `PARTITION`.

  Например: `PARTITION /*+ COMPACT() */ BY key` или `PARTITION /*+ COMPACT() */ BY ()` (в случае если `PARTITION BY` изначально отсутствовал).

  При наличии хинта `COMPACT` потребуется дополнительная память в размере O(размер раздела), но при этом не возникнет дополнительной `JOIN` операции.

* Если рамка окна не начинается с `UNBOUNDED PRECEDING`, то для вычисления оконных функций на таком окне потребуется дополнительная память в размере O(максимальное расстояние от границ окна до текущей строки), а время вычисления будет равно O(число_строк_в_разделе * размер_окна).

* Для рамки окна, начинающейся с `UNBOUNDED PRECEDING` и заканчивающейся на `N`, где `N` не равен `CURRENT ROW` или `UNBOUNDED FOLLOWING`, потребуется дополнительная память в размере O(N), а время вычисления будет O(N * число_строк_в_разделе).

* Функции `LEAD(expr, N)` и `LAG(expr, N)` всегда потребуют O(N) памяти.

Учитывая вышесказанное, запрос с `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` по возможности стоит переделать в `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, поменяв порядок сортировки в `ORDER BY` на обратный.
