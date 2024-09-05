# Hyperscan

[Hyperscan](https://www.hyperscan.io) является opensource библиотекой для поиска по регулярным выражениям, разработанной компанией Intel.

Библиотека имеет 4 реализации с использованием разных наборов процессорных инструкций (SSE3, SSE4.2, AVX2 и AVX512), среди которых автоматически выбирается нужная в соответствии с текущим процессором.

По умолчанию все функции работают в однобайтовом режиме, но если регулярное выражение является валидной UTF-8 строкой, но не является валидной ASCII строкой, — автоматически включается режим UTF-8.

**Список функций**

* ```Hyperscan::Grep(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::Match(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::BacktrackingGrep(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::BacktrackingMatch(pattern:String) -> (string:String?) -> Bool```
* ```Hyperscan::MultiGrep(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::MultiMatch(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::Capture(pattern:String) -> (string:String?) -> String?```
* ```Hyperscan::Replace(pattern:String) -> (string:String?, replacement:String) -> String?```

## Синтаксис вызова {#syntax}

При вызове напрямую, чтобы избежать компиляции регулярного выражения на каждой строке таблицы, необходимо обернуть вызов функции в [именованное выражение](../../syntax/expressions.md#named-nodes):

``` sql
$re = Hyperscan::Grep("\\d+");      -- создаем вызываемое значение для проверки конкретного регулярного выражения
SELECT * FROM table WHERE $re(key); -- используем его для фильтрации таблицы
```

**Обратите внимание** на экранирование спецсимволов в регулярном выражении. Второй слеш нужен, так как все стандартные строковые литералы в SQL могут принимать С-escaped строки, а последовательность `\d` не является валидной последовательностью, и даже если бы являлась — не приводила бы к ожидаемому эффекту поиска чисел.

Есть возможность отключить чувствительность к регистру (то есть включить case-insensitive режим), указав в начале регулярного выражения флаг `(?i)`.


## Grep {#grep}

Проверяет совпадение регулярного выражения с **частью строки** (произвольной подстрокой).

## Match {#match}

Проверяет совпадение регулярного выражения **со строкой целиком**.

Чтобы получить результат аналогичный `Grep` (где учитывается совпадение с подстрокой), нужно обрамлять регулярное выражение с обеих сторон в `.*` (`.*foo.*` вместо `foo`). Однако, с точки зрения читабельности кода, обычно лучше поменять функцию.

## BacktrackingGrep / BacktrackingMatch {#backtrackinggrep}

По принципу работы данные функции полностью совпадают с одноимёнными функциями без префикса `Backtracking`, но поддерживают более широкий ассортимент регулярных выражений. Это происходит за счет того, что если конкретное регулярное выражение в полном объёме не поддерживается Hyperscan, то библиотека переключается в режим предварительной фильтрации (prefilter). В этом случае она отвечает не «да» или «нет», а «точно нет» или «может быть да». Ответы «может быть да» затем автоматически перепроверяются с помощью медленной, но более функциональной библиотеки [libpcre](https://www.pcre.org).

## MultiGrep / MultiMatch {#multigrep}

Библиотека Hyperscan предоставляет возможность за один проход по тексту проверить несколько регулярных выражений и получить по каждому из них отдельный ответ.

Однако, если необходимо проверить совпадение строки с любым из перечисленных выражений (результаты объединялись бы через «или»), то эффективнее сделать одно регулярное выражение, объединив части с помощью оператора `|`, и использовать для поиска обычный `Grep` или `Match`.

При вызове функций `MultiGrep`/`MultiMatch` регулярные выражения передаются по одному на строку с использованием [многострочных строковых литералов](../../syntax/expressions.md#named-nodes):

**Пример**

```sql
$multi_match = Hyperscan::MultiMatch(@@a.*
.*x.*
.*axa.*@@);

SELECT
    $multi_match("a") AS a,     -- (true, false, false)
    $multi_match("axa") AS axa; -- (true, true, true)
```

## Capture и Replace {#capture}

`Hyperscan::Capture` при совпадении строки с указанным регулярным выражением возвращает последнюю подстроку, совпавшую с регулярным выражением. `Hyperscan::Replace` заменяет все вхождения указанного регулярного выражения на заданную строку.

В библиотеке Hyperscan отсутствует развитая функциональность для подобных операций, так что `Hyperscan::Capture` и `Hyperscan::Replace` хоть и реализованы для единообразия, но для сколько-либо нетривиальных поисков и замен лучше использовать одноимённые функции из библиотеки Re2:

* [Re2::Capture](re2.md#capture);
* [Re2::Replace](re2.md#replace).


## Пример использования.

```sql
$value = "xaaxaaXaa";

$match = Hyperscan::Match("a.*");
$grep = Hyperscan::Grep("axa");
$insensitive_grep = Hyperscan::Grep("(?i)axaa$");
$multi_match = Hyperscan::MultiMatch(@@a.*
.*a.*
.*a
.*axa.*@@);

$capture = Hyperscan::Capture(".*a{2}.*");
$capture_many = Hyperscan::Capture(".*x(a+).*");
$replace = Hyperscan::Replace("xa");

SELECT
    $match($value) AS match,                        -- false
    $grep($value) AS grep,                          -- true
    $insensitive_grep($value) AS insensitive_grep,  -- true
    $multi_match($value) AS multi_match,            -- (false, true, true, true)
    $multi_match($value).0 AS some_multi_match,     -- false
    $capture($value) AS capture,                    -- "xaa"
    $capture_many($value) AS capture_many,          -- "xa"
    $replace($value, "b") AS replace                -- "babaXaa"
;
```
