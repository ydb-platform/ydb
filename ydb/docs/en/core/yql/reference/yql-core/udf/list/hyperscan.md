# Hyperscan

[Hyperscan](https://www.hyperscan.io) is an opensource library for regular expression matching developed by Intel.

The library includes 4 implementations that use different sets of processor instructions (SSE3, SSE4.2, AVX2, and AVX512), with the needed instruction automatically selected based on the current processor.

By default, all functions work in the single-byte mode. However, if the regular expression is a valid UTF-8 string but is not a valid ASCII string, the UTF-8 mode is enabled automatically.

**List of functions**

* ```Hyperscan::Grep(String) -> (String?) -> Bool```
* ```Hyperscan::Match(String) -> (String?) -> Bool```
* ```Hyperscan::BacktrackingGrep(String) -> (String?) -> Bool```
* ```Hyperscan::BacktrackingMatch(String) -> (String?) -> Bool```
* ```Hyperscan::MultiGrep(String) -> (String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::MultiMatch(String) -> (String?) -> Tuple<Bool, Bool, ...>```
* ```Hyperscan::Capture(String) -> (String?) -> String?```
* ```Hyperscan::Replace(String) -> (String?, String) -> String?```

## Call syntax {#syntax}

To avoid compiling a regular expression at each table row at direct call, wrap the function call by [a named expression](../../syntax/expressions.md#named-nodes):

```sql
$re = Hyperscan::Grep("\\d+");      -- create a callable value to match a specific regular expression
SELECT * FROM table WHERE $re(key); -- use it to filter the table
```

**Please note** escaping of special characters in regular expressions. Be sure to use the second slash, since all the standard string literals in SQL can accept C-escaped strings, and the `\d` sequence is not valid sequence (even if it were, it wouldn't search for numbers as intended).

You can enable the case-insensitive mode by specifying, at the beginning of the regular expression, the flag `(?i)`.

## Grep {#grep}

Matches the regular expression with a **part of the string** (arbitrary substring).

You can call the `Hyperscan::Grep` function by using a `REGEXP` expression (see the [basic expression syntax](../../syntax/expressions.md#regexp)).

For example, the following two queries are equivalent (also in terms of computing efficiency):

* ```$grep = Hyperscan::Grep("b+"); SELECT $grep("aaabccc");```
* ```SELECT "aaabccc" REGEXP "b+";```

## Match {#match}

Matches **the whole string** against the regular expression.

To get a result similar to `Grep` (where substring matching is included), enclose the regular expression in `.*` (`.*foo.*` instead of `foo`). However, in terms of code readability, it's usually better to change the function.

## BacktrackingGrep/BacktrackingMatch {#backtrackinggrep}

The functions are identical to the same-name functions without the `Backtracking` prefix. However, they support a broader range of regular expressions. This is due to the fact that if a specific regular expression is not fully supported by Hyperscan, the library switches to the prefilter mode. In this case, it responds not by "Yes" or "No", but by "Definitely not" or "Maybe yes". The "Maybe yes" responses are then automatically rechecked using a slower, but more functional, library [libpcre](https://www.pcre.org).

Those functions are currently called in the binary operators [REGEXP](../../syntax/expressions.md#regexp) and [MATCH](../../syntax/expressions.md#match).

## MultiGrep/MultiMatch {#multigrep}

Hyperscan lets you match against multiple regular expressions in a single pass through the text, and get a separate response for each match.

However, if you want to match a string against any of the listed expressions (the results would be joined with "or"), it would be more efficient to combine the query parts in a single regular expression with `|` and match it with regular `Grep` or `Match`.

When you call `MultiGrep`/`MultiMatch`, regular expressions are passed one per line using [multiline string literals](../../syntax/expressions.md#named-nodes):

**Example**

```sql
$multi_match = Hyperscan::MultiMatch(@@a.*
.*x.*
.*axa.*@@);

SELECT
    $multi_match("a") AS a,
    $multi_match("axa") AS axa;

/*
- a: `(true, false, false)`
- axa: `(true, true, true)`
*/
```

## Capture and Replace {#capture}

Hyperscan doesn't support advanced functionality for such operations. Although `Hyperscan::Capture` and `Hyperscan::Replace` are implemented for consistency, it's better to use the same-name functions from the Re2 library for any non-trivial capture and replace:

* [Re2::Capture](re2.md#capture);
* [Re2::Replace](re2.md#replace).

## Usage example

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
    $match($value) AS match,
    $grep($value) AS grep,
    $insensitive_grep($value) AS insensitive_grep,
    $multi_match($value) AS multi_match,
    $multi_match($value).0 AS some_multi_match,
    $capture($value) AS capture,
    $capture_many($value) AS capture_many,
    $replace($value, "b") AS replace
;

/*
- match: false
- grep: true
- insensitive_grep: true
- multi_match: (false, true, true, true)
- some_multi_match: false
- capture: "xaaa"
- capture_many: "xaa"
- replace: "babaXaa"
*/
```

