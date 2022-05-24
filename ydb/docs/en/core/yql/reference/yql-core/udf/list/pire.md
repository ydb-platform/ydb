# Pire

**List of functions**

* ```Pire::Grep(pattern:String) -> (string:String?) -> Bool```
* ```Pire::Match(pattern:String) -> (string:String?) -> Bool```
* ```Pire::MultiGrep(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Pire::MultiMatch(pattern:String) -> (string:String?) -> Tuple<Bool, Bool, ...>```
* ```Pire::Capture(pattern:String) -> (string:String?) -> String?```
* ```Pire::Replace(pattern:String) -> (string:String?, replacement:String) -> String?```

One of the options to match regular expressions in YQL is to use  [Pire](https://github.com/yandex/pire) (Perl Incompatible Regular Expressions). This is a very fast library of regular expressions developed at Yandex: at the lower level, it looks up the input string once, without any lookaheads or rollbacks, spending 5 machine instructions per character (on x86 and x86_64).

The speed is achieved by using the reasonable restrictions:

* Pire is primarily focused at checking whether a string matches a regular expression.
* The matching substring can also be returned (by Capture), but with restrictions (a match with only one group is returned).

By default, all functions work in the single-byte mode. However, if the regular expression is a valid UTF-8 string but is not a valid ASCII string, the UTF-8 mode is enabled automatically.

To enable the Unicode mode, you can put one character that's beyond ASCII with the `?` operator, for example: `\\w+я?`.

## Call syntax {#call-syntax}

To avoid compiling a regular expression at each table row, wrap the function call by [a named expression](../../syntax/expressions.md#named-nodes):

```sql
$re = Pire::Grep("\\d+"); -- create a callable value to match a specific regular expression
SELECT * FROM table WHERE $re(key); -- use it to filter the table
```

{% note alert %}

When escaping special characters in a regular expression, be sure to use the second slash, since all the standard string literals in SQL can accept C-escaped strings, and the `\d` sequence is not a valid sequence (even if it were, it wouldn't search for numbers as intended).

{% endnote %}

You can enable the case-insensitive mode by specifying, at the beginning of the regular expression, the flag `(?i)`.

**Examples**

```sql
$value = "xaaxaaxaa";
$match = Pire::Match("a.*");
$grep = Pire::Grep("axa");
$insensitive_grep = Pire::Grep("(?i)axa");
$multi_match = Pire::MultiMatch(@@a.*
.*a.*
.*a
.*axa.*@@);
$capture = Pire::Capture(".*x(a).*");
$capture_many = Pire::Capture(".*x(a+).*");
$replace = Pire::Replace(".*x(a).*");

SELECT
  $match($value) AS match,                        -- false
  $grep($value) AS grep,                          -- true
  $insensitive_grep($value) AS insensitive_grep,  -- true
  $multi_match($value) AS multi_match,            -- (false, true, true, true)
  $multi_match($value).0 AS some_multi_match,     -- false
  $capture($value) AS capture,                    -- "a"
  $capture_many($value) AS capture_many,          -- "aa"
  $replace($value, "b") AS replace;               -- "xaaxaaxba"
```

## Grep {#grep}

Matches the regular expression with a **part of the string** (arbitrary substring).

## Match {#match}

Matches **the whole string** against the regular expression.
To get a result similar to `Grep`  (where substring matching is included), enclose the regular expression in `.*`. For example, use `.*foo.*` instead of `foo`.

## MultiGrep/MultiMatch {#multigrep}

Pire lets you match against multiple regular expressions in a single pass through the text and get a separate response for each match.
Use the MultiGrep/MultiMatch functions to optimize the query execution speed. Be sure to do it carefully, since the size of the state machine used for matching grows exponentially with the number of regular expressions:

* If you want to match a string against any of the listed expressions (the results are joined with "or"), it would be much more efficient to combine the query parts in a single regular expression with `|` and match it using regular Grep or Match.
* Pire has a limit on the size of the state machine (YQL uses the default value set in the library). If you exceed the limit, the error is raised at the start of the query: `Failed to glue up regexes, probably the finite state machine appeared to be too large`.
When you call MultiGrep/MultiMatch, regular expressions are passed one per line using [multiline string literals](../../syntax/expressions.md#multiline-string-literals):

**Examples**

```sql
$multi_match = Pire::MultiMatch(@@a.*
.*x.*
.*axa.*@@);

SELECT
    $multi_match("a") AS a,      -- (true, false, false)
    $multi_match("axa") AS axa;  -- (true, true, true)
```

## Capture {#capture}

If a string matches the specified regular expression, it returns a substring that matches the group enclosed in parentheses in the regular expression.
Capture is non-greedy: the shortest possible substring is returned.

{% note alert %}

The expression must contain only **one** group in parentheses. `NULL` (empty Optional) is returned in case of no match.

{% endnote %}

If the above limitations and features are unacceptable for some reason, we recommend that you consider [Re2::Capture](re2.md#capture).

## REPLACE {#replace}

Pire doesn't support replace based on a regular expression. `Pire::Replace` implemented in YQL is a simplified emulation using `Capture`. It may run correctly, if the substring occurs more than once in the source string.

As a rule, it's better to use [Re2::Replace](re2.md#replace) instead.

