# Re2

**List of functions**

* ```Re2::Grep(pattern:String, options:Struct<...>?) -> (string:String?) -> Bool```
* ```Re2::Match(pattern:String, options:Struct<...>?) -> (string:String?) -> Bool```
* ```Re2::Capture(pattern:String, options:Struct<...>?) -> (string:String?) -> Struct<_1:String?,foo:String?,...>```
* ```Re2::FindAndConsume(pattern:String, options:Struct<...>?) -> (string:String?) -> List<String>```
* ```Re2::Replace(pattern:String, options:Struct<...>?) -> (string:String?, replacement:String) -> String?```
* ```Re2::Count(pattern:String, options:Struct<...>?) -> (string:String?) -> Uint32```
* ```Re2::Options([CaseSensitive:Bool?,DotNl:Bool?,Literal:Bool?,LogErrors:Bool?,LongestMatch:Bool?,MaxMem:Uint64?,NeverCapture:Bool?,NeverNl:Bool?,OneLine:Bool?,PerlClasses:Bool?,PosixSyntax:Bool?,Utf8:Bool?,WordBoundary:Bool?]) -> Struct<CaseSensitive:Bool,DotNl:Bool,Literal:Bool,LogErrors:Bool,LongestMatch:Bool,MaxMem:Uint64,NeverCapture:Bool,NeverNl:Bool,OneLine:Bool,PerlClasses:Bool,PosixSyntax:Bool,Utf8:Bool,WordBoundary:Bool>```

The Re2 module supports regular expressions based on [google::RE2](https://github.com/google/re2) with a wide range of features provided ([see the official documentation](https://github.com/google/re2/wiki/Syntax)).

By default, the UTF-8 mode is enabled automatically if the regular expression is a valid UTF-8-encoded string, but is not a valid ASCII string. You can manually control the settings of the re2 library, if you pass the result of the `Re2::Options` function as the second argument to other module functions, next to the regular expression.

{% note warning %}

Make sure to double all the backslashes in your regular expressions (if they are within a quoted string): standard string literals are treated as C-escaped strings in SQL. You can also format regular expressions as raw strings `@@regexp@@`: double slashes are not needed in this case.

{% endnote %}

**Examples**

```sql
$value = "xaaxaaxaa";
$options = Re2::Options(false AS CaseSensitive);
$match = Re2::Match("[ax]+\\d");
$grep = Re2::Grep("a.*");
$capture = Re2::Capture(".*(?P<foo>xa?)(a{2,}).*");
$replace = Re2::Replace("x(a+)x");
$count = Re2::Count("a", $options);

SELECT
  $match($value) AS match,                -- false
  $grep($value) AS grep,                  -- true
  $capture($value) AS capture,            -- (_0: 'xaaxaaxaa', _1: 'aa', foo: 'x')
  $capture($value)._1 AS capture_member,  -- "aa"
  $replace($value, "b\\1z") AS replace,   -- "baazaaxaa"
  $count($value) AS count;                -- 6
```

## Re2::Grep / Re2::Match {#match}

If you leave out the details of implementation and syntax of regular expressions, those functions are totally similar [to the same-name functions](pire.md#match) from the Pire module. With other things equal and no specific preferences, we recommend that you use `Pire::Grep or Pire::Match`.

You can call the `Re2::Grep` function by using a `REGEXP` expression (see the [basic expression syntax](../../syntax/expressions.md#regexp)).

For example, the following two queries are equivalent (also in terms of computing efficiency):

* ```$grep = Re2::Grep("b+"); SELECT $grep("aaabccc");```
* ```SELECT "aaabccc" REGEXP "b+";```

## Re2::Capture {#capture}

Unlike [Pire::Capture](pire.md#capture), `Re2::Capture` supports multiple and named capturing groups.
Result type: a structure with the fields of the type `String?`.

* Each field corresponds to a capturing group with the applicable name.
* For unnamed groups, the following names are generated: `_1`, `_2`, etc.
* The result always includes the `_0` field containing the entire substring matching the regular expression.

For more information about working with structures in YQL, see the [section on containers](../../types/containers.md).

## Re2::FindAndConsume {#findandconsume}

Searches for all occurrences of the regular expression in the passed text and returns a list of values corresponding to the parenthesized part of the regular expression for each occurrence.

## Re2::Replace {#replace}

Works as follows:

* In the input string (first argument), all the non-overlapping substrings matching the regular expression are replaced by the specified string (second argument).
* In the replacement string, you can use the contents of capturing groups from the regular expression using back-references in the format: `\\1`, `\\2` etc. The `\\0` back-reference stands for the whole substring that matches the regular expression.

## Re2::Count {#count}

Returns the number of non-overlapping substrings of the input string that have matched the regular expression.

## Re2::Options {#options}

Notes on Re2::Options from the official [repository](https://github.com/google/re2/blob/main/re2/re2.h#L595-L617)

| Parameter | Default | Comments |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------- |
| CaseSensitive:Bool? | true | match is case-sensitive (regexp can override with (?i) unless in posix_syntax mode) |
| DotNl:Bool? | false | let `.` match `\n` (default ) |
| Literal:Bool? | false | interpret string as literal, not regexp |
| LogErrors:Bool? | true | log syntax and execution errors to ERROR |
| LongestMatch:Bool? | false | search for longest match, not first match |
| MaxMem:Uint64? | - | (see below) approx. max memory footprint of RE2 |
| NeverCapture:Bool? | false | parse all parents as non-capturing |
| NeverNl:Bool? | false | never match \n, even if it is in regexp |
| PosixSyntax:Bool? | false | restrict regexps to POSIX egrep syntax |
| Utf8:Bool? | true | text and pattern are UTF-8; otherwise Latin-1 |
| The following options are only consulted when PosixSyntax == true. <bt>When PosixSyntax == false, these features are always enabled and cannot be turned off; to perform multi-line matching in that case, begin the regexp with (?m). |
| PerlClasses:Bool? | false | allow Perl's \d \s \w \D \S \W |
| WordBoundary:Bool? | false | allow Perl's \b \B (word boundary and not) |
| OneLine:Bool? | false | ^ and $ only match beginning and end of text |

It is not recommended to use Re2::Options in the code. Most parameters can be replaced with regular expression flags.

**Flag usage examples**

```sql
$value = "Foo bar FOO"u;
-- enable case-insensitive mode
$capture = Re2::Capture(@@(?i)(foo)@@);

SELECT
    $capture($value) AS capture; -- ("_0": "Foo", "_1": "Foo")

$capture = Re2::Capture(@@(?i)(?P<foo>FOO).*(?P<bar>bar)@@);

SELECT
    $capture($value) AS capture; -- ("_0": "Foo bar", "bar": "bar", "foo": "Foo")
```

In both cases, the word FOO will be found. Using the raw string @@regexp@@ lets you avoid double slashes.

