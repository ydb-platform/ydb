# Re2

**List of functions**

* ```Re2::Grep(String) -> (String?) -> Bool```
* ```Re2::Match(String) -> (String?) -> Bool```
* ```Re2::Capture(String) -> (String?) -> Struct<_1:String?,foo:String?,...>```
* ```Re2::FindAndConsume(String) -> (String?) -> List<String>```
* ```Re2::Replace(String) -> (String?, String) -> String?```
* ```Re2::Count(String) -> (String?) -> Uint32```
* ```Re2::Options([CaseSensitive:Bool?,DotNl:Bool?,Literal:Bool?,LogErrors:Bool?,LongestMatch:Bool?,MaxMem:Uint64?,NeverCapture:Bool?,NeverNl:Bool?,OneLine:Bool?,PerlClasses:Bool?,PosixSyntax:Bool?,Utf8:Bool?,WordBoundary:Bool?]) -> Struct<CaseSensitive:Bool,DotNl:Bool,Literal:Bool,LogErrors:Bool,LongestMatch:Bool,MaxMem:Uint64,NeverCapture:Bool,NeverNl:Bool,OneLine:Bool,PerlClasses:Bool,PosixSyntax:Bool,Utf8:Bool,WordBoundary:Bool>```

As Pire has certain limitations needed to ensure efficient string matching against regular expressions, it might be too complex or even impossible to use [Pire](pire.md) for some tasks. For such situations, we added another module to support regular expressions based on [google::RE2](https://github.com/google/re2). It offers a broader range of features ([see the official documentation](https://github.com/google/re2/wiki/Syntax)).

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
  $match($value) AS match,
  $grep($value) AS grep,
  $capture($value) AS capture,
  $capture($value)._1 AS capture_member,
  $replace($value, "b\\1z") AS replace,
  $count($value) AS count;

/*
- match: `false`
- grep: `true`
- capture: `(_0: 'xaaxaaxaa', _1: 'aa', foo: 'x')`
- capture_member: `"aa"`
- replace: `"baazaaxaa"`
- count:: `6`
*/
```

## Re2::Grep / Re2::Match {#match}

If you leave out the details of implementation and syntax of regular expressions, those functions are totally similar [to the applicable functions](pire.md#match) from the Pire modules. With other things equal and no specific preferences, we recommend that you use `Pire::Grep or Pire::Match`.

## Re2::Capture {#capture}

Unlike [Pire::Capture](pire.md#capture) ,  `Re2:Capture` supports multiple and named capturing groups.

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

