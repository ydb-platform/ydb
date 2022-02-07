# About
This is a PCRE library wrapper which provides unified interface for UTF-8, UTF-16 and UTF-32 strings matching and optimization control.

# Rationale
Many Arcadia related libraries (telfinder, lemmer etc.) provides only UTF-16 interfaces, because this is way faster for cyrillic texts. Any algorithm that is working with such libraries and regular expressions must use `WideToUTF8` and `UTF8ToWide` at the borderline between regular expression and UTF-18 interface. This leads us to great performance penalty.
This library allows us to erase these charset conversions.

# Interface

Before starting with interface details, let's consider simplest library usage example:
`UNIT_ASSERT(NPcre::TPcre<wchar16>(u"ba+d").Matches(TWtringBuf(u"baaad")));`

Here we see regular expression construction for UTF-16 charset:

`NPcre::TPcre<wchar16>(u"ba+d")`

and matching of the subject string `baaad` against this pattern:

`.Matches(TWtringBuf(u"baaad"))`;

Let's consider both of them in details.

## Construction
`NPcre::TPcre` class accepts single template parameter: `TCharType`. Currently supported char types are `char`, `wchar16` and `wchar32`. Additional char types traits can be defined in `traits.h`

Constructor accepts three arguments. Two of them are optional:
1. Zero-terminated string on characters with pattern
2. Optimization type. The default value is `NPcre::EOptimize::None` which means no pattern optimization. Another possible value is `NPcre::EOptimize::Study` which will take some time at construction stage but could give up to 4x speed boost. And the last but not the least is `NPcre::EOptimize::JIT` which performs JIT optimization which could take significant time but could give up to 10x speed boost.
3. Regular expressions compile flags. We don't want to reimplement every constant from PCRE library, so they are passed as they are. Full list of compile flags can be found [here](https://www.pcre.org/original/doc/html/pcre_compile2.html), but for most cases `PCRE_UTF8 | PCRE_UCP` will be enough. The default value is `0`.

## Matching
{% note tip %}
Two words on PCRE workspaces. Workspace is memory area where PCRE stores information about back references and capturing groups. If passed workspace size is not enough, PCRE will allocate bigger workspace in heap. For simple matching and string searching of string without back references, workspace is not required and this library provides separate functions that won't waste space on workspace and this could save ≈0.5% of CPU TIME on simple patterns.
For regular expressions with capturing groups, recommended workspace size is `(capturing groups count + 1)`.
{% endnote %}

In the example above matching function `Matches` returns boolean indicating that subject string matched pattern and accepts two arguments:
1. `TBasicStringBuf<TCharType>` with subject string
2. Regular expression execute flags. We don't want to reimplement every constant from PCRE library, so they are passed as they are. Full list of compile flags can be found [here](https://www.pcre.org/original/doc/html/pcre_exec.html). For most cases `0` will be just fine and this is the default value.

## Searching
Function `Find` accepts the same arguments as `Match` and returns `TMaybe<NPcre::TPcreMatch>` which contains pair of ints with start and end offsets of string found. Check result for `Defined` to ensure that pattern was found in subject string.

## Capturing
The last member function of `NPcre::TPcre` is `Capture` which searches for pattern and returns capturing group.

### Return value
Return value is `NPcre::TPcreMatches` which is alias for `TVector<NPcre::TPcreMatch>`.
Vector will be empty if pattern wasn't found in subject string.
If pattern was found, first element will contain start and end offsets of string found.
All other elements will contains start and end offsets of capturing groups in order they appeared in regular expression.
{% note tip %}
If some capturing group not matched subject string, but some of consequent capturing groups did, this capturing group will present as `-1, -1` pair.
For example: calling `Capture` on pattern `(a)(?:(b)c|b(d))` against subject string `zabda` will return `[{1,4},{1,2},{-1,-1},{3,4}]` because capturing group `(b)` wasn't matched.
{% endnote %}
### Arguments
1. `TBasicStringBuf<TCharType>` with subject string
2. Regular expression execute flags.
3. Initial workspace size. Default value is `16` but if pattern contains more than 16 capturing groups, this function will reallocate workspace with bigger size.
