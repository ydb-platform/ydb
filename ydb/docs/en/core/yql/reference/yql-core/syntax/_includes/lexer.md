
# Lexical structure

The {% if feature_mapreduce %}program{% else %}query{% endif %} in the YQL language is a valid UTF-8 text consisting of _commands_ (statements) separated by semicolons (`;`).
The last semicolon can be omitted.
Each command is a sequence of _tokens_ that are valid for this command.
Tokens can be _keywords_, _IDs_, _literals_, and so on.
Tokens are separated by whitespace characters (space, tab, line feed) or _comments_. The comment is not a part of the command and is syntactically equivalent to a space character.

## Syntax compatibility modes {#lexer-modes}

Two syntax compatibility modes are supported:
* Advanced C++ (default)
* ANSI SQL

ANSI SQL mode is enabled with a special comment `--!ansi-lexer`, which must be in the beginning of the {% if feature_mapreduce %}program{% else %}query{% endif %}.

Specifics of interpretation of lexical elements in different compatibility modes are described below.

## Comments {#comments}

The following types of comments are supported:

* Single-line comment: starts with `--` (two minus characters _following one another_) and continues to the end of the line
* Multiline comment: starts with `/*` and ends with `*/`

```sql
SELECT 1; -- A single-line comment
/*
   Some multi-line comment
*/
```
In C++ syntax compatibility mode (default), a multiline comment ends with the _nearest_ `*/`.
The ANSI SQL syntax compatibility mode accounts for nesting of multiline comments:

```sql
--!ansi_lexer
SELECT * FROM T; /* this is a comment /* this is a nested comment, without ansi_lexer it raises an error  */ */
```

## Keywords and identifiers {#keywords-and-ids}

**Keywords** are tokens that have a fixed value in the YQL language. Examples of keywords: `SELECT`, `INSERT`, `FROM`, `ACTION`, and so on. Keywords are case-insensitive, that is, `SELECT` and `SeLEcT` are equivalent to each other.
The list of keywords is not fixed and is going to expand as the language develops. A keyword can't contain numbers and begin or end with an underscore.

**Identifiers** are tokens that identify the names of tables, columns, and other objects in YQL. Identifiers in YQL are always case-sensitive.
An identifier can be written in the body of the program without any special formatting, if the identifier:

* Is not a keyword
* Begins with a Latin letter or underscore
* Is followed by a Latin letter, an underscore, or a number

```sql
SELECT my_column FROM my_table; -- my_column and my_table are identifiers
```

To include an arbitrary ID in the body of a {% if feature_mapreduce %}program{% else %}query{% endif %}, the ID is enclosed in backticks:
```sql
SELECT `column with space` from T;
SELECT * FROM `my_dir/my_table`
```

IDs in backticks are never interpreted as keywords:

```sql
SELECT `select` FROM T; -- select - Column name in the T table
```
When using backticks, you can use the standard C escaping:

```sql
SELECT 1 as `column with\n newline, \x0a newline and \` backtick `;
```

In ANSI SQL syntax compatibility mode, arbitrary IDs can also be enclosed in double quotes. To include a double quote in a quoted ID, use two double quotes:

```sql
--!ansi_lexer
SELECT 1 as "column with "" double quote"; -- column name will be: column with " double quote
```

## SQL hints {#sql-hints}

SQL hints are special settings with which a user can modify a query execution plan
(for example, enable/disable specific optimizations or force the JOIN execution strategy).
Unlike [PRAGMA](../pragma.md), SQL hints act locally – they are linked to a specific point in the YQL query (normally, after the keyword)
and affect only the corresponding statement or even a part of it.
SQL hints are a set of settings "name-value list" and defined inside special comments —
comments with SQL hints must have `+` as the first character:
```sql
--+ Name1(Value1 Value2 Value3) Name2(Value4) ...
```
An SQL hint name must be comprised of ASCII alphanumeric characters and start with a letter. Hint names are case insensitive.
A hint name must be followed by a custom number of space-separated values. A value can be a custom set of characters.
If there's a space or parenthesis in a set of characters, single quotation marks must be used:

```sql
--+ foo('value with space and paren)')
```

```sql
--+ foo('value1' value2)
-- equivalent to
--+ foo(value1 value2)
```

To escape a single quotation within a value, double it:

```sql
--+ foo('value with single quote '' inside')
```

If there're two or more hints with the same name in the list, the latter is used:
```sql
--+ foo(v1 v2) bar(v3) foo()
-- equivalent to
--+ bar(v3) foo()
```

Unknown SQL hint names (or syntactically incorrect hints) never result in errors, they're simply ignored:
```sql
--+ foo(value1) bar(value2  baz(value3)
-- due to a missing closing parenthesis in bar, is equivalent to
--+ foo(value1)
```
Thanks to this behavior, previous valid YQL queries with comments that look like hints remain intact.
Syntactically correct SQL hints in a place unexpected for YQL result in a warning:

```sql
-- presently, hints after SELECT are not supported
SELECT /*+ foo(123) */ 1; -- warning 'Hint foo will not be used'
```

What's important is that SQL hints are hints for an optimizer, so:

* Hints never affect search results.
* As YQL optimizers improve, a situation is possible when a hint becomes outdated and is ignored (for example, the algorithm based on a given hint completely changes or the optimizer becomes so sophisticated that it can be expected to choose the best solution, so some manual settings are likely to interfere).

## String literals {#string-literals}

A string literal (constant) is expressed as a sequence of characters enclosed in single quotes. Inside a string literal, you can use the C-style escaping rules:
```yql
SELECT 'string with\n newline, \x0a newline and \' backtick ';
```

In the C++ syntax compatibility mode (default), you can use double quotes instead of single quotes:
```yql
SELECT "string with\n newline, \x0a newline and \" backtick ";
```

In ASNI SQL compatibility mode, double quotes are used for IDs, and the only escaping that can be used for string literals is a pair of single quotes:

```sql
--!ansi_lexer
SELECT 'string with '' quote'; -- result: string with ' quote
```

Based on string literals, [simple literals](../../builtins/basic#data-type-literals) can be obtained.

### Multi-line string literals {#multiline-string-literals}

A multiline string literal is expressed as an arbitrary set of characters enclosed in double at signs `@@`:

```yql
$text = @@some
multiline
text@@;
SELECT LENGTH($text);
```

If you need to use double at signs in your text, duplicate them:

```yql
$text = @@some
multiline with double at: @@@@
text@@;
SELECT $text;
```

### Typed string literals {#typed-string-literals}

* For string literals, including [multi-string](#multiline-string-literals) ones, the `String` type is used by default (see also [PRAGMA UnicodeLiterals](../pragma.md#UnicodeLiterals)).
* You can use the following suffixes to explicitly control the literal type:

  * `s` — `String`
  * `u` — `Utf8`
  * `y` — `Yson`
  * `j` — `Json`

**Example:**
```yql
SELECT "foo"u, '[1;2]'y, @@{"a":null}@@j;
```

## Numeric literals {#literal-numbers}

* Integer literals have the default type `Int32`, if they fit within the Int32 range. Otherwise, they automatically expand to `Int64`.
* You can use the following suffixes to explicitly control the literal type:

  * `l`: `Int64`
  * `s`: `Int16`
  * `t`: `Int8`

* Add the suffix `u` to convert a type to its corresponding unsigned type:

  * `ul`: `Uint64`
  * `u`: `Uint32`
  * `us`: `Uint16`
  * `ut`: `Uint8`

* You can also use hexadecimal, octal, and binary format for integer literals using the prefixes `0x`, `0o` and `0b`, respectively. You can arbitrarily combine them with the above-mentioned suffixes.
* Floating point literals have the `Double` type by default, but you can use the suffix `f` to narrow it down to `Float`.

```sql
SELECT
  123l AS `Int64`,
  0b01u AS `Uint32`,
  0xfful AS `Uint64`,
  0o7ut AS `Uint8`,
  456s AS `Int16`,
  1.2345f AS `Float`;
```
