# Lexical structure

The {% if feature_mapreduce %}program {% else %}query {% endif %} in the YQL language is a valid UTF-8 text consisting of _commands_ (statements) separated by semicolons (`;`).
The last semicolon can be omitted.
Each command is a sequence of _tokens_ that are valid for this command.
Tokens can be _keywords_, _IDs_, _literals_, and so on.
Tokens are separated by whitespace characters (space, tab, line feed) or _comments_. The comment is not a part of the command and is syntactically equivalent to a space character.

## Syntax compatibility modes {#lexer-modes}

Two syntax compatibility modes are supported:

* Advanced C++ (default)
* ANSI SQL

ANSI SQL mode is enabled with a special comment `--!ansi-lexer` that must be in the beginning of the {% if feature_mapreduce %}program{% else %}query{% endif %}.

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
SELECT 'string with '' quote'; -- result: a string with a ' quote
```

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

* For string literals, for example, [multiline](#multiline-string-literals) literals, the `String` type is used by default.
* You can use the following suffixes to explicitly control the literal type:
    * `u`: `Utf8`.
    * `y`: `Yson`.
    * `j`: `Json`.

**Example:**

```yql
SELECT "foo"u, '[1;2]'y, @@{"a":null}@@j;
```

## Numeric literals {#literal-numbers}

* Integer literals have the default type `Int32`, if they fit within the Int32 range. Otherwise, they automatically expand to `Int64`.
* You can use the following suffixes to explicitly control the literal type:
    * `l`: `Int64`.
    * `s`: `Int16`.
    * `t`: `Int8`.
* Add the suffix `u` to convert a type to its corresponding unsigned type:
    * `ul`: `Uint64`.
    * `u`: `Uint32`.
    * `us`: `Uint16`.
    * `ut`: `Uint8`.
* You can also use hexadecimal, octal, and binary format for integer literals using the prefixes `0x`, `0o` and `0b`, respectively. You can arbitrarily combine them with the above-mentioned suffixes.
* Floating point literals have the `Double`  type by default, but you can use the suffix `f` to narrow it down to `Float`.

```sql
SELECT
  123l AS `Int64`,
  0b01u AS `Uint32`,
  0xfful AS `Uint64`,
  0o7ut AS `Uint8`,
  456s AS `Int16`,
  1.2345f AS `Float`;
```

