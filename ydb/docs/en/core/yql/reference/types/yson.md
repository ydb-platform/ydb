# Yson

This section describes YSON, a JSON-like data format developed at Yandex.

{% note info %}

SQL functions for working with YSON are documented [here](../udf/list/yson.md).

{% endnote %}

## Introduction {#intro}

**How YSON differs from JSON:**

1. Binary encoding is supported for scalar types (numbers, strings, and booleans);
2. [Attributes](#attributes): an arbitrary dictionary that can be attached to a literal of any type (including scalars).

**Syntactic differences:**

1. List items are separated by semicolons instead of commas;
2. In maps, keys are separated from values with `=` rather than `:`;
3. String literals do not always need quotes (only when parsing would otherwise be ambiguous).

The following **scalar** types exist:

1. [Strings](#string) (`string`);
2. [Signed](#int) and [unsigned](#uint) 64-bit integers (`int64` and `uint64`);
3. [Double-precision floating-point](#double) numbers (`double`);
4. [Boolean](#boolean) (`boolean`);
5. The [special entity type](#entity) with a single literal (`#`).

Scalar types usually have both text and binary representations.

There are two **composite** types:

1. [List](#list) (`list`);
2. [Map](#map) (`map`).

## Scalar types {#scalar_types}

### Strings {#string}

String tokens come in three forms:

1. **Identifiers** match the regular expression `[A-Za-z_][A-Za-z0-9_.\-]*` (first character is a letter or underscore; from the second character onward, digits and `-`, `.` are also allowed). An identifier denotes a string equal to its text and is mainly a shorthand (no quotes).

   Examples:

   - `abc123`;
   - `_`;
   - `a-b`.

2. **Text strings** are [C-escaped](https://en.wikipedia.org/wiki/Escape_sequences_in_C) strings in double quotes.

   Examples:

   - `"abc123"`;
   - `""`;
   - `"quotation-mark: \", backslash: \\, tab: \t, unicode: \xEA"`.

3. **Binary strings**: `\x01 + length (protobuf sint32 wire format) + data (<length> bytes)`.

### Signed 64-bit integers (`int64`) {#int}

Two encodings:

1. **Text**: (`0`, `123`, `-123`, `+123`);
2. **Binary**: `\x02 + value (protobuf sint64 wire format)`.

### Unsigned 64-bit integers (`uint64`) {#uint}

Two encodings:

1. **Text**: (`10000000000000`, `123u`);
2. **Binary**: `\x06 + value (protobuf uint64 wire format)`.

### Floating-point numbers (`double`) {#double}

Two encodings:

1. **Text**: (`0.0`, `-1.0`, `1e-9`, `1.5E+9`, `32E1`, `%inf`, `%-inf`, `%nan`);
2. **Binary**: `\x03 + protobuf double wire format`.

{% note warning %}

Text encoding of floating-point numbers involves rounding; parsing the text back may yield a different value. Use binary encoding when exact values matter.

{% endnote %}

{% note warning %}

The values `%inf`, `%-inf`, and `%nan` are not valid in JSON, so calling `Yson::SerializeJson` on YSON that contains them results in an error.

{% endnote %}

### Boolean literals (`boolean`) {#boolean}

Two encodings:

1. **Text** (`%false`, `%true`);
2. **Binary** (`\x04`, `\x05`).

### Entity (`entity`) {#entity}

Entity is an atomic scalar value with no payload of its own. It is useful in many scenarios—for example, `entity` often represents `null`. An `entity` may still carry [attributes](#attributes).

Lexically, entity is written as `#`.

### Reserved literals {#special_literals}

Special tokens:

`;`, `=`, `#`, `[`, `]`, `{`, `}`, `<`, `>`, `)`, `/`, `@`, `!`, `+`, `^`, `:`, `,`, `~`.

Not all of these are used in YSON; some appear in [YPath](#ypath).

## Composite types {#composite_types}

### List (`list`) {#list}

Written as `[value; ...; value]`, where each `value` is a literal of any scalar or composite type.

Example: `[1; "hello"; {a=1; b=2}]`.

### Map (`map`) {#map}

Written as `{key = value; ...; key = value}`. Here each key is a string literal, and each `value` is a literal of any scalar or composite type.

Example: `{a = "hello"; "38 parrots" = [38]}`.

### Attributes {#attributes}

**Attributes** can be attached to any YSON literal. Syntax: `<key = value; ...; key = value> value`. Inside the angle brackets the syntax matches that of a map. For example, `<a = 10; b = [7,7,8]>"some-string"` or `<"44" = 44>44`. Attributes on `entity` literals are common, for example `<id="aaad6921-b5704588-17990259-7b88bad3">#`.

## Grammar {#grammar}

YSON data can be one of three kinds:

  1. **Node** (a single tree; in the grammar below, `<tree>`)
  2. **ListFragment** (values separated by `;`; `<list-fragment>`)
  3. **MapFragment** (key–value pairs separated by `;`; `<map-fragment>`)

The grammar below is defined up to whitespace, which may be inserted or removed freely between tokens:

```antlr
          <tree> = [ <attributes> ], <object>;
        <object> = <scalar> | <map> | <list> | <entity>;

        <scalar> = <string> | <int64> | <uint64> | <double> | <boolean>;
          <list> = "[", <list-fragment>, "]";
           <map> = "{", <map-fragment>, "}";
        <entity> = "#";
    <attributes> = "<", <map-fragment>, ">";

 <list-fragment> = { <list-item>, ";" }, [ <list-item> ];
     <list-item> = <tree>;

  <map-fragment> = { <key-value-pair>, ";" }, [ <key-value-pair> ];
<key-value-pair> = <string>, "=", <tree>;  % Key cannot be empty
```

The trailing `;` after the last element inside `<list-fragment>` and `<map-fragment>` may be omitted. The following forms are valid when reading:

#|
|| With trailing `;` | Short form ||
||

```yson
<a=b;>c
{a=b;}
1;2;3;
```

|

```yson
<a=b>c
{a=b}
1;2;3
```

 ||
|#


## Examples {#examples}

- Map (Node)

```yson
{ performance = 1 ; precision = 0.78 ; recall = 0.21 }
```

- Map (Node)

```yson
{ cv-precision = [ 0.85 ; 0.24 ; 0.71 ; 0.70 ] }
```


- List (Node)

```yson
[ 1; 2; 3; 4; 5 ]
```


- String (Node)

```yson
foobar
```

```yson
"hello world"
```

- Int64 (Node) `42`

- Double (Node) `3.1415926`

- ListFragment

```yson
{ key = a; value = 0 };
{ key = b; value = 1 };
{ key = c; value = 2; unknown_value = [] }
```

- MapFragment

```yson
do = create; type = table; scheme = {}
```

- HomeDirectory (Node)

```yson
{ home = { sandello = { mytable = <type = table> # ; anothertable = <type = table> # } ; monster = { } } }
```

## YPATH {#ypath}

This section describes YPath, a language for addressing objects inside YSON.

YPath expresses paths that identify nodes in YSON. It supports navigating the tree and attaching annotations useful for operations such as reading and writing properties.

Examples:

- `/0-25-3ec012f-406daf5c/@type` — path to the `type` attribute of the object with id `0-25-3ec012f-406daf5c`;

There are several YPath variants. In the simplest case, YPath is a string that encodes a path.

### Lexical structure {#simple_ypath_lexis}

A simple YPath string is split into **tokens** as follows:

1. **Special characters**: slash (`/`), at sign (`@`), ampersand (`&`), asterisk (`*`);
2. **Literals**: maximal non-empty sequences of non-special characters. Literals may use escaping `\<escape-sequence>`, where `<escape-sequence>` is one of `\`, `/`, `@`, `&`, `*`, `[`, `{`, or `x<hex1><hex2>` with hexadecimal digits `<hex1>` and `<hex2>`.

### Syntax and semantics {#simple_ypath_syntax}

Structurally, YPath looks like `/<relative-path>`. `<relative-path>` is parsed left to right into steps:

- **Child step**: a `/` token followed by a literal. Applies to maps and lists. For a map, the literal is the child name. Example: `/child` — child named `child`. For a list, the literal is a decimal integer index (zero-based). Negative indices count from the end. Examples: `/1` — second item; `/-1` — last item.
- **Attribute step**: `/@` followed by a literal. Can be used anywhere; moves to the attribute with that name. Example: `/@attr`.

{% note info %}

In YPath, relative paths start with a slash. The slash is not a separator like in file systems but part of the navigation command. Concatenating two YPaths is therefore plain string concatenation. This may look unusual but is convenient in many places and easy to get used to.

{% endnote %}

### Examples {#simple_ypath_examples}

```yql
$data = Yson(@@{"0-25-3ec012f-406daf5c" = {a=<why="I can just do it">1;b=2}}@@);
SELECT Yson::SerializeJson($data), Yson::SerializeJson(Yson::YPath($data, "/0-25-3ec012f-406daf5c/a/@/why"));
```

Result:

#|
|| column0 | column1 ||
||

```json
{
  "0-25-3ec012f-406daf5c": {
    "a": {
      "$attributes": {
        "why": "I can just do it"
      },
      "$value": 1
    },
    "b": 2
  }
}
```

| `"I can just do it"` ||
|#


```yql
$data = Yson(@@{
  a = <a=z;x=y>[
    {abc=123; def=456};
    {abc=234; xyz=789; entity0123 = #};
  ];
  b = {str = <it_is_string=%true>"hello"; "38 parrots" = [38]};
  entity0 = <here_you_can_store=something>#;
  }
@@);

SELECT Yson::ConvertToStringDict(Yson::YPath($data, "/a/@")) AS attrs_root,
Yson::SerializeJson(Yson::YPath($data, "/b/str/@")) AS attrs_b_str,
Yson::SerializeJson(Yson::YPath($data, "/b/str/@/it_is_string")) AS attr_exact,
Yson::SerializeJson(Yson::YPath($data, "/a/0")) as array_index0,
Yson::SerializeJson(Yson::YPath($data, "/a/-1")) as array_last,
Yson::SerializeJson(Yson::YPath($data, "/entity0")) as entity,
Yson::SerializeJson(Yson::YPath($data, "/a/#entity0123/abc")) as entity1,
Yson::SerializeJson(Yson::YPath($data, "/a")) AS whole_a,
Yson::SerializeJson($data) AS whole_data;
```

Result:

#|
|| attrs_root | attrs_b_str | attr_exact | array_index0 | array_last | entity | entity1 | whole_a | whole_data ||
||

```json
{
    "a": "z",
    "x": "y"
}
```

|

```json
{
    "it_is_string": true
}
```

|

```json
true
```

|

```json
{
    "abc": 123,
    "def": 456
}
```

|

```json
{
    "abc": 234,
    "entity0123": null,
    "xyz": 789
}
```

|

```json
{
    "$attributes": {
        "here_you_can_store": "something"
    },
    "$value": null
}
```

|

```json
null
```

|

```json
{
    "$attributes": {
        "a": "z",
        "x": "y"
    },
    "$value": [
        {
            "abc": 123,
            "def": 456
        },
        {
            "abc": 234,
            "entity0123": null,
            "xyz": 789
        }
    ]
}
```

|

```json
{
    "a": {
        "$attributes": {
            "a": "z",
            "x": "y"
        },
        "$value": [
            {
                "abc": 123,
                "def": 456
            },
            {
                "abc": 234,
                "xyz": 789
            }
        ]
    },
    "b": {
        "38 parrots": [
            38
        ],
        "str": {
            "$attributes": {
                "it_is_string": true
            },
            "$value": "hello"
        }
    }
}
```

||
|#


