# YQL SQL Syntax Highlighting Specfication

## Overview

This document specifies the syntax highlighting system for the YQL. The system specifies how to identify and categorize different syntactic elements in queries for highlighting porposes.

## Terms

- `Highlighting` is a _list_ of `Highlighting Unit`s that define how to recognize different parts of SQL syntax.

- `Highlighting Unit` is a language construction to be highlighted (e.g., keywords, identifiers, literals).

- `Highlighting Token` is a text fragment matched with a `Highlighting Unit`.

- `Highlighter` is an function parametrized by `Highlighting` transforming a text into a stream of `Highlighting Token`s.

- `Theme` is a mapping from `Highlighting Unit` to a `Color`.

## Highlighting Unit

Here are examples of `Highlighting Unit`s. They will evolve and should be taken from the JSON `Highlighting` programmatically. Only comments are always present, as they may require special processing.

- `keyword`: SQL reserved words (e.g., `SELECT`, `INSERT`, `FROM`).

- `punctuation`: Syntactic symbols (e.g., `.`, `;`, `(`, `)`).

- `identifier`: Unquoted names (e.g., table or column names).

- `quoted-identifier`: Backtick-quoted names (e.g., ``` `table` ```).

- `bind-parameter-identifier`: Parameter references (e.g., `$param`).

- `type-identifier`: Type names (e.g., `Int32`, `String`).

- `function-identifier`: Function names (e.g., `MIN`, `Math::Sin`).

- `literal`: Numeric constants (e.g., `123`, `1.23`).

- `string-literal`: Quoted strings (e.g., `"example"`).

- `comment`: Single-line (`--`) or multi-line (`/* */`) comments.

- `ws`: Spaces, tabs, newlines.

- `error`: Unrecognized syntax.

Each `Highlighting Unit` contains one or more `Patterns` that define how to recognize the unit in text.

## Pattern Matching

A `Pattern` consists of:

- `body`: The main regex pattern to match.

- `after`: A lookahead pattern.

- `is-case-insensitive`: Whether matching should be case-insensitive.

The matching behavior is equivalent to the regex: `body(?=after)`.

## Highlighter Algorithm

The highlighter algorithm can be described with the following pseudocode.

```python
# Consume matched tokens until empty.
# For each iteration:
# 1. Find the next token match (or error)
# 2. Emit the token
# 3. Continues with the remaining text
highlight(text) = 
  if text is not empty do 
    token = match(text)
    emit token
    highlight(text[token.length:])

# Select the longest match from all possible 
# patterns. Leftmost is chosen. If no match, 
# emits a 1-character error token for as a 
# recovery. 
match(text) =
  max of matches(text) by length
    or error token with length = 1

# For each highlighting unit and its patterns,
# attempt to match.
matches(text) = do
  unit <- highlighting.units
  pattern <- unit.patterns
  content <- match(text, pattern)
  yield token with unit, content

# Match both the pattern body and lookahead 
# (after) portion with case sensitivity settings.
match(text, pattern) = do
  body <- (
    regex pattern.body 
    matches text prefix 
    with pattern.case_sensivity)
  after <- (
    regex pattern.body
    matches text[body.length:] prefix
    with pattern.case_sensivity)
  yield body + after

# Special ANSI Comment handling.
# Recursively process nested multiline comments.
match(text, Comment if ANSI) =
  if text not starts with "/*" do
    return match(text, Comment if Default)

  text = text after "/*"
  loop do
    if text starts with "*/" do
      return text after "/*"

    if text starts with "/*" do
      budget = text before last "*/"
      match = match(budget, Comment if ANSI)
      text = text after match

    if match:
      continue

    if text is empty:
      return Nothing

    text = text[1:]
```

## Highlighting JSON Example

The highlighting can be generated using the `yql_highlight` tool in JSON format.

```json
{
    "units": [
        ...
        {
            "kind":"type-identifier",
            "patterns": [
                {
                    "body":"([a-z]|[A-Z]|_)([a-z]|[A-Z]|_|[0-9])*",
                    "after":"\\<"
                },
                {
                    "body":"Int32|Int16|Utf8|...",
                    "is-case-insensitive":true
                }
            ]
        },
        ...
    ]
}
```

## Test Suite

The reference implementation includes a comprehensive test suite that verifies correct highlighting behavior. The test suite is defined in JSON format with the following structure:

```json
{
  "SQL": [
    ["SELECT id, alias from users", "KKKKKK#_#II#P#_#IIIII#_#KKKK#_#IIIII"],
  ],
  "TypeIdentifier": [
    ["Bool(value)", "TTTT#P#IIIII#P"]
  ]
}
```

Where the first element is the SQL text to highlight and the second one is a string where each character represents the highlighting unit kind for each character in the input.

Here's the table representation of the unit kind to character mapping:

| Unit Kind                 | Character |
| ------------------------- | --------- |
| keyword                   | K         |
| punctuation               | P         |
| identifier                | I         |
| quoted-identifier         | Q         |
| bind-parameter-identifier | B         |
| type-identifier           | T         |
| function-identifier       | F         |
| literal                   | L         |
| string-literal            | S         |
| comment                   | C         |
| ws                        | _         |
| error                     | E         |

Note: The `#` is used to make tokens visually distinct from other.

The test driver pseudocode:

```cpp
run_test_suite = 
  let
    highlighting = load_highlighting()
    highlighter = make_highlighter(highlighting) 
    suite = load_sest_suite()
  in do
    scenario <- suite
    test <- scenario
    (input, expected) = test

    tokens = highlighter.highlight(input)
    actual = to_pattern(tokens)
    assert actual == expected
```

## Implementation Guidelines

- The module `yql/essentials/sql/v1/highlight` is a reference implementation of the `YQL` highlighting. Module includes a comprehensive test suite to check an implementation compliance with the specification. Also this module contains this specification document.

- The module `yql/essentials/tools/yql_highlight` contains a tool to play with the reference highlighting implementation and to generate various representation of highlighting (e.g. in JSON).

- The test suite data can be found at `yql/essentials/sql/v1/highlight/ut/suite.json`.
