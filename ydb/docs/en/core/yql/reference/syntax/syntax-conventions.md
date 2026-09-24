# YQL command syntax conventions

Syntax blocks in the YQL reference are written as a synopsis — a diagram of the command that shows which clauses exist, in what order they appear, and which of them are optional. A synopsis is not formal BNF or EBNF grammar: it is meant to be read by a person and does not describe the full lexical structure of the language.

## Notation

| Notation | Meaning |
|----------|---------|
| `KEYWORD` | A keyword. Write it literally; the documentation uses uppercase, while YQL itself is case-insensitive for keywords. |
| `<name>` | A value you supply: an object name, a data type, an expression. The angle brackets are not part of the query. |
| `[ ... ]` | An optional construct. |
| `{ A \| B }` | A mandatory choice: specify exactly one option. |
| `[ A \| B ]` | An optional choice: specify one option or omit the construct. |
| `...` | The preceding construct can be repeated. |
| `[, ...]` | The preceding construct can be repeated in a comma-separated list. |
| `( )`, `,`, `=`, and other punctuation | Part of the query, written literally. |

Read nested brackets from the inside out: an outer optional construct may contain its own optional parts and its own choices.

If a construct is too large for the main block, it is shown in a separate block after the synopsis, introduced with "where `<name>` is".

## Examples

### Mandatory parts

```yql
TRUNCATE TABLE <table_name>;
```

`TRUNCATE TABLE` is written literally; replace `<table_name>` with a table name:

```yql
TRUNCATE TABLE my_table;
```

### Optional construct

```yql
DROP VIEW [IF EXISTS] <view_name>
```

Both forms are valid:

```yql
DROP VIEW recent_series;
DROP VIEW IF EXISTS recent_series;
```

### Repetition and nesting

```yql
CREATE TABLE [IF NOT EXISTS] <table_name> (
    <column_name> <column_data_type> [<column_option> ...] [, ...]
    PRIMARY KEY ( <column_name> [, ...] )
)
[WITH ( <setting_name> = <setting_value> [, ...] )]
```

A column is described by its name and type, followed by any number of options in a row, and the whole column definition repeats in a comma-separated list. Items inside `PRIMARY KEY` and inside the `WITH` block are comma-separated as well, but the constructs themselves differ: `PRIMARY KEY` is mandatory, while the entire `WITH` block can be omitted.

Column options are shown in a separate block:

```yql
FAMILY <family_name>
[NULL | NOT NULL]
DEFAULT <default_value>
ENCODING ( [ { OFF | DICT } ] )
```

Here `[NULL | NOT NULL]` is an optional choice, and `{ OFF | DICT }` is a mandatory choice inside an optional construct: if you write `ENCODING`, the parentheses are required, and the value inside them can either be omitted or set to one of the two options.

## Angle brackets in data types

Angle brackets also appear in container type names: `List<Int32>`, `Optional<String>`, `Struct<name:String>`. There they are part of the type name, not a placeholder. Tell them apart by context: in a command synopsis, `<...>` is replaced by your value, while in a type name it is written to the query as is.

## See also

* [{#T}](lexer.md)
* [{#T}](expressions.md)
