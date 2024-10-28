## Table naming rules

Every table in {{ ydb-short-name }} has a name. In YQL statements, table names are specified by identifiers that can be enclosed in backticks or not. For more information on identifiers, refer to [{#T}](../../../yql/reference/syntax/lexer.md#keywords-and-ids).

Table names in {{ ydb-short-name }} must meet the following requirements:

- Table name length must not exceed 255 characters.
- Table names must not contain dots only.
- Tables cannot be created in the system folder (`.sys`).
- Table names can include the following characters:
    - uppercase latin characters
    - lowercase latin characters
    - digits
    - special characters: `!`, `"`, `#`, `$`, `%`, `&`, `'`, `(`, `)`, `*`, `+`, `,`, `-`, `.`, `:`, `;`, `<`, `=`, `>`, `?`, `@`, `[`, `\`, `]`, `^`, `_`, `` ` ``, `{`, `|`, `}`, `~`.

        {% note info %}

        To use special characters in table names, surround table names with backticks (`` ` ``).

        {% endnote %}

## Column naming rules

Column names in {{ ydb-short-name }} must meet the following requirements:

- Column names must not start with the system prefix `__ydb_`.
- Column names can include the following characters:
    - uppercase latin characters
    - lowercase latin characters
    - digits
    - special characters: `-` and `_`.

