## Database Object Naming Rules {#object-naming-rules}

Every [scheme object](../../../concepts/glossary.md#scheme-object) in {{ ydb-short-name }} has a name. In YQL statements, object names are specified by identifiers that can be enclosed in backticks or not. For more information on identifiers, refer to [{#T}](../../../yql/reference/syntax/lexer.md#keywords-and-ids).

Scheme object names in {{ ydb-short-name }} must meet the following requirements:

- Object names can include the following characters:
    - Uppercase Latin characters
    - Lowercase Latin characters
    - Digits
    - Special characters: `.`, `-`, and `_`
- Object name length must not exceed 255 characters.
- Objects cannot be created in folders which names start with a dot, such as `.sys`, `.metadata`, and `.sys_health`.

## Column Naming Rules {#column-naming-rules}

Column names in {{ ydb-short-name }} must meet the following requirements:

- Column names can include the following characters:
    - Uppercase Latin characters
    - Lowercase Latin characters
    - Digits
    - Special characters: `-` and `_`
- Column names must not start with the system prefix `__ydb_`.
