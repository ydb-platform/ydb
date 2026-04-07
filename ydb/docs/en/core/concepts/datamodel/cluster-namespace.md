# Cluster Namespace and Object Naming

In {{ ydb-short-name }}, all schema objects are organized into a hierarchical namespace called the **cluster schema**. This structure determines how databases, directories, and other objects are arranged and named, providing a clear and scalable way to manage data.

## Cluster Schema

The cluster schema forms a tree-like hierarchy where:

- The **root** represents the top level of the namespace.
- At the first level are [databases](../glossary.md#database).
- Inside each database, you can create nested [directories](dir.md) to build a custom hierarchy.
- [Schema objects](../glossary.md#scheme-object)—such as tables, views, and indexes—can reside either at the database root or within any directory.

![cluster schema diagram](_assets/cluster-scheme.png =500x)

## Naming Rules for Schema Objects {#object-naming-rules}

Every [schema object](../glossary.md#scheme-object) in {{ ydb-short-name }} has a name. In YQL expressions, object names are used as identifiers, which can be unquoted or enclosed in backticks (`` ` ``). For details on identifier syntax, see [{#T}](../../yql/reference/syntax/lexer.md#keywords-and-ids).

Valid names for schema objects must meet the following criteria:

- **Allowed characters**:
  - Latin letters (`A–Z`, `a–z`)
  - Digits (`0–9`)
  - Special characters: `.`, `-`, and `_`
- **Maximum length**: 255 characters
- **Placement restrictions**: Objects cannot be created in directories with names that start with a dot (for example, `.sys`, `.metadata`, `.sys_health`)