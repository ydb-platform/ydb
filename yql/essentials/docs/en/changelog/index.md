## Changes in YQL versions

This section contains a list of changes in different versions of the YQL

## General description of YQL versions

The goal of language versioning is a controlled and user-safe evolution of the language, within which it is possible to:

* add new features;

* change default values ​​for pragmas or perform other non-backward-compatible changes;

* remove obsolete features.

The starting version of the language is `2025.01`.

Once a year, a new version of the language is released, called `YEAR.X`, for example `2025.01`. Other versions in the same year will increase the minor version number (`YEAR.02`, etc.).

For each version of the type `YEAR.X` **support is guaranteed for three years with the preservation of semantics and bug fixes**.

When version `YEAR+2.1` is released, a warning is issued for all versions of the type `YEAR.X` that they will soon cease to be supported.

When version `YEAR+3.1` is released, a warning is issued for all versions of the type `YEAR.X` that they are not supported and query execution is not guaranteed.

When specifying a version of `YEAR.X` greater than what is currently available in some service/library/tool, a query error is issued.

When each version is released, the documentation in the changelog indicates the changes included in this version and recommendations for migration.
