# Working with files

## File {#file}

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments: alias, URL, and optional token name | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions [FilePath and FileContent](../../builtins/basic.md#filecontent).

When specifying the token name, its value will be used to access the target system.

## Folder {#folder}

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two or three string arguments: alias, URL, and optional token name | — | Static |

Attach a set of files to the query by URL. Works similarly to adding multiple files via [`PRAGMA File`](#file) by direct links to files with aliases obtained by combining a prefix with a file name using `/`.

When specifying the token name, its value will be used to access the target system.

## Library {#library}

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: the file name and an optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](../export_import.md). The syntax type for the library is determined from the file extension:

* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yqls`: For [s-expressions](/docs/s_expressions).

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the pre-attached file as in the following example:

```yql
PRAGMA library("a.sql","http://intranet.site/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","http://intranet.site/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

## Package

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Two or three arguments: package name, URL and optional token | — | Static |

Attach a hierarchical set of files to the query by URL, treating them as a package with a given name - an interconnected set of libraries.

Package name is expected to be given as ``project_name.package_name``; from package's libraries you can do [IMPORT](../export_import.md) with a module name like ``pkg.project_name.package_name.maybe.nested.module.name``.

Example for a package with flat hierarchy which consists of two libraries - foo.sql and bar.sql:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

You can also use text parameter value substitution in the URL:

```yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package("project.package","http://intranet.site/{$_path}/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

## OverrideLibrary

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One argument: the file name | — | Static |

Treat the specified attached file as a library and override with it one of package's libraries.

File name is expected to be given as ``project_name/package_name/maybe/nested/module/name.EXTENSION``, extensions analogous to [PRAGMA Library](#library) are supported.

Example:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```
