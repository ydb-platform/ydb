{% if feature_mapreduce %}

## Working with files

### File

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two string arguments: alias and URL | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions [FilePath and FileContent](../../../builtins/basic.md#filecontent). This `PRAGMA` is a universal alternative to attaching files using built-in mechanisms from the [web](../../../interfaces/web.md#attach) or [console](../../../interfaces/cli.md#attach) clients.

YQL reserves the right to cache files at the URL for an indefinite period, hence, if there is a significant change in the content behind it, we strongly recommended to modify the URL by adding/editing dummy parameters.

### Folder

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two string arguments: prefix and URL | — | Static |

Attach a set of files to the query by URL. Works similarly to adding multiple files via [`PRAGMA File`](#file) by direct links to files with aliases obtained by combining a prefix with a file name using `/`.

### Library

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: the file name and an optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](../../export_import.md). The syntax type for the library is determined from the file extension:

* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yql`: For [s-expressions](/docs/s_expressions).

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the pre-attached file as in the following example:

```yql
PRAGMA library("a.sql","{{ corporate-paste }}/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","{{ corporate-paste }}/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

### Package

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Two or three arguments: package name, URL and optional token | — | Static |

Attach a hierarchical set of files to the query by URL, treating them as a package with a given name - an interconnected set of libraries.

Package name is expected to be given as ``project_name.package_name``; from package's libraries you can do [IMPORT](../../export_import.md) with a module name like ``pkg.project_name.package_name.maybe.nested.module.name``.

Example for a package with flat hierarchy which consists of two libraries - foo.sql and bar.sql:

``` yql
PRAGMA package("project.package", "{{ corporate-yt }}/{{ corporate-yt-cluster }}/path/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

You can also use text parameter value substitution in the URL:

``` yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package("project.package","{{ corporate-yt }}/{{ corporate-yt-cluster }}/{$_path}/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

### OverrideLibrary

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One argument: the file name | — | Static |

Treat the specified attached file as a library and override with it one of package's libraries.

File name is expected to be given as ``project_name/package_name/maybe/nested/module/name.EXTENSION``, extensions analagous to [PRAGMA Library](#library) are supported.

Example:

```yql
PRAGMA package("project.package", "{{ corporate-yt }}/{{ corporate-yt-cluster }}/path/to/package");
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```

{% endif %}
