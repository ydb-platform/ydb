# Работа с файлами

## File {#file}

| Тип значения | По умолчанию | Статическая /<br/>динамическая |
| --- | --- | --- |
| Два или три строковых аргумента — алиас, URL и опциональное имя токена | — | Статическая |

Приложить файл к запросу по URL. Использовать приложенные файлы можно с помощью встроенных функций [FilePath и FileContent](../../builtins/basic.md#filecontent).

При указании имени токена, его значение будет использоваться для обращения к целевой системе.

## FileOption

| Тип значения                                    | По умолчанию | Статическая /<br/> динамическая |
|-------------------------------------------------|--------------|--------------------------------|
| Три строковых аргумента — алиас, ключ, значение | —            | Статическая                    |

Установить для указанного файла опцию по заданному ключу в заданное значение. Файл с этим алиасом уже должен быть объявлен через [PRAGMA File](#file) или приложен к запросу.

## Folder {#folder}

| Тип значения | По умолчанию | Статическая /<br/>динамическая |
| --- | --- | --- |
| Два или три строковых аргумента — префикс, URL и опциональное имя токена | — | Статическая |

Приложить набор файлов к запросу по URL. Работает аналогично добавлению множества файлов через [PRAGMA File](#file) по прямым ссылкам на файлы с алиасами, полученными объединением префикса с именем файла через `/`.

При указании имени токена, его значение будет использоваться для обращения к целевой системе.

## Library {#library}

| Тип значения | По умолчанию | Статическая /<br/>динамическая |
| --- | --- | --- |
| Один или два аргумента &mdash; имя файла и опциональный URL | — | Статическая |

Интерпретировать указанный приложенный файл как библиотеку, из которой можно делать [IMPORT](../export_import.md). Тип синтаксиса библиотеки определяется по расширению файла:
* `.sql` для YQL диалекта SQL <span style="color: green;">(рекомендуется)</span>;
* `.yqls` для [s-expressions](/docs/s_expressions).

Пример с приложенным файлом к запросу:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

В случае указания URL библиотека скачивается с него, а не с предварительного приложенного файла, как в следующем примере:

```yql
PRAGMA library("a.sql","http://intranet.site/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

При этом можно использовать подстановку текстовых параметров в URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","http://intranet.site/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

## Package

| Тип значения | По умолчанию | Статическая /<br/>динамическая |
| --- | --- | --- |
| Два или три аргумента &mdash; имя пакета, URL и опциональный токен | — | Статическая |

Приложить иерархический набор файлов к запросу по URL, интерпретируя их в качестве пакета с указанным именем &mdash; взаимосвязанного набора библиотек.

Имя пакета ожидается в формате ``project_name.package_name``; из библиотек пакета в дальнейшем можно делать [IMPORT](../export_import.md) с именем модуля вида ``pkg.project_name.package_name.maybe.nested.module.name``.

Пример для пакета с плоской иерархией, состоящего из двух библиотек &mdash; foo.sql и bar.sql:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

При этом можно использовать подстановку текстовых параметров в URL:

```yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package("project.package","http://intranet.site/{$_path}/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

## OverrideLibrary

| Тип значения | По умолчанию | Статическая /<br/>динамическая |
| --- | --- | --- |
| Один аргумент &mdash; имя файла | — | Статическая |

Интерпретировать указанный приложенный файл как библиотеку и перекрыть ей одну из библиотек пакета.

Имя файла ожидается в формате ``project_name/package_name/maybe/nested/module/name.EXTENSION``, поддерживаются аналогичные [PRAGMA Library](#library) расширения.

Пример:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```
