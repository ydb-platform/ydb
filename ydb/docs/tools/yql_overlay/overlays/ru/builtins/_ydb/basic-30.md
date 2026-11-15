### Сигнатура

```yql
AsTagged(T, tagName:String)->Tagged<T,tagName>
AsTagged(T?, tagName:String)->Tagged<T,tagName>?

Untag(Tagged<T, tagName>)->T
Untag(Tagged<T, tagName>?)->T?
```

Обязательные аргументы:

1. Значение произвольного типа;
2. Имя метки.

Возвращает копию значения из первого аргумента с указанной меткой в типе данных.

Примеры сценариев использования:

* Возвращение на клиент для отображения в веб-интерфейсе медиа-файлов из base64-encoded строк{% if feature_webui %}. Поддержка меток в веб-UI YQL [описана здесь](../../interfaces/web_tagged.md){% endif %}.

{% if feature_mapreduce %}

* Защита на границах вызова UDF от передачи некорректных значений;

{% endif %}

* Дополнительные уточнения на уровне типов возвращаемых колонок.


{% if feature_bulk_tables %}

## TablePath {#tablepath}

Доступ к текущему имени таблицы, что бывает востребовано при использовании [CONCAT](../../syntax/select/concat.md#concat), [RANGE](../../syntax/select/concat.md#range) и других подобных механизмов.

### Сигнатура

```yql
TablePath()->String
```

Аргументов нет. Возвращает строку с полным путём, либо пустую строку и warning при использовании в неподдерживаемом контексте (например, при работе с подзапросом или диапазоном из 1000+ таблиц).

{% note info %}

Функции [TablePath](#tablepath), [TableName](#tablename) и [TableRecordIndex](#tablerecordindex) не работают для временных и анонимных таблиц (возвращают пустую строку или 0 для [TableRecordIndex](#tablerecordindex)).
Данные функции вычисляются в момент [выполнения](../../syntax/select/index.md#selectexec) проекции в `SELECT`, и к этому моменту текущая таблица уже может быть временной.
Чтобы избежать такой ситуации, следует поместить вычисление этих функций в подзапрос, как это сделано во втором примере ниже.

{% endnote %}

### Примеры

```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Получить имя таблицы из пути к таблице. Путь можно получить через функцию [TablePath](#tablepath), или в виде колонки `Path` при использовании табличной функции {% if feature_mapreduce %}[FOLDER](../../syntax/select/folder.md){% else %} `FOLDER`{% endif %}.

### Сигнатура

```yql
TableName()->String
TableName(String)->String
TableName(String, String)->String
```

Необязательные аргументы:

* путь к таблице, по умолчанию используется `TablePath()` (также см. его ограничения);
* указание системы ("yt"), по правилам которой выделяется имя таблицы. Указание системы нужно только в том случае, если с помощью {% if feature_mapreduce %}[USE](../../syntax/use.md){% else %}`USE`{% endif %} не указан текущий кластер.

### Примеры

```yql
USE hahn;
SELECT TableName() FROM CONCAT(table_a, table_b);
```

```yql
SELECT TableName(Path, "yt") FROM cluster.FOLDER(folder_name);
```

## TableRecordIndex {#tablerecordindex}

Доступ к текущему порядковому номеру строки в исходной физической таблице, **начиная с 1** (зависит от реализации хранения).

### Сигнатура

```yql
TableRecordIndex()->Uint64
```

Аргументов нет. При использовании в сочетании с [CONCAT](../../syntax/select/concat.md#concat), [RANGE](../../syntax/select/concat.md#range) и другими подобными механизмами нумерация начинается заново для каждой таблицы на входе. В случае использования в некорректном контексте возвращает 0.

### Пример

```yql
SELECT TableRecordIndex() FROM my_table;
```


{% endif %}

## TableRow{% if feature_join %}, JoinTableRow{% endif %} {#tablerow}

Получение всей строки таблицы целиком в виде структуры. Аргументов нет{% if feature_join %}. `JoinTableRow` в случае `JOIN`-ов всегда возвращает структуру с префиксами таблиц{% endif %}.

### Сигнатура

```yql
TableRow()->Struct
```

### Пример

```yql
SELECT TableRow() FROM my_table;
```


{% if feature_mapreduce %}

## FileContent и FilePath {#file-content-path}

{% if oss != true %}

Как [консольный](../../interfaces/cli.md), так и [веб](../../interfaces/web.md)-интерфейсы позволяют «прикладывать» к запросу произвольные именованные файлы. С помощью этих функций можно по имени приложенного файла получить его содержимое или путь в «песочнице» и в дальнейшем использовать в запросе произвольным образом.

{% endif %}

### Сигнатуры

```yql
FilePath(String)->String
FileContent(String)->String
```

Аргумент `FileContent` и `FilePath` — строка с алиасом.

### Примеры

```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```
## FolderPath {#folderpath}

Получение пути до корня директории с несколькими «приложенными» файлами с указанным общим префиксом.

### Сигнатура

```yql
FolderPath(String)->String
```

Аргумент — строка с префиксом среди алиасов.

Также см. [PRAGMA File](../../syntax/pragma.md#file) и [PRAGMA Folder](../../syntax/pragma.md#folder).

### Примеры

```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- в директории по возвращённому пути будут
                          -- находиться файлы 1.txt и 2.txt, скачанные по указанным выше ссылкам
```

## ParseFile

Получить из приложенного текстового файла список значений. Может использоваться в сочетании с [IN](../../syntax/expressions.md#in) и прикладыванием файла по URL{% if oss != true %} (инструкции по прикладыванию файлов для {% if feature_webui %}[веб-интерфейса](../../interfaces/web.md#attach) и {% endif %} [клиента](../../interfaces/cli.md#attach)){% endif %}.

Поддерживается только один формат файла — по одному значению на строку.{% if feature_udf_noncpp and oss != true %} Для чего-то более сложного прямо сейчас придется написать небольшую UDF на [Python](../../udf/python.md) или [JavaScript](../../udf/javascript.md).{% endif %}

### Сигнатура

```yql
ParseFile(String, String)->List<T>
```

Два обязательных аргумента:

1. Тип ячейки списка: поддерживаются только строки и числовые типы;
2. Имя приложенного файла.

{% note info %}

Возвращаемое значение - ленивый список. Для многократного использования его нужно обернуть в функцию [ListCollect](../list.md#listcollect)

{% endnote %}

### Примеры

```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```

```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt");
```


## WeakField {#weakfield}

Вытаскивает колонку таблицы из строгой схемы, если оно там есть, либо из полей `_other` и `_rest`. В случае отсутствия значения возвращается `NULL`.

Синтаксис: `WeakField([<table>.]<field>, <type>[, <default_value>])`.

Значение по умолчанию используется только в случае отсутствия колонки в схеме данных. Чтобы подставить значение по умолчанию в любом случае можно воспользоваться [COALESCE](#coalesce).

### Примеры

```yql
SELECT
    WeakField(my_column, String, "no value"),
    WeakField(my_table.other_column, Int64)
FROM my_table;
```

{% endif %}
