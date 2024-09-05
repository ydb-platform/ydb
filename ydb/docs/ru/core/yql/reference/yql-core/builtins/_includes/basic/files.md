## FileContent и FilePath {#file-content-path}

{% if oss != true %}

Как [консольный](../../../interfaces/cli.md), так и [веб](../../../interfaces/web.md)-интерфейсы позволяют «прикладывать» к запросу произвольные именованные файлы. С помощью этих функций можно по имени приложенного файла получить его содержимое или путь в «песочнице» и в дальнейшем использовать в запросе произвольным образом.

{% endif %}

**Сигнатуры**
```
FilePath(String)->String
FileContent(String)->String
```

Аргумент `FileContent` и `FilePath` — строка с алиасом.

**Примеры**
``` yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```
## FolderPath {#folderpath}

Получение пути до корня директории с несколькими «приложенными» файлами с указанным общим префиксом.

**Сигнатура**
```
FolderPath(String)->String
```

Аргумент — строка с префиксом среди алиасов.

Также см. [PRAGMA File](../../../syntax/pragma.md#file) и [PRAGMA Folder](../../../syntax/pragma.md#folder).

**Примеры**
``` yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- в директории по возвращённому пути будут
                          -- находиться файлы 1.txt и 2.txt, скачанные по указанным выше ссылкам
```

## ParseFile

Получить из приложенного текстового файла список значений. Может использоваться в сочетании с [IN](../../../syntax/expressions.md#in) и прикладыванием файла по URL{% if oss != true %} <span style="color:gray;">(инструкции по прикладыванию файлов для {% if feature_webui %}[веб-интерфейса](../../../interfaces/web.md#attach) и {% endif %} [клиента](../../../interfaces/cli.md#attach))</span>{% endif %}.

Поддерживается только один формат файла — по одному значению на строку.{% if feature_udf_noncpp and oss != true %} Для чего-то более сложного прямо сейчас придется написать небольшую UDF на [Python](../../../udf/python.md) или [JavaScript](../../../udf/javascript.md). {% endif %}

**Сигнатура**
```
ParseFile(String, String)->List<T>
```

Два обязательных аргумента:

1. Тип ячейки списка: поддерживаются только строки и числовые типы;
2. Имя приложенного файла.

{% note info "Примечание" %}

Возвращаемое значение - ленивый список. Для многократного использования его нужно обернуть в функцию [ListCollect](../../list.md#listcollect)

{% endnote %}

**Примеры:**
``` yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```
``` yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt"));
```
