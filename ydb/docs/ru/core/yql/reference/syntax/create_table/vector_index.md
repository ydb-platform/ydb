# Векторный индекс

{% if backend_name == "YDB" and oss == true %}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

{% include [limitations](../../../../_includes/vector_index_limitations.md) %}

{% endif %}

{% note warning %}

Создание пустой таблицы с векторным индексом в настоящее время не имеет практического смысла, так как модификация данных в таблицах с векторными индексами пока не поддерживается.

Следует использовать {% if feature_secondary_index %}[команду](../alter_table/indexes.md){% else %}команду{% endif %} `ALTER TABLE ... ADD INDEX`  для добавления векторного индекса в существующую таблицу.

{% endnote %}

Конструкция `INDEX` используется для определения {% if backend_name == 'YDB' %}[векторного индекса](../../../../concepts/glossary.md#vector-index){% else %}векторного индекса{% endif %} в {% if backend_name == 'YDB' %}[строчно-ориентированных](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}строчно-ориентированных{% endif %} таблицах:

```yql
CREATE TABLE table_name (
    ...
    INDEX <имя_индекса> GLOBAL [SYNC] USING <тип_индекса> ON ( <колонки_индекса> ) COVER ( <покрывающие_колонки> ) WITH ( <параметры_индекса> ),
    ...
)
```

Где:

* **имя_индекса** - уникальное имя индекса для доступа к данным
* **SYNC** - указывает на синхронную запись данных в индекс. Если не указано - синхронная.
* **тип_индекса** - тип индекса, в настоящее время поддерживается только `vector_kmeans_tree`
* **колонки_индекса** - список колонок таблицы через запятую, используемых для поиска по индексу (последняя колонка используется как эмбеддинг, остальные - как колонки для ускорения фильтрации)
* **покрывающие_колонки** - дополнительные колонки таблицы, сохраняемые в индексе для возможности их извлечения без обращения к основной таблице
* **параметры_индекса** - список параметров в формате ключ-значение:
  * общие параметры для всех векторных индексов:
    * `vector_dimension` - размерность вектора эмбеддинга (<= 16384);
    * `vector_type` - тип значений вектора (`float`, `uint8`, `int8`, `bit`);
    * `distance` - функция расстояния (`cosine`, `manhattan`, `euclidean`) или `similarity` - функция схожести (`inner_product`, `cosine`).
  * специфичные параметры для `vector_kmeans_tree`:
    * `clusters` - количество центроидов для алгоритма k-means (значения > 1000 могут ухудшить производительность);
    * `levels` - количество уровней в дереве.

{% note warning %}

Параметры `distance` и `similarity` не могут быть указаны одновременно.

{% endnote %}

{% note warning %}

Векторные индексы с `vector_type=bit` в настоящее время не поддерживаются

{% endnote %}

## Пример

```yql
CREATE TABLE user_articles (
    article_id Uint64,
    user String,
    title String,
    text String,
    embedding String,
    INDEX emb_cosine_idx GLOBAL SYNC USING vector_kmeans_tree
    ON (user, embedding) COVER (title, text)
    WITH (distance="cosine", vector_type="float", vector_dimension=512, clusters=128, levels=2),
    PRIMARY KEY (article_id)
)
```
