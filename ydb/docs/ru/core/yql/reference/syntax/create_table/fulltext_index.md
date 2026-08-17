# Полнотекстовый индекс

{% if backend_name == 'YDB' %}[Полнотекстовые индексы](../../../../dev/fulltext-indexes.md){% else %}Полнотекстовые индексы{% endif %} в {% if backend_name == 'YDB' %}[строковых](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}строковых{% endif %} таблицах создаются с помощью того же синтаксиса, что и [вторичные индексы](secondary_index.md), при указании `fulltext_plain` или `fulltext_relevance` в качестве типа индекса. Подмножество доступного для полнотекстовых индексов синтаксиса:

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING fulltext_plain | fulltext_relevance
        ON ( <text_column> )
        [COVER ( <cover_columns> )]
        [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [,   ...]
)
```

Где:

* `<index_name>` - уникальное имя индекса для доступа к данным
* `SYNC` - указывает на синхронную запись данных в индекс. Это единственная доступная на данный момент опция, явно указывать не обязательно.
* `<text_column>` - одна колонка таблицы с текстовым содержимым (на данный момент поддерживается только одна индексируемая колонка)
* `<cover_columns>` - список дополнительных колонок создаваемой таблицы, которые будут сохранены в индексе для возможности их извлечения без обращения к основной таблице
* `<parameter_name>` и `<parameter_value>` - список параметров в формате ключ-значение:

{% include [fulltext_index_parameters.md](../_includes/fulltext_index_parameters.md) %}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

## Примеры

### Базовый пример

```yql
CREATE TABLE articles (
    id Uint64,
    title String,
    body String,
    INDEX ft_idx GLOBAL USING fulltext_relevance
    ON (body) COVER (title)
    WITH (
        tokenizer=standard,
        use_filter_lowercase=true
    ),
    PRIMARY KEY (id)
)
```

### Пример с N-граммами

N-граммы позволяют находить подстроки внутри слов. При использовании фильтра N-грамм каждое слово разбивается на все возможные подстроки заданной длины. Например, слово "search" с параметрами `filter_ngram_min_length=3` и `filter_ngram_max_length=5` будет разбито на: "sea", "ear", "arc", "rch" (3-граммы), "sear", "earc", "arch" (4-граммы), "searc", "earch" (5-граммы). Это позволяет находить документы по частичному совпадению в любой части слова.

```yql
CREATE TABLE products (
    id Uint64,
    name String,
    description String,
    INDEX ft_ngram_idx GLOBAL USING fulltext_plain
    ON (name)
    WITH (
        tokenizer=standard,
        use_filter_lowercase=true,
        use_filter_ngram=true,
        filter_ngram_min_length=3,
        filter_ngram_max_length=5
    ),
    PRIMARY KEY (id)
)
```

### Пример с краевыми N-граммами

Краевые N-граммы (edge n-grams) создают подстроки только от начала слова. Например, слово "search" с параметрами `filter_ngram_min_length=2` и `filter_ngram_max_length=10` будет разбито на: "se", "sea", "sear", "searc", "search". Это особенно полезно для реализации функции автодополнения (autocomplete), когда нужно находить слова, начинающиеся с введённого пользователем префикса.

```yql
CREATE TABLE users (
    id Uint64,
    username String,
    email String,
    INDEX ft_edge_ngram_idx GLOBAL USING fulltext_plain
    ON (username)
    WITH (
        tokenizer=standard,
        use_filter_lowercase=true,
        use_filter_edge_ngram=true,
        filter_ngram_min_length=2,
        filter_ngram_max_length=10
    ),
    PRIMARY KEY (id)
)
```
