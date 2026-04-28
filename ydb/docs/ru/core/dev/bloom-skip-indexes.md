# Bloom skip индексы

Bloom skip индексы — это локальные вспомогательные структуры, которые ускоряют селективные запросы за счёт пропуска фрагментов данных, в которых с высокой вероятностью нет искомых значений. В отличие от глобальных [вторичных индексов](../concepts/glossary.md#secondary-index), они не являются отдельной таблицей с собственным ключом для `VIEW` и не требуют явного указания индекса в запросе: использование определяется оптимизатором при выполнении предикатов по индексируемым колонкам.

Общее описание роли таких индексов в модели выполнения запросов см. в разделе [Bloom skip индексы и фильтрация](../concepts/query_execution/bloom_skip_indexes.md).

## Типы {#types}

Поддерживаются два типа локальных Bloom skip индексов:

* `bloom_filter` — фильтр Блума по точным значениям индексируемой колонки (удобно для равенств и наборов значений).
* `bloom_ngram_filter` — Bloom-фильтр по n-граммам строковой колонки (удобно для поиска по подстроке в сочетании с предикатами вроде `LIKE`).

Синтаксис создания и список параметров приведены в [CREATE TABLE: Bloom skip индекс](../yql/reference/syntax/create_table/bloom_skip_index.md) и в [`ALTER TABLE ADD INDEX`](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

## Ограничения и особенности {#limitations}

* Индекс всегда локальный (`LOCAL`); глобального варианта нет.
* Секции `COVER (...)` и data columns не поддерживаются.
* Для колоночных таблиц в `ON (...)` должна быть ровно одна колонка. Для строковых таблиц допускается несколько колонок в `ON (...)` (в зависимости от сценария и типа индекса).
* В запросах не используется синтаксис `VIEW <index>` (в отличие, например, от [полнотекстовых индексов](fulltext-indexes.md)).

## Параметры и значения по умолчанию {#parameters}

Краткий перечень параметров `WITH (...)` и значений по умолчанию:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

Изменение параметров после создания: [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Примеры {#examples}

Создание таблицы с индексом `bloom_filter`:

```yql
CREATE TABLE events (
    id Uint64,
    resource_id Utf8,
    PRIMARY KEY (id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
);
```

Добавление `bloom_ngram_filter` к существующей таблице:

```yql
ALTER TABLE `/Root/logs`
  ADD INDEX idx_ngram LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

Изменение параметров:

```yql
ALTER TABLE `/Root/logs` ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## См. также {#see-also}

* [Вторичные индексы](secondary-indexes.md)
* [Справочник YQL: `ALTER TABLE` / индексы](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Рецепты: Bloom skip индексы](../recipes/bloom-skip-indexes/index.md)
