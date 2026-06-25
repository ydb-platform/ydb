# Блум-индексы

Блум-индексы — [локальные](../concepts/glossary.md#local-index) вспомогательные структуры на основе [фильтра Блума](https://ru.wikipedia.org/wiki/Фильтр_Блума), которые ускоряют селективные запросы за счёт пропуска фрагментов данных, в которых искомое значение гарантированно отсутствует. В отличие от глобальных [вторичных индексов](../concepts/glossary.md#secondary-index), они работают как фильтры чтения основной таблицы и уменьшают объём данных, которые нужно фактически читать.

## Типы {#types}

Поддерживаются два типа:

* `bloom_filter` — фильтр по точным значениям индексируемой колонки; подходит для условий равенства и `IN` (см. [когда применять](../concepts/query_execution/local_indexes.md#bloom-skip-indexes)).
* `bloom_ngram_filter` — фильтр по n-граммам строковой колонки (`String`, `Utf8`); подходит для поиска подстрок и шаблонов `LIKE` в [колоночных таблицах](../concepts/glossary.md#column-oriented-table).

## Параметры и значения по умолчанию {#parameters}

Полный перечень параметров `WITH (...)` и значений по умолчанию:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

Синтаксис создания: [CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md), [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

Изменение параметров после создания: [ALTER INDEX](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Примеры {#examples}

Создание таблицы с индексом `bloom_filter`:

```yql
CREATE TABLE events (
    id Uint64,
    resource_id Utf8,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
);
```

Добавление `bloom_ngram_filter` к существующей таблице:

```yql
ALTER TABLE events
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
ALTER TABLE events ALTER INDEX idx_ngram SET (
    ngram_size = 4,
    false_positive_probability = 0.005,
    case_sensitive = false
);
```

## Настройка параметров {#tuning}

Если после развёртывания индекса запросы всё ещё читают слишком много данных, попробуйте уменьшить `false_positive_probability`. Если индекс занимает слишком много места — попробуйте увеличить его.

Значения меняются через [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index), см. [пример](#examples).

Рекомендации:

* Начните с `false_positive_probability = 0.01`, затем корректируйте по фактическим метрикам чтения и размеру индекса.
* `ngram_size` для `bloom_ngram_filter` обычно начинают с `3`; увеличение значения может сделать фильтрацию строже для более длинных подстрок.
* Меняйте параметры по одному и сравнивайте результат на одной и той же нагрузке.

## Особенности и ограничения {#limitations}

{% include [bloom_skip_index_features.md](../yql/reference/syntax/_includes/bloom_skip_index_features.md) %}

{% note info "Ограничения" %}

{% include [bloom_skip_index_limitations.md](../yql/reference/syntax/_includes/bloom_skip_index_limitations.md) %}

{% endnote %}

## Дополнительные материалы {#see-also}

* [Вторичные индексы](secondary-indexes.md)
* [Справочник YQL: CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md)
* [Справочник YQL: SELECT](../yql/reference/syntax/select/index.md)
* [Справочник YQL: ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Быстрый старт](../recipes/bloom-skip-indexes/bloom-skip-index-quickstart.md)
