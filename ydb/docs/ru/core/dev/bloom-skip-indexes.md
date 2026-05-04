# Блум индексы

Блум индексы — это локальные вспомогательные структуры, которые ускоряют селективные запросы за счёт пропуска фрагментов данных, в которых с высокой вероятностью нет искомых значений. В отличие от глобальных [вторичных индексов](../concepts/glossary.md#secondary-index), они работают как локальные фильтры чтения и уменьшают объём данных, которые нужно фактически читать.

## Типы {#types}

Поддерживаются два типа локальных Блум индексов:

* `bloom_filter` — фильтр Блума по точным значениям индексируемой колонки (удобно для равенств и наборов значений).
* `bloom_ngram_filter` — Bloom-фильтр по n-граммам строковой колонки.

Синтаксис создания и список параметров приведены в [CREATE TABLE](../yql/reference/syntax/create_table/bloom_skip_index.md) и в [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

## Параметры и значения по умолчанию {#parameters}

Краткий перечень параметров `WITH (...)` и значений по умолчанию:

{% include [bloom_skip_index_parameters.md](../yql/reference/syntax/_includes/bloom_skip_index_parameters.md) %}

Подробное описание параметров при создании индекса: [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-bloom).

Изменение параметров после создания: [`ALTER INDEX`](../yql/reference/syntax/alter_table/indexes.md#alter-index).

## Примеры {#examples}

Создание таблицы с индексом `bloom_filter`:

```yql
CREATE TABLE /Root/events (
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
ALTER TABLE `/Root/events`
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
ALTER TABLE `/Root/events` ALTER INDEX idx_ngram SET (
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

## Ограничения и особенности {#limitations}

{% include [bloom_skip_index_limitations.md](../yql/reference/syntax/_includes/bloom_skip_index_limitations.md) %}

## Дополнительные материалы {#see-also}

* [Вторичные индексы](secondary-indexes.md)
* [Справочник YQL: CREATE TABLE — Блум индекс](../yql/reference/syntax/create_table/bloom_skip_index.md)
* [Справочник YQL: SELECT](../yql/reference/syntax/select/index.md)
* [Справочник YQL: ALTER TABLE — индексы](../yql/reference/syntax/alter_table/indexes.md#local-bloom)
* [Быстрый старт (рецепт)](../recipes/bloom-skip-indexes/bloom-skip-index-quickstart.md)
