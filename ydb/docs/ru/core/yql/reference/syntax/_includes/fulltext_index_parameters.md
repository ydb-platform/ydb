  * общие параметры для всех полнотекстовых индексов:
    * `tokenizer` - тип токенизатора (`standard`, `whitespace` или `keyword`)
    * `use_filter_lowercase` - фильтр приведения к нижнему регистру (`true` или `false`)
    * `use_filter_length` - фильтр по длине токена (`true` или `false`); при значении `true` токены короче `filter_length_min` или длиннее `filter_length_max` не индексируются и не участвуют в поиске
    * `filter_length_min` - минимальная длина токена (положительное целое); применяется только при `use_filter_length=true`
    * `filter_length_max` - максимальная длина токена (положительное целое); применяется только при `use_filter_length=true`
    * `use_filter_snowball` - фильтр стемминга [Snowball](https://snowballstem.org/) (`true` или `false`)
    * `use_filter_superlemmer` - фильтр нормализации слов [SuperLemmer](../../../../dev/fulltext-indexes.md#superlemmer) (`true` или `false`, по умолчанию `false`), доступный только в Корпоративной СУБД Яндекса. Требует параметра `language` и включённого флага кластера `enable_super_lemmer`. Нельзя включить одновременно с `use_filter_snowball`, `use_filter_ngram` или `use_filter_edge_ngram`.
    * `language` - язык для Snowball или SuperLemmer (например, `"english"` или `"russian"`). Обязателен при включении одного из этих фильтров; не может быть задан, если оба выключены.
    * `use_filter_ngram` - фильтр [N-грамм](https://en.wikipedia.org/wiki/N-gram) (`true` или `false`)
    * `use_filter_edge_ngram` - фильтр краевых [N-грамм](https://en.wikipedia.org/wiki/N-gram) (`true` или `false`)
    * `filter_ngram_min_length` - минимальная длина N-граммы (положительное целое)
    * `filter_ngram_max_length` - максимальная длина N-граммы (положительное целое)
