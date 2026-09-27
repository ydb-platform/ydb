  * общие параметры для всех полнотекстовых индексов:
    * `analyzer` - готовый анализатор: `standard` (стандартный токенизатор, нижний регистр и удаление английских стоп-слов), `snowball` (стандартный токенизатор, нижний регистр, удаление стоп-слов и стемминг Snowball; требуется `language`) или `keyword` (весь текст становится одним токеном с сохранением регистра)
    * `tokenizer` - тип токенизатора (`standard`, `whitespace`, `alphanumeric` или `keyword`)
    * `use_filter_lowercase` - фильтр приведения к нижнему регистру (`true` или `false`)
    * `use_filter_stopwords` - удаление частотных слов, например `the` и `and` (`true` или `false`). Поддерживаются английский и русский языки; если `language` не указан, используется английский
    * `use_filter_length` - фильтр по длине токена (`true` или `false`); при значении `true` токены короче `filter_length_min` или длиннее `filter_length_max` не индексируются
    * `filter_length_min` - минимальная длина токена (положительное целое); применяется только при `use_filter_length=true`
    * `filter_length_max` - максимальная длина токена (положительное целое); применяется только при `use_filter_length=true`
    * `use_filter_snowball` - фильтр стемминга [Snowball](https://snowballstem.org/) (`true` или `false`)
    * `language` - язык или список языков через запятую для стеммера [Snowball](https://snowballstem.org/), например `"english,russian"`
    * `use_filter_ngram` - фильтр [N-грамм](https://en.wikipedia.org/wiki/N-gram) (`true` или `false`)
    * `use_filter_edge_ngram` - фильтр краевых [N-грамм](https://en.wikipedia.org/wiki/N-gram) (`true` или `false`)
    * `filter_ngram_min_length` - минимальная длина N-граммы (положительное целое)
    * `filter_ngram_max_length` - максимальная длина N-граммы (положительное целое)
