  * common parameters for all fulltext indexes:
    * `tokenizer` - tokenizer type (`standard`, `whitespace`, or `keyword`)
    * `use_filter_lowercase` - lowercase filter (`true` or `false`)
    * `use_filter_length` - token length filter (`true` or `false`); when `true`, tokens shorter than `filter_length_min` or longer than `filter_length_max` are not indexed and are ignored during search
    * `filter_length_min` - minimum token length (positive integer); only applied when `use_filter_length=true`
    * `filter_length_max` - maximum token length (positive integer); only applied when `use_filter_length=true`
    * `use_filter_snowball` - [Snowball](https://snowballstem.org/) stemmer filter (`true` or `false`)
    * `use_filter_superlemmer` - [SuperLemmer](../../../../dev/fulltext-indexes.md#superlemmer) word normalization filter (`true` or `false`, default `false`), available only in Yandex Enterprise Database. Requires `language` and the `enable_super_lemmer` cluster feature flag. Cannot be enabled together with `use_filter_snowball`, `use_filter_ngram`, or `use_filter_edge_ngram`.
    * `language` - language for Snowball or SuperLemmer (for example, `"english"` or `"russian"`). Required when either filter is enabled; cannot be set when both are disabled.
    * `use_filter_ngram` - [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `use_filter_edge_ngram` - edge [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `filter_ngram_min_length` - minimum n-gram length (positive integer)
    * `filter_ngram_max_length` - maximum n-gram length (positive integer)
