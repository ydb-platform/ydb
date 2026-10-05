  * common parameters for all fulltext indexes:
    * `analyzer` - predefined analyzer: `standard` (standard tokenizer, lowercase and English stopword removal), `snowball` (standard tokenizer, lowercase, stopword removal, and Snowball lemmatization; requires `language`), or `keyword` (the whole text is one token, preserving case)
    * `tokenizer` - tokenizer type (`standard`, `whitespace`, `alphanumeric`, or `keyword`)
    * `use_filter_lowercase` - lowercase filter (`true` or `false`)
    * `use_filter_stopwords` - remove common words such as `the` and `and` (`true` or `false`). Supports English and Russian; defaults to English when `language` is omitted
    * `use_filter_length` - token length filter (`true` or `false`); when `true`, tokens shorter than `filter_length_min` or longer than `filter_length_max` are not indexed
    * `filter_length_min` - minimum token length (positive integer); only applied when `use_filter_length=true`
    * `filter_length_max` - maximum token length (positive integer); only applied when `use_filter_length=true`
    * `use_filter_snowball` - [Snowball](https://snowballstem.org/) lemmatization filter (`true` or `false`)
    * `language` - one language or a comma-separated list for the [Snowball](https://snowballstem.org/) lemmatizer. Spaces around commas are allowed, for example `"english, russian"`. The `snowball` analyzer also removes stopwords, so with that preset only `english` and `russian` can be set
    * `use_filter_ngram` - [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `use_filter_edge_ngram` - edge [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `filter_ngram_min_length` - minimum n-gram length (positive integer)
    * `filter_ngram_max_length` - maximum n-gram length (positive integer)
