  * common parameters for all fulltext indexes:
    * `analyzer` - predefined tokenizer and filter chain:
      * `standard` - `standard` tokenizer with lowercase and English stopword filters
      * `snowball` - `standard` tokenizer with lowercase, stopword, and Snowball filters; requires `language` (`english` or `russian`)
      * `keyword` - `keyword` tokenizer without filters
    * `tokenizer` - tokenizer type (`standard`, `whitespace`, or `keyword`)
    * `use_filter_lowercase` - lowercase filter (`true` or `false`)
    * `use_filter_stopwords` - stopword filter (`true` or `false`); English is used when `language` is not specified, and `english` and `russian` are supported
    * `use_filter_length` - token length filter (`true` or `false`); when `true`, tokens shorter than `filter_length_min` or longer than `filter_length_max` are not indexed and are ignored during search
    * `filter_length_min` - minimum token length (positive integer); only applied when `use_filter_length=true`
    * `filter_length_max` - maximum token length (positive integer); only applied when `use_filter_length=true`
    * `use_filter_snowball` - [Snowball](https://snowballstem.org/) stemmer filter (`true` or `false`)
    * `language` - language for the stopword filter or the [Snowball](https://snowballstem.org/) stemmer (`english` or `russian`)
    * `use_filter_ngram` - [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `use_filter_edge_ngram` - edge [n-gram](https://en.wikipedia.org/wiki/N-gram) filter (`true` or `false`)
    * `filter_ngram_min_length` - minimum n-gram length (positive integer)
    * `filter_ngram_max_length` - maximum n-gram length (positive integer)
