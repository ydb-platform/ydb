LLVM_BC(
    postgresql/src/backend/access/gin/ginarrayproc.c
    postgresql/src/backend/tsearch/dict_thesaurus.c
    postgresql/src/backend/utils/adt/formatting.c
    postgresql/src/backend/utils/adt/tsquery_rewrite.c
    pg_kernels.3.cpp

    NAME PgFuncs3

    SYMBOLS
    arrow_float4_to_char
    arrow_float8_to_char
    arrow_ginarrayconsistent
    arrow_ginarrayextract
    arrow_ginarrayextract_2args
    arrow_ginarraytriconsistent
    arrow_ginqueryarrayextract
    arrow_int4_to_char
    arrow_int8_to_char
    arrow_interval_to_char
    arrow_numeric_to_char
    arrow_numeric_to_number
    arrow_thesaurus_init
    arrow_thesaurus_lexize
    arrow_timestamp_to_char
    arrow_timestamptz_to_char
    arrow_to_date
    arrow_to_timestamp
    arrow_tsquery_rewrite
    arrow_tsquery_rewrite_query
)
