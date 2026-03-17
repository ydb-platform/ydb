typedef void (*rs_produce_column_header)(void *csv_metadata, const char *column, readstat_variable_t* var);
typedef void (*rs_produce_missingness)(void *csv_metadata, const char *column);
typedef void (*rs_produce_value_label)(void *csv_metadata, const char *column);
typedef void (*rs_produce_csv_value)(void *csv_metadata, const char *s, size_t len);

typedef struct rs_read_module_s {
    int                      format;
    rs_produce_column_header header;
    rs_produce_missingness   missingness;
    rs_produce_value_label   value_label;
    rs_produce_csv_value     csv_value;
} rs_read_module_t;
