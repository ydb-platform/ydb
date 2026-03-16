
typedef struct csv_metadata {
    int pass;
    long rows;
    long columns;
    long _columns;
    long _rows;
    int output_format;
    size_t* column_width;
    int open_row;
    readstat_callbacks_t handle;
    void *user_ctx;
    readstat_variable_t *variables;
    int* is_date;
    struct json_metadata *json_md;
    rs_read_module_t *output_module;
} csv_metadata;
