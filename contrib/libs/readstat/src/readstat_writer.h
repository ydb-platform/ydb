
#define READSTAT_PRODUCT_NAME       "ReadStat"
#define READSTAT_PRODUCT_URL        "https://github.com/WizardMac/ReadStat"

readstat_error_t readstat_begin_writing_file(readstat_writer_t *writer, void *user_ctx, long row_count);

readstat_error_t readstat_write_bytes(readstat_writer_t *writer, const void *bytes, size_t len);
readstat_error_t readstat_write_bytes_as_lines(readstat_writer_t *writer,
        const void *bytes, size_t len, size_t line_len, const char *line_sep);
readstat_error_t readstat_write_line_padding(readstat_writer_t *writer, char pad,
        size_t line_len, const char *line_sep);

readstat_error_t readstat_write_zeros(readstat_writer_t *writer, size_t len);
readstat_error_t readstat_write_spaces(readstat_writer_t *writer, size_t len);
readstat_error_t readstat_write_string(readstat_writer_t *writer, const char *bytes);
readstat_error_t readstat_write_space_padded_string(readstat_writer_t *writer, const char *string, size_t max_len);
readstat_value_label_t *readstat_get_value_label(readstat_label_set_t *label_set, int index);
readstat_label_set_t *readstat_get_label_set(readstat_writer_t *writer, int index);
readstat_variable_t *readstat_get_label_set_variable(readstat_label_set_t *label_set, int index);
void readstat_sort_label_set(readstat_label_set_t *label_set,
        int (*compare)(const readstat_value_label_t *, const readstat_value_label_t *));
