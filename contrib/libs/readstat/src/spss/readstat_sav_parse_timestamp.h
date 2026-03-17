
readstat_error_t sav_parse_time(const char *data, size_t len, struct tm *timestamp,
        readstat_error_handler error_cb, void *user_ctx);
readstat_error_t sav_parse_date(const char *data, size_t len, struct tm *timestamp,
        readstat_error_handler error_cb, void *user_ctx);
