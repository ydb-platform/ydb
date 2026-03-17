#include <stdlib.h>
#include <string.h>
#include <ctype.h>

void readstat_copy(char *buf, size_t buf_len, const char *str_start, size_t str_len) {
    size_t this_len = str_len;
    if (this_len >= buf_len) {
        this_len = buf_len - 1;
    }
    memcpy(buf, str_start, this_len);
    buf[this_len] = '\0';
}

void readstat_copy_lower(char *buf, size_t buf_len, const char *str_start, size_t str_len) {
    int i;
    readstat_copy(buf, buf_len, str_start, str_len);
    for (i=0; i<buf_len && buf[i]; i++)
        buf[i] = tolower(buf[i]);
}

void readstat_copy_quoted(char *buf, size_t buf_len, const char *str_start, size_t str_len) {
    size_t this_len = str_len;
    if (this_len >= buf_len) {
        this_len = buf_len - 1;
    }
    size_t i=0;
    size_t j=0;
    int slash = 0;
    for (i=0; i<this_len; i++) {
        if (slash) {
            if (str_start[i] == 't') {
                buf[j++] = '\t';
            } else {
                buf[j++] = str_start[i];
            }
            slash = 0;
        } else {
            if (str_start[i] == '\\') {
                slash = 1;
            } else {
                buf[j++] = str_start[i];
            }
        }
    }
    buf[j] = '\0';
}

