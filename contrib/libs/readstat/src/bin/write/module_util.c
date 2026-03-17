#include <string.h>

int rs_ends_with(const char *filename, const char *ending) {
    size_t filename_len = strlen(filename);
    size_t ending_len = strlen(ending);
    return filename_len > ending_len && strncmp(filename + filename_len - ending_len, ending, ending_len) == 0;
}
