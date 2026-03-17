#include <stdlib.h>

#define MAX_MALLOC_SIZE 0x1000000
/* =16 MiB. Needs to be at least 0x3FF00, i.e. the default ~4MB block size used
 * in compressed SPSS (ZSAV) files. Some SAS installations use 16MiB page sizes
 * by default, see https://github.com/tidyverse/haven/issues/697.
 * The purpose here is to prevent massive allocations in the event of a
 * malformed file or a bug in the library. */

void *readstat_malloc(size_t len) {
    if (len > MAX_MALLOC_SIZE || len == 0) {
        return NULL;
    }
    return malloc(len);
}

void *readstat_calloc(size_t count, size_t size) {
    if (count > MAX_MALLOC_SIZE || size > MAX_MALLOC_SIZE || count * size > MAX_MALLOC_SIZE) {
        return NULL;
    }
    if (count == 0 || size == 0) {
        return NULL;
    }
    return calloc(count, size);
}

void *readstat_realloc(void *ptr, size_t len) {
    if (len > MAX_MALLOC_SIZE || len == 0) {
        if (ptr)
            free(ptr);
        return NULL;
    }
    return realloc(ptr, len);
}
