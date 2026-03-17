
#include <errno.h>
#include "readstat.h"
#include "readstat_iconv.h"
#include "readstat_convert.h"

readstat_error_t readstat_convert(char *dst, size_t dst_len, const char *src, size_t src_len, iconv_t converter) {
    /* strip off spaces from the input because the programs use ASCII space
     * padding even with non-ASCII encoding. */
    while (src_len && (src[src_len-1] == ' ' || src[src_len-1] == '\0')) {
        src_len--;
    }
    if (dst_len == 0) {
        return READSTAT_ERROR_CONVERT_LONG_STRING;
    } else if (converter) {
        size_t dst_left = dst_len - 1;
        char *dst_end = dst;
        size_t status = iconv(converter, (readstat_iconv_inbuf_t)&src, &src_len, &dst_end, &dst_left);
        if (status == (size_t)-1) {
            if (errno == E2BIG) {
                return READSTAT_ERROR_CONVERT_LONG_STRING;
            } else if (errno == EILSEQ) {
                return READSTAT_ERROR_CONVERT_BAD_STRING;
            } else if (errno != EINVAL) { /* EINVAL indicates improper truncation; accept it */
                return READSTAT_ERROR_CONVERT;
            }
        }
        dst[dst_len - dst_left - 1] = '\0';
    } else if (src_len + 1 > dst_len) {
        return READSTAT_ERROR_CONVERT_LONG_STRING;
    } else {
        memcpy(dst, src, src_len);
        dst[src_len] = '\0';
    }
    return READSTAT_OK;
}
