#include <ydb/library/yql/parser/pg_wrapper/pg_compat.h>

extern "C" {
#include <utils/builtins.h>
}

#include "utils.h"

namespace NYql {
    ui64 hex_encode(const char *src, size_t len, char *dst) {
        return ::hex_encode(src, len, dst);
    }
}

