#include <ydb/library/yql/parser/pg_wrapper/interface/interface.h>
#include <ydb/library/yql/parser/pg_wrapper/pg_compat.h>

extern "C" {
#include <utils/builtins.h>
}


namespace NYql {
    ui64 HexEncode(const char *src, size_t len, char *dst) {
        return ::hex_encode(src, len, dst);
    }
}

