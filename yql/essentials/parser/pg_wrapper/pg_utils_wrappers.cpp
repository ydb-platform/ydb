#include <yql/essentials/parser/pg_wrapper/pg_compat.h>
#include <yql/essentials/parser/pg_wrapper/interface/utils.h>
extern "C" {
#include <utils/builtins.h>
}


namespace NYql {
    ui64 HexEncode(const char *src, size_t len, char *dst) {
        return ::hex_encode(src, len, dst);
    }
}

