#include "yql_clickhouse_util.h"

#include <util/string/builder.h>

namespace NYql {

TString EscapeChString(TStringBuf in, bool ident) {
    TStringBuilder ret;
    ret << (ident ? "`" : "'");
    for (char c : in) {
        if (c == '\\') {
            ret << "\\\\";
        } else if (c == (ident ? '`' : '\'')) {
            ret << "\\" << c;
        } else {
            ret << c;
        }
    }

    ret << (ident ? "`" : "'");
    return ret;
}

} // namespace NYql
