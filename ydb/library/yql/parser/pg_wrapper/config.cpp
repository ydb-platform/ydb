#include <util/generic/string.h>
#include <ydb/library/yql/parser/pg_wrapper/postgresql/src/include/pg_config.h>

namespace NYql {

TString GetPostgresServerVersionNum() {
    return Y_STRINGIZE(PG_VERSION_NUM);
}

TString GetPostgresServerVersionStr() {
    return PG_VERSION_STR;
}

} // NYql
