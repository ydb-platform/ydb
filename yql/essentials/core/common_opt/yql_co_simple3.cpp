#include "yql_co.h"
#include "yql_co_sqlselect.h"

namespace NYql {

void RegisterCoSimpleCallables3(TCallableOptimizerMap& map) {
    map["PgGroupRef"] = ExpandSqlGroupRef;
    map["YqlGroupRef"] = ExpandSqlGroupRef;
}

}
