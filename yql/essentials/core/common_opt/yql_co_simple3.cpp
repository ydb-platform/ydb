#include "yql_co.h"
#include "yql_co_pgselect.h"

namespace NYql {

void RegisterCoSimpleCallables3(TCallableOptimizerMap& map) {
    map["PgGroupRef"] = ExpandPgGroupRef;
}

}
