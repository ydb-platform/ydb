
#include "yql_cost_function.h"

namespace NYql {

namespace {

THashMap<TString,EJoinAlgoType> JoinAlgoMap = {
    {"Undefined",EJoinAlgoType::Undefined},
    {"LookupJoin",EJoinAlgoType::LookupJoin},
    {"MapJoin",EJoinAlgoType::MapJoin},
    {"GraceJoin",EJoinAlgoType::GraceJoin},
    {"StreamLookupJoin",EJoinAlgoType::StreamLookupJoin}};

}  // namespace

bool NDq::operator < (const NDq::TJoinColumn& c1, const NDq::TJoinColumn& c2) {
    if (c1.RelName < c2.RelName){
        return true;
    } else if (c1.RelName == c2.RelName) {
        return c1.AttributeName < c2.AttributeName;
    }
    return false;
}

TString ConvertToJoinAlgoString(EJoinAlgoType joinAlgo) {
    for (const auto& [k,v] : JoinAlgoMap) {
        if (v == joinAlgo) {
            return k;
        }
    }
    Y_ENSURE(false, "Unknown join algo");
}

}  // namespace NYql
