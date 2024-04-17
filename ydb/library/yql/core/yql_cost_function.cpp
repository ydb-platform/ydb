
#include "yql_cost_function.h"

namespace NYql {

bool NDq::operator < (const NDq::TJoinColumn& c1, const NDq::TJoinColumn& c2) {
    if (c1.RelName < c2.RelName){
        return true;
    } else if (c1.RelName == c2.RelName) {
        return c1.AttributeName < c2.AttributeName;
    }
    return false;
}

}  // namespace NYql
