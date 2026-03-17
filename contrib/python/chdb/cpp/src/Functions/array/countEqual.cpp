#include "arrayIndex.h"
#include <Functions/FunctionFactory.h>

namespace DB_CHDB
{
struct NameCountEqual { static constexpr auto name = "countEqual"; };

using FunctionCountEqual = FunctionArrayIndex<CountEqualAction, NameCountEqual>;

REGISTER_FUNCTION(CountEqual)
{
    factory.registerFunction<FunctionCountEqual>();
}
}
