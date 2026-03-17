#include <Functions/FunctionFactory.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/URL/queryString.h>

namespace DB_CHDB
{

struct NameCutQueryString { static constexpr auto name = "cutQueryString"; };
using FunctionCutQueryString = FunctionStringToString<CutSubstringImpl<ExtractQueryString<false>>, NameCutQueryString>;

REGISTER_FUNCTION(CutQueryString)
{
    factory.registerFunction<FunctionCutQueryString>();
}

}
