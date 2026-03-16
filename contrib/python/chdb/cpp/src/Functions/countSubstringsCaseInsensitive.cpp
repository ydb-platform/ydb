#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB_CHDB
{
namespace
{

struct NameCountSubstringsCaseInsensitive
{
    static constexpr auto name = "countSubstringsCaseInsensitive";
};

using FunctionCountSubstringsCaseInsensitive = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitive, PositionCaseInsensitiveASCII>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitive)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitive>();
}
}
