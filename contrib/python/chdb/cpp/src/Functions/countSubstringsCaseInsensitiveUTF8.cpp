#include "FunctionsStringSearch.h"
#include "FunctionFactory.h"
#include "CountSubstringsImpl.h"


namespace DB_CHDB
{
namespace
{

struct NameCountSubstringsCaseInsensitiveUTF8
{
    static constexpr auto name = "countSubstringsCaseInsensitiveUTF8";
};

using FunctionCountSubstringsCaseInsensitiveUTF8 = FunctionsStringSearch<CountSubstringsImpl<NameCountSubstringsCaseInsensitiveUTF8, PositionCaseInsensitiveUTF8>>;

}

REGISTER_FUNCTION(CountSubstringsCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionCountSubstringsCaseInsensitiveUTF8>();
}
}
