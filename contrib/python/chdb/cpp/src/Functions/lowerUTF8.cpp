#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>
#include <Functions/FunctionFactory.h>
#include <CHDBPoco/Unicode.h>


namespace DB_CHDB
{
namespace
{

struct NameLowerUTF8
{
    static constexpr auto name = "lowerUTF8";
};

using FunctionLowerUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'A', 'Z', CHDBPoco::Unicode::toLower, UTF8CyrillicToCase<true>>, NameLowerUTF8>;

}

REGISTER_FUNCTION(LowerUTF8)
{
    factory.registerFunction<FunctionLowerUTF8>();
}

}
