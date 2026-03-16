#include <Functions/FunctionStringToString.h>
#include <Functions/LowerUpperUTF8Impl.h>
#include <Functions/FunctionFactory.h>
#include <CHDBPoco/Unicode.h>


namespace DB_CHDB
{
namespace
{

struct NameUpperUTF8
{
    static constexpr auto name = "upperUTF8";
};

using FunctionUpperUTF8 = FunctionStringToString<LowerUpperUTF8Impl<'a', 'z', CHDBPoco::Unicode::toUpper, UTF8CyrillicToCase<false>>, NameUpperUTF8>;

}

REGISTER_FUNCTION(UpperUTF8)
{
    factory.registerFunction<FunctionUpperUTF8>();
}

}
