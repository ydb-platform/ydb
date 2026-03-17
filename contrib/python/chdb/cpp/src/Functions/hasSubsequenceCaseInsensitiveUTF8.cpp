#include <Functions/FunctionFactory.h>
#include <Functions/HasSubsequenceImpl.h>

#include "CHDBPoco/Unicode.h"

namespace DB_CHDB
{
namespace
{

struct HasSubsequenceCaseInsensitiveUTF8
{
    static constexpr bool is_utf8 = true;

    static int toLowerIfNeed(int code_point) { return CHDBPoco::Unicode::toLower(code_point); }
};

struct NameHasSubsequenceCaseInsensitiveUTF8
{
    static constexpr auto name = "hasSubsequenceCaseInsensitiveUTF8";
};

using FunctionHasSubsequenceCaseInsensitiveUTF8 = HasSubsequenceImpl<NameHasSubsequenceCaseInsensitiveUTF8, HasSubsequenceCaseInsensitiveUTF8>;
}

REGISTER_FUNCTION(hasSubsequenceCaseInsensitiveUTF8)
{
    factory.registerFunction<FunctionHasSubsequenceCaseInsensitiveUTF8>({}, FunctionFactory::Case::Insensitive);
}

}
