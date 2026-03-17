#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct HorizontalImpl
{
    static constexpr auto Kind = DB_CHDB::ExtractAllGroupsResultKind::HORIZONTAL;
    static constexpr auto Name = "extractAllGroupsHorizontal";
};

}

namespace DB_CHDB
{

REGISTER_FUNCTION(ExtractAllGroupsHorizontal)
{
    factory.registerFunction<FunctionExtractAllGroups<HorizontalImpl>>();
}

}
