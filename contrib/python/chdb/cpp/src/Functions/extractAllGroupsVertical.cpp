#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct VerticalImpl
{
    static constexpr auto Kind = DB_CHDB::ExtractAllGroupsResultKind::VERTICAL;
    static constexpr auto Name = "extractAllGroupsVertical";
};

}

namespace DB_CHDB
{

REGISTER_FUNCTION(ExtractAllGroupsVertical)
{
    factory.registerFunction<FunctionExtractAllGroups<VerticalImpl>>();
    factory.registerAlias("extractAllGroups", VerticalImpl::Name);
}

}
