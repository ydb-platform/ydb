#if defined(CHDB_VERSION_STRING)

#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionConstantBase.h>
#include <Common/FunctionDocumentation.h>

namespace DB_CHDB
{

namespace
{
    /// chdb() - returns the current chdb version as a string.
    class FunctionChdbVersion : public FunctionConstantBase<FunctionChdbVersion, String, DataTypeString>
    {
    public:
        static constexpr auto name = "chdb";
        static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionChdbVersion>(context); }
        explicit FunctionChdbVersion(ContextPtr context) : FunctionConstantBase(CHDB_VERSION_STRING, context->isDistributed()) {}
    };
}
    
REGISTER_FUNCTION(ChdbVersion)
{
    factory.registerFunction<FunctionChdbVersion>(FunctionDocumentation
        {
          .description=R"(
Returns the version of chDB.  The result type is String.
)",
          .examples{{"chdb", "SELECT chdb();", ""}},
          .categories{"String"}
        },
        FunctionFactory::Case::Insensitive);
}
}
#endif
