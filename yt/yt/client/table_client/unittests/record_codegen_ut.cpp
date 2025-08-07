#include <yt/yt/client/table_client/record_codegen_cpp.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TRecordCodegenTypeV3Test
    : public ::testing::TestWithParam<std::pair<TString, TLogicalTypePtr>>
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_P(TRecordCodegenTypeV3Test, Parse)
{
    auto [rawTypeV3, expectedLogicalType] = GetParam();

    auto logicalType = NDetail::FromRecordCodegenTypeV3(rawTypeV3);
    EXPECT_EQ(*logicalType, *expectedLogicalType);
}

////////////////////////////////////////////////////////////////////////////////

INSTANTIATE_TEST_SUITE_P(
    TRecordCodegenTypeV3Test,
    TRecordCodegenTypeV3Test,
    ::testing::Values(
        std::pair{
            TString(R"({"type_name": "optional", "item": {"type_name": "list", "item": "string"}})"),
            ConvertTo<TLogicalTypePtr>(TYsonString(TString("{type_name=optional;item={type_name=list;item=string}}")))
        },
        std::pair{
            TString(R"({"type_name": "int64"})"),
            ConvertTo<TLogicalTypePtr>(TYsonString(TString("{type_name=\"int64\"}")))
        }));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
