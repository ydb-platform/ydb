#include <ydb/public/lib/ydb_cli/dump/restore_compat.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

namespace NYdb::NDump {

Y_UNIT_TEST_SUITE(RestoreCompat) {

Y_UNIT_TEST(ParseErrorIncludesLocationAndColumn) {
    auto tableDesc = NTable::TTableBuilder()
        .AddNullableColumn("ColFloat", EPrimitiveType::Float)
        .Build();

    THolder<NPrivate::IDataAccumulator> accumulator(
        CreateCompatAccumulator("table_path", tableDesc, TRestoreSettings{}));

    try {
        accumulator->Feed(NPrivate::TLine(TString("not-a-float"), "data_00.csv", 42));
        UNIT_FAIL("Expected a float parsing error");
    } catch (const std::exception& e) {
        const TString message = e.what();
        UNIT_ASSERT_C(message.find("data_00.csv:42:") != TString::npos, message);
        UNIT_ASSERT_C(message.find("Failed to parse value \"not-a-float\"") != TString::npos, message);
        UNIT_ASSERT_C(message.find("for column \"ColFloat\"") != TString::npos, message);
    }
}

} // Y_UNIT_TEST_SUITE(RestoreCompat)

} // namespace NYdb::NDump
