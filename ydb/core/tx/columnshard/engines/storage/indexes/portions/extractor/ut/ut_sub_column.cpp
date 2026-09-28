#include <ydb/core/formats/arrow/accessor/sub_columns/ut_common/ut_helpers.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/portions/extractor/sub_column.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NIndexes {

Y_UNIT_TEST_SUITE(TSubColumnDataExtractorTests) {
    Y_UNIT_TEST(UsesExactStoredPath) {
        TSubColumnDataExtractor extractor;
        NJson::TJsonValue config(NJson::JSON_MAP);
        config.InsertValue("sub_column_name", R"("a"."b")");
        UNIT_ASSERT(extractor.DeserializeFromJson(config).IsSuccess());

        TString result;
        extractor.VisitAll(NArrow::NAccessor::NSubColumns::NTesting::BuildArrayWithStoredPaths(
                               { { R"("a")", R"("columns")" }, { R"("a"."b"."c")", R"("descendant")" } }, R"("a"."b")", R"("others")"),
            {}, [&result](const NArrow::NAccessor::TJsonValueView& value, ui64) {
                result = value.ToJsonValue().GetString();
            });
        UNIT_ASSERT_VALUES_EQUAL(result, "others");
    }
}

}   // namespace NKikimr::NOlap::NIndexes
