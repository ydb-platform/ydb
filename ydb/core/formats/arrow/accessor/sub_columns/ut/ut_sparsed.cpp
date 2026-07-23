#include <ydb/core/formats/arrow/accessor/sub_columns/direct_builder.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/types.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/chunked_array.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

Y_UNIT_TEST_SUITE(SubColumnsSparsedBuilder) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    NBinaryJson::TBinaryJson ToBinaryJson(const TString& json) {
        auto v = NBinaryJson::SerializeToBinaryJson(json);
        auto* bj = std::get_if<NBinaryJson::TBinaryJson>(&v);
        UNIT_ASSERT(bj);
        return *bj;
    }

    std::shared_ptr<IChunkedArray> BuildColumn(
        const std::vector<std::pair<ui32, TString>>& data, const ui32 recordsCount, const EValueType vt, const bool sparsed) {
        TColumnElements col("k");
        for (auto&& [idx, json] : data) {
            col.AddData(ToBinaryJson(json), idx);
        }
        if (sparsed) {
            col.BuildSparsedAccessor(recordsCount, vt);
        } else {
            col.BuildPlainAccessor(recordsCount, vt);
        }
        return col.GetAccessorVerified();
    }

    void CheckSparsedMatchesPlain(
        const std::vector<std::pair<ui32, TString>>& data, const ui32 recordsCount, const EValueType vt) {
        auto sparsed = BuildColumn(data, recordsCount, vt, true);
        auto plain = BuildColumn(data, recordsCount, vt, false);
        UNIT_ASSERT_VALUES_EQUAL((ui32)sparsed->GetType(), (ui32)IChunkedArray::EType::SparsedArray);
        UNIT_ASSERT_VALUES_EQUAL((ui32)plain->GetType(), (ui32)IChunkedArray::EType::Array);
        UNIT_ASSERT_VALUES_EQUAL(sparsed->GetRecordsCount(), recordsCount);
        UNIT_ASSERT_VALUES_EQUAL(plain->GetRecordsCount(), recordsCount);
        auto sc = sparsed->GetChunkedArray();
        auto pc = plain->GetChunkedArray();
        UNIT_ASSERT_C(sc->Equals(pc), TStringBuilder() << "sparsed=" << sc->ToString() << " plain=" << pc->ToString());
    }

    Y_UNIT_TEST(StringMatchesPlain) {
        CheckSparsedMatchesPlain({ { 1, R"("x")" }, { 4, R"("yy")" }, { 7, R"("zzz")" } }, 10, EValueType::String);
    }

    Y_UNIT_TEST(DoubleMatchesPlain) {
        CheckSparsedMatchesPlain({ { 0, "3.5" }, { 2, "-2" }, { 9, "1000000" } }, 10, EValueType::Double);
    }

    Y_UNIT_TEST(BoolMatchesPlain) {
        CheckSparsedMatchesPlain({ { 3, "true" }, { 5, "false" } }, 8, EValueType::Bool);
    }

    Y_UNIT_TEST(BinaryJsonMatchesPlain) {
        CheckSparsedMatchesPlain({ { 1, R"([1,2])" }, { 2, R"({"a":1})" } }, 5, EValueType::BinaryJson);
    }
}
