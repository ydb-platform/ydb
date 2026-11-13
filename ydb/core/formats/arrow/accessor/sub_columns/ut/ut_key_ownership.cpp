#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/direct_builder.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/ut_common/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/binary_json/write.h>

#include <util/string/builder.h>

#include <string_view>
#include <vector>

using NKikimr::NArrow::NAccessor::NSubColumns::NTesting::PrintBinaryJsons;

Y_UNIT_TEST_SUITE(SubColumnsKeyOwnership) {
    using namespace NKikimr;
    using namespace NKikimr::NArrow::NAccessor;
    using namespace NKikimr::NArrow::NAccessor::NSubColumns;

    std::shared_ptr<IChunkedArray> MakeChunk(const std::vector<TString>& documents) {
        TTrivialArray::TPlainBuilder<arrow::BinaryType> builder;
        for (ui32 i = 0; i < documents.size(); ++i) {
            auto value = NBinaryJson::SerializeToBinaryJson(documents[i]);
            const auto* binaryJson = std::get_if<NBinaryJson::TBinaryJson>(&value);
            UNIT_ASSERT(binaryJson);
            builder.AddRecord(i, std::string_view(binaryJson->data(), binaryJson->size()));
        }
        return builder.Finish(documents.size());
    }

    void CheckReleasedChunks(const std::vector<TString>& documents) {
        const TSettings settings(4, 1024, 0, 0, TDataAdapterContainer::GetDefault());
        const auto actual = [&] {
            TDataBuilder builder(arrow::binary(), settings);
            for (const auto& document : documents) {
                const auto input = MakeChunk({document})->GetChunkedArray();
                for (const auto& chunk : input->chunks()) {
                    const auto status = settings.GetDataExtractor()->AddDataToBuilders(chunk, builder);
                    UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
                }
            }
            return builder.Finish();
        }();

        const auto expected = TSubColumnsArray::Make(MakeChunk(documents), settings, arrow::binary()).DetachResult();
        UNIT_ASSERT_VALUES_EQUAL(actual->GetRecordsCount(), documents.size());
        UNIT_ASSERT_VALUES_EQUAL(PrintBinaryJsons(actual->GetChunkedArray()),
            PrintBinaryJsons(expected->GetChunkedArray()));
    }

    Y_UNIT_TEST(RepeatedNestedKeysAcrossReleasedChunks) {
        CheckReleasedChunks({
            R"({"a":1,"nested":{"leaf":2},"other":{"leaf":3}})",
            R"({"a":4,"nested":{"leaf":5},"other":{"leaf":6}})",
            R"({"a":7,"nested":{"leaf":8},"other":{"leaf":9}})",
        });
    }

    Y_UNIT_TEST(LongKeysAcrossReleasedChunks) {
        const TString key(128, 'k');
        CheckReleasedChunks({
            TStringBuilder() << "{\"" << key << "\":1,\"nested\":{\"" << key << "\":2}}",
            TStringBuilder() << "{\"" << key << "\":3,\"nested\":{\"" << key << "\":4}}",
        });
    }
}
