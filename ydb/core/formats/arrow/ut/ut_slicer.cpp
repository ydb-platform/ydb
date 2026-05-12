#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/join.h>


static std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray> BuildArray() {
    NKikimr::NArrow::NAccessor::TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
    arrBuilder.AddRecord(1, "b1");
    arrBuilder.AddRecord(3, "b2");
    arrBuilder.AddRecord(4, "b3");
    return arrBuilder.Finish(5);
}

Y_UNIT_TEST_SUITE(Slicer) {
    using namespace NKikimr::NArrow;

    Y_UNIT_TEST(SplitBySizes) {
        auto arr = BuildArray();
        NKikimr::NArrow::NAccessor::TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NKikimr::NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer());
        auto serialized = NKikimr::NArrow::NAccessor::NPlain::TConstructor().SerializeToString(arr, info);
        const auto predSaver = [&](const std::shared_ptr<NKikimr::NArrow::NAccessor::IChunkedArray>& arr) {
            return NKikimr::NArrow::NAccessor::NPlain::TConstructor().SerializeToString(arr, info);
        };
        
        NKikimr::NArrow::NSerialization::TNativeSerializer serializer;
        for (const auto& chunk: arr->SplitBySizes(predSaver, serialized, {1, 1, 1})) {
            auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", arrow::utf8()) }));
            TString serializedData = chunk.GetSerializedData();
            arrow::Result<std::shared_ptr<arrow::RecordBatch>> result = serializer.Deserialize(serializedData, schema);
            UNIT_ASSERT_C(result.status().ok(), result.status().ToString());
        }
    }
}
