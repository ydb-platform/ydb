#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/constructor.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/chunked_array_serialized.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NArrow;
using namespace NKikimr::NArrow::NAccessor;
using namespace NKikimr::NOlap::NChunks;

namespace {

std::shared_ptr<IChunkedArray> BuildArray() {
    TTrivialArray::TPlainBuilder<arrow::BinaryType> arrBuilder;
    arrBuilder.AddRecord(1, "b1");
    arrBuilder.AddRecord(3, "b2");
    arrBuilder.AddRecord(4, "b3");
    return arrBuilder.Finish(5);
}

}   // namespace

Y_UNIT_TEST_SUITE(ChunkedArraySerialized) {
    Y_UNIT_TEST(SplitBySizes) {
        auto arr = BuildArray();
        TChunkConstructionData info(
            arr->GetRecordsCount(), nullptr, arr->GetDataType(), NSerialization::TSerializerContainer::GetDefaultSerializer());
        const TString serialized = NPlain::TConstructor().SerializeToBlobAndMeta(arr, info).Blob;
        const auto predSaver = [&](const std::shared_ptr<IChunkedArray>& slice) {
            return NPlain::TConstructor().SerializeToBlobAndMeta(slice, info);
        };

        NSerialization::TNativeSerializer serializer;
        auto chunks = SplitBySizes(*arr, predSaver, serialized, { 1, 1, 1 });
        UNIT_ASSERT(!chunks.empty());
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector({ std::make_shared<arrow::Field>("val", arrow::utf8()) }));
        ui32 totalRecords = 0;
        for (const auto& chunk : chunks) {
            totalRecords += chunk.GetArray()->GetRecordsCount();
            // Each slice carries its blob and its (here empty) metadata, produced by one serialize call.
            UNIT_ASSERT(chunk.GetMeta());
            UNIT_ASSERT(!chunk.GetMeta()->SerializeToProto());
            const auto result = serializer.Deserialize(chunk.GetSerializedData(), schema);
            UNIT_ASSERT_C(result.status().ok(), result.status().ToString());
        }
        UNIT_ASSERT_VALUES_EQUAL(totalRecords, arr->GetRecordsCount());
    }
}
