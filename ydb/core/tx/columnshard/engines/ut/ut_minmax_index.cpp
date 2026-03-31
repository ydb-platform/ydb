#include <ydb/core/tx/columnshard/engines/storage/indexes/minmax/meta.h>
#include <ydb/core/tx/columnshard/splitter/chunks.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NIndexes::NMinMax {

namespace {

class TTestChunk : public IPortionDataChunk {
    TString Data;
    ui32 RecordsCount;

protected:
    const TString& DoGetData() const override {
        return Data;
    }
    TString DoDebugString() const override {
        return "";
    }
    std::vector<std::shared_ptr<IPortionDataChunk>> DoInternalSplit(
        const TColumnSaver&, const std::shared_ptr<NColumnShard::TSplitterCounters>&,
        const std::vector<ui64>&) const override {
        AFL_VERIFY(false);
        return {};
    }
    bool DoIsSplittable() const override {
        return false;
    }
    std::optional<ui32> DoGetRecordsCount() const override {
        return RecordsCount;
    }
    std::optional<ui64> DoGetRawBytes() const override {
        return Data.size();
    }
    std::shared_ptr<arrow::Scalar> DoGetFirstScalar() const override {
        return nullptr;
    }
    std::shared_ptr<arrow::Scalar> DoGetLastScalar() const override {
        return nullptr;
    }
    void DoAddIntoPortionBeforeBlob(const TBlobRangeLink16&, TPortionAccessorConstructor&) const override {
        AFL_VERIFY(false);
    }

public:
    TTestChunk(TString data, ui32 recordsCount, ui32 entityId)
        : IPortionDataChunk(entityId)
        , Data(std::move(data))
        , RecordsCount(recordsCount) {
    }
};

class TTestIndexMeta : public TIndexMeta {
public:
    using TIndexMeta::DoBuildIndexImpl;
};

}   // anonymous namespace

Y_UNIT_TEST_SUITE(TMinMaxIndexTests) {
    Y_UNIT_TEST(CreateMetaFromProto) {
        NKikimrSchemeOp::TOlapIndexDescription proto;
        proto.SetId(1);
        proto.SetName("test_index");
        proto.SetClassName("MINMAX");
        proto.MutableMinMaxIndex()->SetColumnId(42);

        TIndexMeta meta;
        UNIT_ASSERT(meta.DeserializeFromProto(proto));
    }

    Y_UNIT_TEST(BuildIndex) {
        const ui32 columnId = 42;

        NKikimrSchemeOp::TOlapIndexDescription proto;
        proto.SetId(1);
        proto.SetName("test_index");
        proto.SetClassName("MINMAX");
        proto.MutableMinMaxIndex()->SetColumnId(columnId);

        TTestIndexMeta meta;
        UNIT_ASSERT(meta.DeserializeFromProto(proto));

        // build int32 array [1, 2, 3]
        arrow::Int32Builder builder;
        AFL_VERIFY(builder.AppendValues({1, 2, 3}).ok());
        std::shared_ptr<arrow::Array> array;
        AFL_VERIFY(builder.Finish(&array).ok());

        auto field = std::make_shared<arrow::Field>("val", arrow::int32());
        auto schema = std::make_shared<arrow::Schema>(arrow::FieldVector{field});
        auto rb = arrow::RecordBatch::Make(schema, 3, arrow::ArrayVector{array});

        auto serializer = NArrow::NSerialization::TSerializerContainer::GetDefaultSerializer();
        TString serializedData = serializer->SerializePayload(rb);

        auto chunk = std::make_shared<TTestChunk>(serializedData, 3, columnId);

        auto loader = std::make_shared<NArrow::NAccessor::TColumnLoader>(
            NArrow::NSerialization::TSerializerContainer(serializer),
            NArrow::NAccessor::TConstructorContainer::GetDefaultConstructor(),
            field,
            nullptr,
            columnId);

        TChunkedColumnReader columnReader({chunk}, loader);
        TChunkedBatchReader reader({columnReader});

        auto result = meta.DoBuildIndexImpl(reader, 3);
        UNIT_ASSERT_EQUAL(result.size(), 1);
        UNIT_ASSERT_EQUAL(result[0]->GetRecordsCountVerified(), 3);
    }
}

}   // namespace NKikimr::NOlap::NIndexes::NMinMax
