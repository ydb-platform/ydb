#include "batch_slice.h"

#include <ydb/core/formats/arrow/accessor/abstract/constructor.h>
#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/save_load/saver.h>
#include <ydb/core/formats/arrow/serializer/native.h>
#include <ydb/core/formats/arrow/simple_builder/array.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/splitter/scheme_info.h>
#include <ydb/core/formats/arrow/splitter/similar_packer.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/splitter/batch_slice.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>
#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(Splitter) {
    using namespace NKikimr::NArrow;

    class TTestSnapshotSchema: public NKikimr::NArrow::NSplitter::ISchemaDetailInfo {
    private:
        mutable std::map<std::string, ui32> Decoder;

    protected:
        virtual NKikimr::NArrow::NAccessor::TColumnSaver DoGetColumnSaver(const ui32 columnId) const override {
            return NKikimr::NArrow::NAccessor::TColumnSaver(
                nullptr, std::make_shared<NSerialization::TNativeSerializer>(arrow::ipc::IpcOptions::Defaults()));
        }

    public:
        virtual bool NeedMinMaxForColumn(const ui32 /*columnId*/) const override {
            return true;
        }
        virtual bool IsSortedColumn(const ui32 /*columnId*/) const override {
            return false;
        }

        virtual std::optional<NKikimr::NArrow::NSplitter::TColumnSerializationStat> GetColumnSerializationStats(
            const ui32 /*columnId*/) const override {
            return {};
        }
        virtual std::optional<NKikimr::NArrow::NSplitter::TBatchSerializationStat> GetBatchSerializationStats(
            const std::shared_ptr<arrow::RecordBatch>& /*rb*/) const override {
            return {};
        }

        NKikimr::NArrow::NAccessor::TColumnLoader GetColumnLoader(const ui32 columnId) const {
            return NKikimr::NArrow::NAccessor::TColumnLoader(nullptr, NSerialization::TSerializerContainer::GetDefaultSerializer(),
                NKikimr::NArrow::NAccessor::TConstructorContainer::GetDefaultConstructor(), GetField(columnId), nullptr, columnId);
        }

        virtual std::shared_ptr<arrow::Field> GetField(const ui32 columnId) const override {
            return std::make_shared<arrow::Field>(GetColumnName(columnId), std::make_shared<arrow::StringType>());
        }

        virtual ui32 GetColumnId(const std::string& columnName) const override {
            auto it = Decoder.find(columnName);
            if (it == Decoder.end()) {
                it = Decoder.emplace(columnName, Decoder.size() + 1).first;
            }
            return it->second;
        }

        std::string GetColumnName(const ui32 columnId) const {
            for (auto&& i : Decoder) {
                if (i.second == columnId) {
                    return i.first;
                }
            }
            Y_ABORT("cannot find column by id");
            return "";
        }
    };

    class TSplitTester {
    private:
        std::shared_ptr<TTestSnapshotSchema> Schema = std::make_shared<TTestSnapshotSchema>();
        YDB_ACCESSOR_DEF(std::optional<ui32>, ExpectSlicesCount);
        YDB_ACCESSOR_DEF(std::optional<ui32>, ExpectBlobsCount);
        YDB_ACCESSOR(std::optional<ui32>, ExpectPortionsCount, 1);
        YDB_ACCESSOR_DEF(std::optional<ui32>, ExpectChunksCount);
        YDB_ACCESSOR(std::optional<ui32>, ExpectedInternalSplitsCount, 0);

    public:
        void Execute(std::shared_ptr<arrow::RecordBatch> batch,
            const NKikimr::NOlap::NSplitter::TSplitSettings& settings = NKikimr::NOlap::NSplitter::TSplitSettings()) {
            using namespace NKikimr::NOlap;
            NKikimr::NColumnShard::TIndexationCounters counters("test");
            std::vector<TGeneralSerializedSlice> generalSlices;
            {
                auto slices = TBatchSerializedSlice::BuildSimpleSlices(batch, settings, counters.SplitterCounters, Schema);
                for (auto&& i : slices) {
                    generalSlices.emplace_back(i);
                }
            }

            NKikimr::NArrow::NSplitter::TSimilarPacker packer(settings.GetExpectedPortionSize());
            auto packs = packer.Split(generalSlices);
            const NKikimr::NOlap::NSplitter::TEntityGroups groups(settings, "default");
            const ui32 portionsCount = packs.size();
            ui32 blobsCount = 0;
            ui32 chunksCount = 0;
            ui32 pagesSum = 0;
            ui32 portionShift = 0;
            ui32 internalSplitsCount = 0;
            for (auto&& i : packs) {
                ui32 portionRecordsCount = 0;
                for (auto&& rc : i) {
                    portionRecordsCount += rc.GetRecordsCount();
                }
                const ui32 pagesOriginal = i.size();
                pagesSum += pagesOriginal;
                TGeneralSerializedSlice slice(std::move(i));
                auto blobsLocal = slice.GroupChunksByBlobs(groups);
                internalSplitsCount += slice.GetInternalSplitsCount();
                blobsCount += blobsLocal.size();
                THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> entityChunks;
                ui32 portionSize = 0;
                for (auto&& b : blobsLocal) {
                    chunksCount += b.GetChunks().size();
                    ui64 bSize = 0;
                    for (auto&& c : b.GetChunks()) {
                        bSize += c->GetData().size();
                        AFL_VERIFY(c->GetEntityId());
                        auto& v = entityChunks[c->GetEntityId()];
                        if (v.size()) {
                            AFL_VERIFY(v.back()->GetChunkIdxVerified() + 1 == c->GetChunkIdxVerified());
                        }
                        entityChunks[c->GetEntityId()].emplace_back(c);
                    }
                    portionSize += bSize;
                    AFL_VERIFY(bSize < (ui64)settings.GetMaxBlobSize());
                    AFL_VERIFY(bSize * 1.01 > (ui64)settings.GetMinBlobSize() || (packs.size() == 1 && blobsLocal.size() == 1))(
                                                                                                           "blob_size", bSize);
                }
                AFL_VERIFY(portionSize >= settings.GetExpectedPortionSize() || packs.size() == 1)("size", portionSize)(
                                                                                   "limit", settings.GetMaxPortionSize());

                THashMap<ui32, std::set<ui32>> entitiesByRecordsCount;
                ui32 pagesRestore = 0;
                for (auto&& e : entityChunks) {
                    const std::shared_ptr<arrow::Array> arr = batch->GetColumnByName(Schema->GetColumnName(e.first));
                    AFL_VERIFY(arr);
                    ui32 count = 0;
                    for (auto&& c : e.second) {
                        auto slice = arr->Slice(count + portionShift, c->GetRecordsCountVerified());
                        auto readBatch = Schema->GetColumnLoader(e.first).ApplyRawVerified(c->GetData());
                        AFL_VERIFY(slice->length() == readBatch->num_rows());
                        Y_ABORT_UNLESS(readBatch->column(0)->RangeEquals(*slice, 0, readBatch->num_rows(), 0, arrow::EqualOptions::Defaults()));
                        count += c->GetRecordsCountVerified();
                        AFL_VERIFY(entitiesByRecordsCount[count].emplace(e.first).second);
                        AFL_VERIFY(entitiesByRecordsCount[count].size() <= (ui32)batch->num_columns());
                        if (entitiesByRecordsCount[count].size() == (ui32)batch->num_columns()) {
                            ++pagesRestore;
                        }
                    }
                    AFL_VERIFY(count == portionRecordsCount);
                }
                AFL_VERIFY(entitiesByRecordsCount.size() >= i.size());
                AFL_VERIFY(pagesRestore == pagesOriginal || batch->num_columns() == 1)("restore", pagesRestore)("original", pagesOriginal);
                for (auto&& c : entityChunks.begin()->second) {
                    portionShift += c->GetRecordsCountVerified();
                }
            }
            AFL_VERIFY(portionShift = batch->num_rows());
            AFL_VERIFY(pagesSum == generalSlices.size())("sum", pagesSum)("general_slices", generalSlices.size());
            AFL_VERIFY(internalSplitsCount == ExpectedInternalSplitsCount.value_or(internalSplitsCount))(
                                                  "expected", *ExpectedInternalSplitsCount)("real", internalSplitsCount);
            AFL_VERIFY(blobsCount == ExpectBlobsCount.value_or(blobsCount))("blobs_count", blobsCount)("expected", *ExpectBlobsCount);
            AFL_VERIFY(pagesSum == ExpectSlicesCount.value_or(pagesSum))("sum", pagesSum)("expected", *ExpectSlicesCount);
            AFL_VERIFY(portionsCount == ExpectPortionsCount.value_or(portionsCount))("portions_count", portionsCount)(
                                            "expected", *ExpectPortionsCount);
            AFL_VERIFY(chunksCount == ExpectChunksCount.value_or(chunksCount))("chunks_count", chunksCount)("expected", *ExpectChunksCount);
        }
    };

    Y_UNIT_TEST(Simple) {
        NConstruction::IArrayBuilder::TPtr column =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ column }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(8).SetExpectSlicesCount(8).Execute(batch);
    }

    Y_UNIT_TEST(Small) {
        NConstruction::IArrayBuilder::TPtr column =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 24));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ column }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(8).Execute(batch);
    }

    Y_UNIT_TEST(Minimal) {
        NConstruction::IArrayBuilder::TPtr column =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ column }).BuildBatch(4048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(1).Execute(batch);
    }

    Y_UNIT_TEST(Trivial) {
        NConstruction::IArrayBuilder::TPtr column =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ column }).BuildBatch(10048);

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(1).Execute(batch);
    }

    Y_UNIT_TEST(BigAndSmall) {
        NConstruction::IArrayBuilder::TPtr columnBig =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        NConstruction::IArrayBuilder::TPtr columnSmall =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field2", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 1));
        std::shared_ptr<arrow::RecordBatch> batch =
            NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ columnBig, columnSmall }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(8).SetExpectSlicesCount(8).Execute(batch);
    }

    Y_UNIT_TEST(CritSmallPortions) {
        NConstruction::IArrayBuilder::TPtr columnBig =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 7120));
        NConstruction::IArrayBuilder::TPtr columnSmall =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field2", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 128));
        std::shared_ptr<arrow::RecordBatch> batch =
            NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ columnBig, columnSmall }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(80).SetExpectSlicesCount(80).SetExpectedInternalSplitsCount(0).SetExpectPortionsCount(40).Execute(
            batch, NKikimr::NOlap::NSplitter::TSplitSettings().SetMinRecordsCount(1000).SetMaxPortionSize(8000000));
    }

    Y_UNIT_TEST(Crit) {
        NConstruction::IArrayBuilder::TPtr columnBig =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 7120));
        NConstruction::IArrayBuilder::TPtr columnSmall =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field2", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 128));
        std::shared_ptr<arrow::RecordBatch> batch =
            NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ columnBig, columnSmall }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(80).SetExpectSlicesCount(8).SetExpectedInternalSplitsCount(8).SetExpectPortionsCount(8).Execute(
            batch);
    }

    Y_UNIT_TEST(CritSimple) {
        NConstruction::IArrayBuilder::TPtr columnBig =
            std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
                "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 7120));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({ columnBig }).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(72).SetExpectSlicesCount(8).SetExpectedInternalSplitsCount(0).SetExpectPortionsCount(8).Execute(
            batch);
    }
};
