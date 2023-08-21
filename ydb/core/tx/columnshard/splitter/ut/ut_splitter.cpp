#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/columnshard/splitter/rb_splitter.h>
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/formats/arrow/serializer/batch_only.h>
#include <ydb/core/formats/arrow/simple_builder/batch.h>
#include <ydb/core/formats/arrow/simple_builder/filler.h>
#include <ydb/core/formats/arrow/serializer/full.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

Y_UNIT_TEST_SUITE(Splitter) {

    using namespace NKikimr::NArrow;

    class TTestSnapshotSchema: public NKikimr::NOlap::ISchemaDetailInfo {
    private:
        mutable std::map<std::string, ui32> Decoder;
    public:
        virtual NKikimr::NOlap::TColumnSaver GetColumnSaver(const ui32 columnId) const override {
            return NKikimr::NOlap::TColumnSaver(nullptr, std::make_shared<NKikimr::NArrow::NSerialization::TFullDataSerializer>(arrow::ipc::IpcWriteOptions::Defaults()));
        }

        virtual std::optional<NKikimr::NOlap::TColumnSerializationStat> GetColumnSerializationStats(const ui32 /*columnId*/) const override {
            return {};
        }
        virtual std::optional<NKikimr::NOlap::TColumnSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& /*rb*/) const override {
            return {};
        }

        NKikimr::NOlap::TColumnLoader GetColumnLoader(const ui32 columnId) const {
            arrow::FieldVector v = {std::make_shared<arrow::Field>(GetColumnName(columnId), std::make_shared<arrow::StringType>())};
            auto schema = std::make_shared<arrow::Schema>(v);
            return NKikimr::NOlap::TColumnLoader(nullptr, std::make_shared<NKikimr::NArrow::NSerialization::TFullDataDeserializer>(), schema, columnId);
        }

        virtual ui32 GetColumnId(const std::string& columnName) const override {
            auto it = Decoder.find(columnName);
            if (it == Decoder.end()) {
                it = Decoder.emplace(columnName, Decoder.size()).first;
            }
            return it->second;
        }

        std::string GetColumnName(const ui32 columnId) const {
            for (auto&& i : Decoder) {
                if (i.second == columnId) {
                    return i.first;
                }
            }
            Y_FAIL("cannot find column by id");
            return "";
        }
    };

    class TSplitTester {
    private:
        std::shared_ptr<TTestSnapshotSchema> Schema = std::make_shared<TTestSnapshotSchema>();
        YDB_ACCESSOR_DEF(std::optional<ui32>, ExpectSlicesCount);
        YDB_ACCESSOR_DEF(std::optional<ui32>, ExpectBlobsCount);
        YDB_ACCESSOR(bool, HasMultiSplit, false);
    public:

        void Execute(std::shared_ptr<arrow::RecordBatch> batch) {
            NKikimr::NColumnShard::TIndexationCounters counters("test");
            NKikimr::NOlap::TRBSplitLimiter limiter(counters.SplitterCounters, Schema, batch, NKikimr::NOlap::TSplitSettings());
            std::vector<std::vector<NKikimr::NOlap::TOrderedColumnChunk>> chunksForBlob;
            std::map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> restoredBatch;
            std::vector<i64> blobsSize;
            bool hasMultiSplit = false;
            ui32 blobsCount = 0;
            ui32 slicesCount = 0;
            ui32 chunksCount = 0;
            std::shared_ptr<arrow::RecordBatch> sliceBatch;
            while (limiter.Next(chunksForBlob, sliceBatch)) {
                ++slicesCount;
                TStringBuilder sb;
                std::map<ui32, ui32> recordsCountByColumn;
                for (auto&& chunks : chunksForBlob) {
                    ++blobsCount;
                    ui64 blobSize = 0;
                    sb << "[";
                    std::set<ui32> blobColumnChunks;
                    for (auto&& i : chunks) {
                        ++chunksCount;
                        const ui32 columnId = i.GetChunkAddress().GetColumnId();
                        recordsCountByColumn[columnId] += i.GetRecordsCount();
                        restoredBatch[Schema->GetColumnName(columnId)].emplace_back(*Schema->GetColumnLoader(columnId).Apply(i.GetData()));
                        blobSize += i.GetData().size();
                        if (i.GetRecordsCount() != NKikimr::NOlap::TSplitSettings().GetMinRecordsCount() && !blobColumnChunks.emplace(columnId).second) {
                            hasMultiSplit = true;
                        }
                        sb << "(" << i.DebugString() << ")";
                    }
                    blobsSize.emplace_back(blobSize);
                    sb << "];";
                }
                std::optional<ui32> columnRecordsCount;
                for (auto&& i : recordsCountByColumn) {
                    if (!columnRecordsCount) {
                        columnRecordsCount = i.second;
                    } else {
                        Y_VERIFY(i.second == *columnRecordsCount);
                    }
                }
                Cerr << sb << Endl;
            }
            if (ExpectBlobsCount) {
                Y_VERIFY(*ExpectBlobsCount == blobsCount);
            }
            if (ExpectSlicesCount) {
                Y_VERIFY(*ExpectSlicesCount == slicesCount);
            }
            Y_VERIFY(hasMultiSplit == HasMultiSplit);
            for (auto&& i : blobsSize) {
                Y_VERIFY(i < NKikimr::NOlap::TSplitSettings().GetMaxBlobSize());
                Y_VERIFY(i + 10000 >= NKikimr::NOlap::TSplitSettings().GetMinBlobSize() || blobsSize.size() == 1);
            }
            Y_VERIFY(restoredBatch.size() == (ui32)batch->num_columns());
            for (auto&& i : batch->schema()->fields()) {
                auto it = restoredBatch.find(i->name());
                Y_VERIFY(it != restoredBatch.end());
                auto column = batch->GetColumnByName(i->name());
                Y_VERIFY(column);
                ui64 recordsCount = 0;
                for (auto&& c : it->second) {
                    Y_VERIFY(c->num_columns() == 1);
                    Y_VERIFY(c->column(0)->RangeEquals(column, 0, c->num_rows(), recordsCount, arrow::EqualOptions::Defaults()));
                    recordsCount += c->num_rows();
                }
                Y_VERIFY(recordsCount == (ui32)batch->num_rows());

            }
        }
    };

    Y_UNIT_TEST(Simple) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({column}).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(8).SetExpectSlicesCount(8).Execute(batch);
    }

    Y_UNIT_TEST(Small) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 24));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({column}).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(1).SetHasMultiSplit(true).Execute(batch);
    }

    Y_UNIT_TEST(Minimal) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({column}).BuildBatch(4048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(1).Execute(batch);
    }

    Y_UNIT_TEST(Trivial) {
        NConstruction::IArrayBuilder::TPtr column = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({column}).BuildBatch(10048);

        TSplitTester().SetExpectBlobsCount(1).SetExpectSlicesCount(1).Execute(batch);
    }

    Y_UNIT_TEST(BigAndSmall) {
        NConstruction::IArrayBuilder::TPtr columnBig = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 512));
        NConstruction::IArrayBuilder::TPtr columnSmall = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field2", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 1));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({columnBig, columnSmall}).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(8).SetExpectSlicesCount(8).Execute(batch);
    }

    Y_UNIT_TEST(Crit) {
        NConstruction::IArrayBuilder::TPtr columnBig = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field1", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 712));
        NConstruction::IArrayBuilder::TPtr columnSmall = std::make_shared<NKikimr::NArrow::NConstruction::TSimpleArrayConstructor<NKikimr::NArrow::NConstruction::TStringPoolFiller>>(
            "field2", NKikimr::NArrow::NConstruction::TStringPoolFiller(8, 128));
        std::shared_ptr<arrow::RecordBatch> batch = NKikimr::NArrow::NConstruction::TRecordBatchConstructor({columnBig, columnSmall}).BuildBatch(80048);
        NKikimr::NColumnShard::TIndexationCounters counters("test");

        TSplitTester().SetExpectBlobsCount(16).SetExpectSlicesCount(8).Execute(batch);
    }

};
