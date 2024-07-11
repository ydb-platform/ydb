#pragma once

#include "kqp_compute.h"
#include "kqp_scan_data_meta.h"

#include <ydb/library/services/services.pb.h>

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/formats/arrow/permutations.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api_vector.h>

namespace NKikimrTxDataShard {
    class TKqpTransaction_TScanTaskMeta;
}

namespace NKikimr {
namespace NMiniKQL {

struct TBytesStatistics {
    ui64 AllocatedBytes = 0;
    ui64 DataBytes = 0;

    TBytesStatistics() = default;
    TBytesStatistics(const ui64 allocated, const ui64 data)
        : AllocatedBytes(allocated)
        , DataBytes(data)
    {}

    TBytesStatistics operator+(const TBytesStatistics& item) const {
        return TBytesStatistics(AllocatedBytes + item.AllocatedBytes, DataBytes + item.DataBytes);
    }

    void operator+=(const TBytesStatistics& item) {
        AllocatedBytes += item.AllocatedBytes;
        DataBytes += item.DataBytes;
    }

    template <class T>
    TBytesStatistics operator*(const T kff) const {
        static_assert(std::is_arithmetic<T>());
        return TBytesStatistics(AllocatedBytes * kff, DataBytes * kff);
    }

    void AddStatistics(const TBytesStatistics& other) {
        AllocatedBytes += other.AllocatedBytes;
        DataBytes += other.DataBytes;
    }

};

class TBatchDataAccessor {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::Table>, Batch);
    YDB_READONLY_DEF(std::vector<ui32>, DataIndexes);
    mutable std::shared_ptr<arrow::Table> FilteredBatch;
public:
    std::shared_ptr<arrow::Table> GetFiltered() const {
        if (!FilteredBatch) {
            if (DataIndexes.size()) {
                auto permutation = NArrow::MakeFilterPermutation(DataIndexes);
                FilteredBatch = NArrow::TStatusValidator::GetValid(arrow::compute::Take(Batch, permutation)).table();
            } else {
                FilteredBatch = Batch;
            }
        }
        return FilteredBatch;
    }

    bool HasDataIndexes() const {
        return DataIndexes.size();
    }

    ui32 GetRecordsCount() const {
        return DataIndexes.size() ? DataIndexes.size() : Batch->num_rows();
    }

    TBatchDataAccessor(const std::shared_ptr<arrow::Table>& batch, std::vector<ui32>&& dataIndexes)
        : Batch(batch)
        , DataIndexes(std::move(dataIndexes))
    {
        AFL_VERIFY(Batch);
        AFL_VERIFY(Batch->num_rows());
    }

    TBatchDataAccessor(const std::shared_ptr<arrow::Table>& batch)
        : Batch(batch) {
        AFL_VERIFY(Batch);
        AFL_VERIFY(Batch->num_rows());

    }

    TBatchDataAccessor(const std::shared_ptr<arrow::RecordBatch>& batch)
        : Batch(NArrow::TStatusValidator::GetValid(arrow::Table::FromRecordBatches({batch}))) {
        AFL_VERIFY(Batch);
        AFL_VERIFY(Batch->num_rows());

    }
};

TBytesStatistics GetUnboxedValueSize(const NUdf::TUnboxedValue& value, const NScheme::TTypeInfo& type);
TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, NScheme::TTypeInfo columnType);
TBytesStatistics WriteColumnValuesFromArrow(NUdf::TUnboxedValue* editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, const ui32 columnsCount, NScheme::TTypeInfo columnType);
TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const TBatchDataAccessor& batch, i64 columnIndex, i64 resultColumnIndex, NScheme::TTypeInfo columnType);

void FillSystemColumn(NUdf::TUnboxedValue& rowItem, TMaybe<ui64> shardId, NTable::TTag tag, NScheme::TTypeInfo type);

std::pair<ui64, ui64> GetUnboxedValueSizeForTests(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type);

class IKqpTableReader : public TSimpleRefCount<IKqpTableReader> {
public:
    virtual ~IKqpTableReader() = default;

    virtual NUdf::EFetchStatus Next(NUdf::TUnboxedValue& result) = 0;
    virtual EFetchResult Next(NUdf::TUnboxedValue* const* output) = 0;
};

class TKqpScanComputeContext : public TKqpComputeContextBase {
public:
    class TScanData: public TScanDataMeta {
    private:
        using TBase = TScanDataMeta;
    public:
        TScanData(TScanData&&) = default; // needed to create TMap<ui32, TScanData> Scans
        TScanData(const TTableId& tableId, const TTableRange& range, const TSmallVec<TColumn>& columns,
            const TSmallVec<TColumn>& systemColumns, const TSmallVec<TColumn>& resultColumns);

        ui32 FillDataValues(NUdf::TUnboxedValue* const* result);

        TScanData(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta, NYql::NDqProto::EDqStatsMode statsMode);

        ~TScanData() = default;

        const TSmallVec<TColumn>& GetColumns() const {
            return BatchReader->GetColumns();
        }

        ui64 AddData(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory);
        ui64 AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory);

        bool IsEmpty() const {
            return BatchReader->IsEmpty();
        }

        ui64 GetStoredBytes() const {
            return BatchReader->GetStoredBytes();
        }

        void Finish() {
            Finished = true;
        }

        bool IsFinished() const {
            return Finished;
        }

        void Clear() {
            BatchReader->Clear();
        }

    public:
        ui64 TaskId = 0;
        TSerializedTableRange Range;

        // shared with actor via TableReader
        TIntrusivePtr<IKqpTableReader> TableReader;

        struct TBasicStats {
            size_t Rows = 0;
            size_t Bytes = 0;
            ui32 AffectedShards = 0;
        };

        struct TProfileStats {
            size_t PageFaults = 0;
            size_t Messages = 0;
            size_t MessagesByPageFault = 0;

            // Produce statistics
            TDuration ScanCpuTime;
            TDuration ScanWaitTime;   // IScan waiting data time
        };

        enum class EReadType {
            Rows,
            Blocks
        };

        std::unique_ptr<TBasicStats> BasicStats;
        std::unique_ptr<TProfileStats> ProfileStats;

    private:
        class IDataBatchReader: public TScanDataColumnsMeta {
        private:
            using TBase = TScanDataColumnsMeta;
        public:
            using TBase::TBase;

            virtual ~IDataBatchReader() = default;

            ui64 GetStoredBytes() const {
                return StoredBytes;
            }

            virtual TBytesStatistics AddData(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) = 0;
            virtual TBytesStatistics AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) = 0;
            virtual ui32 FillDataValues(NUdf::TUnboxedValue* const* result) = 0;
            virtual void Clear() = 0;
            virtual bool IsEmpty() const = 0;
        protected:
            double StoredBytes = 0;
        };

        class TRowBatchReader : public IDataBatchReader {
        public:
            TRowBatchReader(const TSmallVec<TColumn>& columns, const TSmallVec<TColumn>& systemColumns,
                const TSmallVec<TColumn>& resultColumns)
                : IDataBatchReader(columns, systemColumns, resultColumns)
            {}

            TRowBatchReader(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta)
                : IDataBatchReader(meta)
            {}

            ~TRowBatchReader() {
                Y_VERIFY_DEBUG_S(RowBatches.empty(), "Buffer in TRowBatchReader was not cleared, data is leaking. "
                    << "Queue of UnboxedValues must be emptied under allocator using Clear() method, but has "
                    << RowBatches.size() << " elements!");
            }

            TBytesStatistics AddData(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) override;
            TBytesStatistics AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) override;
            ui32 FillDataValues(NUdf::TUnboxedValue* const* result) override;

            void Clear() override {
                TQueue<TRowBatch> newQueue;
                std::swap(newQueue, RowBatches);
            }

            bool IsEmpty() const override {
                return RowBatches.empty();
            }
        private:
            class TRowBatch {
            private:
                const ui32 CellsCountForRow;
                const ui32 ColumnsCount;
                const ui32 RowsCount;
                TUnboxedValueVector Cells;
                ui64 CurrentRow = 0;
                const ui64 AllocatedBytes;
            public:

                explicit TRowBatch(const ui32 columnsCount, const ui32 rowsCount, TUnboxedValueVector&& cells, const ui64 allocatedBytes)
                    : CellsCountForRow(columnsCount ? columnsCount : 1)
                    , ColumnsCount(columnsCount)
                    , RowsCount(rowsCount)
                    , Cells(std::move(cells))
                    , AllocatedBytes(allocatedBytes)
                {
                    Y_ABORT_UNLESS(AllocatedBytes);
                    Y_ABORT_UNLESS(RowsCount);
                }

                double BytesForRecordEstimation() {
                    return 1.0 * AllocatedBytes / RowsCount;
                }

                bool IsFinished() {
                    return CurrentRow * CellsCountForRow == Cells.size();
                }

                ui32 FillUnboxedCells(NUdf::TUnboxedValue* const* result);
            };

            TQueue<TRowBatch> RowBatches;
        };

        class TBlockBatchReader : public IDataBatchReader {
        public:
            TBlockBatchReader(const TSmallVec<TColumn>& columns, const TSmallVec<TColumn>& systemColumns,
                const TSmallVec<TColumn>& resultColumns)
                : IDataBatchReader(columns, systemColumns, resultColumns)
            {}

            TBlockBatchReader(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta)
                : IDataBatchReader(meta)
            {}

            ~TBlockBatchReader() {
                Y_VERIFY_DEBUG_S(BlockBatches.empty(), "Buffer in TBlockBatchReader was not cleared, data is leaking. "
                    << "Queue of UnboxedValues must be emptied under allocator using Clear() method, but has "
                    << BlockBatches.size() << " elements!");
            }

            TBytesStatistics AddData(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) override;
            TBytesStatistics AddData(const TBatchDataAccessor& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory) override;
            ui32 FillDataValues(NUdf::TUnboxedValue* const* result) override;

            void Clear() override {
                TQueue<TBlockBatch> newQueue;
                std::swap(newQueue, BlockBatches);
            }

            bool IsEmpty() const override {
                return BlockBatches.empty();
            }
        private:
            class TBlockBatch {
            private:
                const ui32 ColumnsCount;
                const ui32 RowsCount;
                TUnboxedValueVector BatchValues;
                const ui64 AllocatedBytes;
            public:
                explicit TBlockBatch(const ui32 columnsCount, const ui32 rowsCount, TUnboxedValueVector&& value, const ui64 allocatedBytes)
                    : ColumnsCount(columnsCount)
                    , RowsCount(rowsCount)
                    , BatchValues(std::move(value))
                    , AllocatedBytes(allocatedBytes)
                {
                    Y_ABORT_UNLESS(AllocatedBytes);
                    Y_ABORT_UNLESS(RowsCount);
                }

                double BytesForRecordEstimation() {
                    return 1.0 * AllocatedBytes;
                }

                ui32 FillBlockValues(NUdf::TUnboxedValue* const* result);
            };

            TQueue<TBlockBatch> BlockBatches;
        };

        std::unique_ptr<IDataBatchReader> BatchReader;
        bool Finished = false;
    };

public:
    explicit TKqpScanComputeContext(NYql::NDqProto::EDqStatsMode statsMode)
        : StatsMode(statsMode) {}

    TIntrusivePtr<IKqpTableReader> ReadTable(ui32 callableId) const;

    void AddTableScan(ui32 callableId, const TTableId& tableId, const TTableRange& range,
        const TSmallVec<TColumn>& columns, const TSmallVec<TColumn>& systemColumns, const TSmallVec<bool>& skipNullKeys);

    void AddTableScan(ui32 callableId, const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta,
        NYql::NDqProto::EDqStatsMode statsMode);

    TScanData& GetTableScan(ui32 callableId);
    TMap<ui32, TScanData>& GetTableScans();
    const TMap<ui32, TScanData>& GetTableScans() const;

    void Clear() {
        for (auto& scan: Scans) {
            scan.second.Clear();
        }
        Scans.clear();
    }

private:
    const NYql::NDqProto::EDqStatsMode StatsMode;
    TMap<ui32, TScanData> Scans;
};

TIntrusivePtr<IKqpTableReader> CreateKqpTableReader(TKqpScanComputeContext::TScanData& scanData);

} // namespace NMiniKQL
} // namespace NKikimr
