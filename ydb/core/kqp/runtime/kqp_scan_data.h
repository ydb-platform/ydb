#pragma once

#include <ydb/core/protos/services.pb.h>
#include "kqp_compute.h"

#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/formats/arrow_helpers.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_database.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <library/cpp/actors/core/log.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NKikimrTxDataShard {
    class TKqpTransaction_TScanTaskMeta;
}

namespace NKikimr {
namespace NMiniKQL {

struct TBytesStatistics {
    ui64 AllocatedBytes = 0;
    ui64 DataBytes = 0;

    void AddStatistics(const TBytesStatistics& other) {
        AllocatedBytes += other.AllocatedBytes;
        DataBytes += other.DataBytes;
    }

};

TBytesStatistics GetUnboxedValueSize(const NUdf::TUnboxedValue& value, NScheme::TTypeInfo type);
TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const arrow::RecordBatch& batch, i64 columnIndex, NScheme::TTypeInfo columnType);
TBytesStatistics WriteColumnValuesFromArrow(NUdf::TUnboxedValue* editAccessors,
    const arrow::RecordBatch& batch, i64 columnIndex, const ui32 columnsCount, NScheme::TTypeInfo columnType);
TBytesStatistics WriteColumnValuesFromArrow(const TVector<NUdf::TUnboxedValue*>& editAccessors,
    const arrow::RecordBatch& batch, i64 columnIndex, i64 resultColumnIndex, NScheme::TTypeInfo columnType);

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
    class TScanData {
    public:
        TScanData(TScanData&&) = default; // needed to create TMap<ui32, TScanData> Scans
        TScanData(const TTableId& tableId, const TTableRange& range, const TSmallVec<TColumn>& columns,
            const TSmallVec<TColumn>& systemColumns, const TSmallVec<bool>& skipNullKeys,
            const TSmallVec<TColumn>& resultColumns);

        ui32 ColumnsCount() const {
            return ResultColumns.size() + SystemColumns.size();
        }

        ui32 FillUnboxedCells(NUdf::TUnboxedValue* const* result);

        TScanData(const NKikimrTxDataShard::TKqpTransaction_TScanTaskMeta& meta, NYql::NDqProto::EDqStatsMode statsMode);

        ~TScanData() {
            Y_VERIFY_DEBUG_S(RowBatches.empty(), "Buffer in TScanData was not cleared, data is leaking. "
                << "Queue of UnboxedValues must be emptied under allocator using Clear() method, but has "
                << RowBatches.size() << " elements!");
        }

        const TSmallVec<TColumn>& GetColumns() const {
            return Columns;
        }

        const TSmallVec<TColumn>& GetSystemColumns() const {
            return SystemColumns;
        }

        const TSmallVec<TColumn>& GetResultColumns() const {
            return ResultColumns;
        }

        ui64 AddRows(const TVector<TOwnedCellVec>& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory);

        ui64 AddRows(const arrow::RecordBatch& batch, TMaybe<ui64> shardId, const THolderFactory& holderFactory);

        bool IsEmpty() const {
            return RowBatches.empty();
        }

        ui64 GetStoredBytes() const {
            return StoredBytes;
        }

        void Finish() {
            Finished = true;
        }

        bool IsFinished() const {
            return Finished;
        }

        void Clear() {
            TQueue<RowBatch> newQueue;
            std::swap(newQueue, RowBatches);
        }

    public:
        ui64 TaskId = 0;
        TTableId TableId;
        TString TablePath;
        TSerializedTableRange Range;
        TSmallVec<bool> SkipNullKeys;

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

        std::unique_ptr<TBasicStats> BasicStats;
        std::unique_ptr<TProfileStats> ProfileStats;

    private:
        class RowBatch {
        private:
            const ui32 CellsCountForRow;
            const ui32 ColumnsCount;
            TUnboxedValueVector Cells;
            ui64 CurrentRow = 0;
        public:
            TMaybe<ui64> ShardId;

            explicit RowBatch(const ui32 columnsCount, TUnboxedValueVector&& cells, TMaybe<ui64> shardId)
                : CellsCountForRow(columnsCount ? columnsCount : 1)
                , ColumnsCount(columnsCount)
                , Cells(std::move(cells))
                , ShardId(shardId)
            {
            }

            const NUdf::TUnboxedValue* GetCurrentData() const {
                return Cells.data() + CurrentRow * CellsCountForRow;
            }

            bool IsFinished() {
                return CurrentRow * CellsCountForRow == Cells.size();
            }

            ui32 FillUnboxedCells(NUdf::TUnboxedValue* const* result);
        };

        TSmallVec<TColumn> Columns;
        TSmallVec<TColumn> SystemColumns;
        TSmallVec<TColumn> ResultColumns;
        TQueue<RowBatch> RowBatches;
        ui64 StoredBytes = 0;
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
