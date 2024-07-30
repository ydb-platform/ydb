#pragma once

#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/protos/table_stats.pb.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/insert_table/rt_insertion.h>

namespace NKikimr::NColumnShard {

class TTabletCountersHandle {
private:
    TTabletCountersBase& TabletCounters;

public:
    TTabletCountersHandle(TTabletCountersBase& stats)
        : TabletCounters(stats) {
    }

    void SetCounter(NColumnShard::ESimpleCounters counter, ui64 num) const {
        TabletCounters.Simple()[counter].Set(num);
    }

    void IncCounter(NColumnShard::ECumulativeCounters counter, ui64 num = 1) const {
        TabletCounters.Cumulative()[counter].Increment(num);
    }

    void IncCounter(NColumnShard::EPercentileCounters counter, const TDuration& latency) const {
        TabletCounters.Percentile()[counter].IncrementFor(latency.MicroSeconds());
    }

    void IncCounter(NDataShard::ESimpleCounters counter, ui64 num = 1) const {
        TabletCounters.Simple()[counter].Add(num);
    }

    void IncCounter(NDataShard::ECumulativeCounters counter, ui64 num = 1) const {
        TabletCounters.Cumulative()[counter].Increment(num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, ui64 num) const {
        TabletCounters.Percentile()[counter].IncrementFor(num);
    }

    void IncCounter(NDataShard::EPercentileCounters counter, const TDuration& latency) const {
        TabletCounters.Percentile()[counter].IncrementFor(latency.MilliSeconds());
    }

    ui64 GetValue(NColumnShard::ESimpleCounters counter) const {
        return TabletCounters.Simple()[counter].Get();
    }

    ui64 GetValue(NColumnShard::ECumulativeCounters counter) const {
        return TabletCounters.Cumulative()[counter].Get();
    }

    const TTabletPercentileCounter& GetValue(NColumnShard::EPercentileCounters counter) const {
        return TabletCounters.Percentile()[counter];
    }

    ui64 GetValue(NDataShard::ESimpleCounters counter) const {
        return TabletCounters.Simple()[counter].Get();
    }

    ui64 GetValue(NDataShard::ECumulativeCounters counter) const {
        return TabletCounters.Cumulative()[counter].Get();
    }

    const TTabletPercentileCounter& GetCounter(NDataShard::EPercentileCounters counter) const {
        return TabletCounters.Percentile()[counter];
    }

    void OnWriteSuccess(const ui64 blobsWritten, const ui64 bytesWritten) const {
        IncCounter(NColumnShard::COUNTER_UPSERT_BLOBS_WRITTEN, blobsWritten);
        IncCounter(NColumnShard::COUNTER_UPSERT_BYTES_WRITTEN, bytesWritten);
        //    self.Stats.GetTabletCounters().IncCounter(NColumnShard::COUNTER_RAW_BYTES_UPSERTED, insertedBytes);
        IncCounter(NColumnShard::COUNTER_WRITE_SUCCESS);
    }

    void OnWriteFailure() const {
        IncCounter(NColumnShard::COUNTER_WRITE_FAIL);
    }

    void OnScanStarted(const NOlap::TSelectInfo::TStats& countersDelta) const {
        IncCounter(NColumnShard::COUNTER_READ_INDEX_PORTIONS, countersDelta.Portions);
        IncCounter(NColumnShard::COUNTER_READ_INDEX_BLOBS, countersDelta.Blobs);
        IncCounter(NColumnShard::COUNTER_READ_INDEX_ROWS, countersDelta.Rows);
        IncCounter(NColumnShard::COUNTER_READ_INDEX_BYTES, countersDelta.Bytes);
    }

    void OnWriteCommitted(const NOlap::TInsertionSummary::TCounters& countersDelta) const {
        IncCounter(COUNTER_BLOBS_COMMITTED, countersDelta.Rows);
        IncCounter(COUNTER_BYTES_COMMITTED, countersDelta.Bytes);
        IncCounter(COUNTER_RAW_BYTES_COMMITTED, countersDelta.RawBytes);
    }

    void OnCompactionWriteIndexCompleted(bool success, const ui64 blobsWritten, const ui64 bytesWritten) const {
        IncCounter(success ? NColumnShard::COUNTER_SPLIT_COMPACTION_SUCCESS : NColumnShard::COUNTER_SPLIT_COMPACTION_FAIL);
        IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BLOBS_WRITTEN, blobsWritten);
        IncCounter(NColumnShard::COUNTER_SPLIT_COMPACTION_BYTES_WRITTEN, bytesWritten);
    }

    void OnInsertionWriteIndexCompleted(const ui64 blobsWritten, const ui64 bytesWritten, const TDuration duration) const {
        IncCounter(NColumnShard::COUNTER_INDEXING_BLOBS_WRITTEN, blobsWritten);
        IncCounter(NColumnShard::COUNTER_INDEXING_BYTES_WRITTEN, bytesWritten);
        IncCounter(NColumnShard::COUNTER_INDEXING_TIME, duration.MilliSeconds());
    }

    void FillStats(::NKikimrTableStats::TTableStats& output) const {
        output.SetRowUpdates(GetValue(COUNTER_WRITE_SUCCESS));
        output.SetRowDeletes(0); // manual deletes are not supported
        output.SetRowReads(0);   // all reads are range reads
        output.SetRangeReadRows(GetValue(COUNTER_READ_INDEX_ROWS));

        output.SetImmediateTxCompleted(GetValue(COUNTER_IMMEDIATE_TX_COMPLETED));
        output.SetTxRejectedByOverload(GetValue(COUNTER_WRITE_OVERLOAD));
        output.SetTxRejectedBySpace(GetValue(COUNTER_OUT_OF_SPACE));
        output.SetPlannedTxCompleted(GetValue(COUNTER_PLANNED_TX_COMPLETED));
        output.SetTxCompleteLagMsec(GetValue(COUNTER_TX_COMPLETE_LAG));
    }
};

}
