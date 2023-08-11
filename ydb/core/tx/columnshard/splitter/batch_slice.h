#pragma once
#include "chunks.h"
#include "stats.h"
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

class ISchemaDetailInfo {
public:
    using TPtr = std::shared_ptr<ISchemaDetailInfo>;
    virtual ~ISchemaDetailInfo() = default;
    virtual ui32 GetColumnId(const std::string& fieldName) const = 0;
    virtual TColumnSaver GetColumnSaver(const ui32 columnId) const = 0;
    virtual std::optional<TColumnSerializationStat> GetColumnSerializationStats(const ui32 columnId) const = 0;
    virtual std::optional<TColumnSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& rb) const = 0;
};

class TDefaultSchemaDetails: public ISchemaDetailInfo {
private:
    ISnapshotSchema::TPtr Schema;
    const TSaverContext Context;
    TSerializationStats Stats;
public:
    TDefaultSchemaDetails(ISnapshotSchema::TPtr schema, const TSaverContext& context, TSerializationStats&& stats)
        : Schema(schema)
        , Context(context)
        , Stats(std::move(stats))
    {

    }
    virtual std::optional<TColumnSerializationStat> GetColumnSerializationStats(const ui32 columnId) const override {
        return Stats.GetColumnInfo(columnId);
    }
    virtual std::optional<TColumnSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& rb) const override {
        return Stats.GetStatsForRecordBatch(rb);
    }
    virtual ui32 GetColumnId(const std::string& fieldName) const override {
        return Schema->GetColumnId(fieldName);
    }
    virtual TColumnSaver GetColumnSaver(const ui32 columnId) const override {
        return Schema->GetColumnSaver(columnId, Context);
    }
};

class TBatchSerializedSlice {
private:
    std::vector<TSplittedColumn> Columns;
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY(ui32, RecordsCount, 0);
    ISchemaDetailInfo::TPtr Schema;
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
    std::shared_ptr<NColumnShard::TSplitterCounters> Counters;
public:
    TBatchSerializedSlice(std::shared_ptr<arrow::RecordBatch> batch, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters);

    void MergeSlice(TBatchSerializedSlice&& slice);

    bool GroupBlobs(std::vector<TSplittedBlob>& blobs);

    bool operator<(const TBatchSerializedSlice& item) const {
        return Size < item.Size;
    }
};

}
