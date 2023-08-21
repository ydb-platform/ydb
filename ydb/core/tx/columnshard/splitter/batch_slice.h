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

template <class TContainer>
class TArrayView {
private:
    typename TContainer::iterator Begin;
    typename TContainer::iterator End;
public:
    TArrayView(typename TContainer::iterator itBegin, typename TContainer::iterator itEnd)
        : Begin(itBegin)
        , End(itEnd) {

    }

    typename TContainer::iterator begin() {
        return Begin;
    }

    typename TContainer::iterator end() {
        return End;
    }

    typename TContainer::value_type& front() {
        return *Begin;
    }

    typename TContainer::value_type& operator[](const size_t index) {
        return *(Begin + index);
    }

    size_t size() {
        return End - Begin;
    }
};

template <class TObject>
using TVectorView = TArrayView<std::vector<TObject>>;

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
    TSplitSettings Settings;
public:
    explicit TBatchSerializedSlice(TVectorView<TBatchSerializedSlice>&& objects) {
        Y_VERIFY(objects.size());
        std::swap(*this, objects.front());
        for (ui32 i = 1; i < objects.size(); ++i) {
            MergeSlice(std::move(objects[i]));
        }
    }
    TBatchSerializedSlice(std::shared_ptr<arrow::RecordBatch> batch, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings);

    void MergeSlice(TBatchSerializedSlice&& slice);

    bool GroupBlobs(std::vector<TSplittedBlob>& blobs);

    bool operator<(const TBatchSerializedSlice& item) const {
        return Size < item.Size;
    }
};

}
