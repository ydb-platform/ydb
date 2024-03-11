#pragma once
#include "chunks.h"
#include "stats.h"
#include "scheme_info.h"
#include "column_info.h"
#include "blob_info.h"
#include <ydb/core/tx/columnshard/counters/indexation.h>
#include <ydb/core/tx/columnshard/engines/scheme/column_features.h>
#include <ydb/core/tx/columnshard/engines/scheme/abstract_scheme.h>
#include <ydb/core/tx/columnshard/engines/storage/granule.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NOlap {

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
    std::shared_ptr<TSerializationStats> Stats;
public:
    TDefaultSchemaDetails(ISnapshotSchema::TPtr schema, const TSaverContext& context, const std::shared_ptr<TSerializationStats>& stats)
        : Schema(schema)
        , Context(context)
        , Stats(stats)
    {
        AFL_VERIFY(Stats);
    }
    virtual std::shared_ptr<arrow::Field> GetField(const ui32 columnId) const override {
        return Schema->GetFieldByColumnIdOptional(columnId);
    }
    virtual bool NeedMinMaxForColumn(const ui32 columnId) const override {
        return Schema->GetIndexInfo().GetMinMaxIdxColumns().contains(columnId);
    }
    virtual bool IsSortedColumn(const ui32 columnId) const override {
        return Schema->GetIndexInfo().IsSortedColumn(columnId);
    }

    virtual std::optional<TColumnSerializationStat> GetColumnSerializationStats(const ui32 columnId) const override {
        auto stats = Stats->GetColumnInfo(columnId);
        if (stats && stats->GetRecordsCount() != 0) {
            return stats;
        }
        return std::nullopt;
    }
    virtual std::optional<TBatchSerializationStat> GetBatchSerializationStats(const std::shared_ptr<arrow::RecordBatch>& rb) const override {
        return Stats->GetStatsForRecordBatch(rb);
    }
    virtual ui32 GetColumnId(const std::string& fieldName) const override {
        return Schema->GetColumnId(fieldName);
    }
    virtual TColumnSaver GetColumnSaver(const ui32 columnId) const override {
        return Schema->GetColumnSaver(columnId, Context);
    }
};

class TGeneralSerializedSlice {
private:
    YDB_READONLY(ui32, RecordsCount, 0);
protected:
    std::vector<TSplittedEntity> Data;
    ui64 Size = 0;
    ISchemaDetailInfo::TPtr Schema;
    std::shared_ptr<NColumnShard::TSplitterCounters> Counters;
    TSplitSettings Settings;
    TGeneralSerializedSlice() = default;

    const TSplittedEntity& GetEntityDataVerified(const ui32& entityId) const {
        for (auto&& i : Data) {
            if (i.GetEntityId() == entityId) {
                return i;
            }
        }
        Y_ABORT_UNLESS(false);
        return Data.front();
    }
    bool GroupBlobsImpl(const TString& currentGroupName, const std::set<ui32>& entityIds, std::vector<TSplittedBlob>& blobs);

public:

    THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> GetPortionChunksToHash() const {
        THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> result;
        for (auto&& i : Data) {
            AFL_VERIFY(result.emplace(i.GetEntityId(), i.GetChunks()).second);
        }
        return result;
    }

    std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> GetPortionChunksToMap() const {
        std::map<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>> result;
        for (auto&& i : Data) {
            AFL_VERIFY(result.emplace(i.GetEntityId(), i.GetChunks()).second);
        }
        return result;
    }

    std::shared_ptr<arrow::RecordBatch> GetFirstLastPKBatch(const std::shared_ptr<arrow::Schema>& pkSchema) const {
        std::vector<std::shared_ptr<arrow::Array>> pkColumns;
        for (auto&& i : pkSchema->fields()) {
            auto aBuilder = NArrow::MakeBuilder(i);
            const TSplittedEntity& splittedEntity = GetEntityDataVerified(Schema->GetColumnId(i->name()));
            NArrow::TStatusValidator::Validate(aBuilder->AppendScalar(*splittedEntity.GetFirstScalar()));
            NArrow::TStatusValidator::Validate(aBuilder->AppendScalar(*splittedEntity.GetLastScalar()));
            pkColumns.emplace_back(NArrow::TStatusValidator::GetValid(aBuilder->Finish()));
        }
        return arrow::RecordBatch::Make(pkSchema, 2, pkColumns);
    }

    ui64 GetSize() const {
        return Size;
    }

    std::vector<TSplittedBlob> GroupChunksByBlobs(const TEntityGroups& groups) {
        std::vector<TSplittedBlob> blobs;
        AFL_VERIFY(GroupBlobs(blobs, groups));
        return blobs;
    }

    explicit TGeneralSerializedSlice(TVectorView<TGeneralSerializedSlice>&& objects, const TSplitSettings& settings)
        : Settings(settings)
    {
        Y_ABORT_UNLESS(objects.size());
        std::swap(*this, objects.front());
        for (ui32 i = 1; i < objects.size(); ++i) {
            MergeSlice(std::move(objects[i]));
        }
    }
    TGeneralSerializedSlice(const THashMap<ui32, std::vector<std::shared_ptr<IPortionDataChunk>>>& data, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings);
    TGeneralSerializedSlice(const ui32 recordsCount, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings);

    void MergeSlice(TGeneralSerializedSlice&& slice);

    bool GroupBlobs(std::vector<TSplittedBlob>& blobs, const TEntityGroups& groups) {
        if (groups.IsEmpty()) {
            return GroupBlobsImpl(groups.GetDefaultGroupName(), {}, blobs);
        } else {
            std::vector<TSplittedBlob> result;
            for (auto&& i : groups) {
                std::vector<TSplittedBlob> blobsLocal;
                if (!GroupBlobsImpl(i.first, i.second, blobsLocal)) {
                    return false;
                }
                result.insert(result.end(), blobsLocal.begin(), blobsLocal.end());
            }
            std::swap(result, blobs);
            return true;
        }
    }

    bool operator<(const TGeneralSerializedSlice& item) const {
        return Size < item.Size;
    }
};

class TBatchSerializedSlice: public TGeneralSerializedSlice {
private:
    using TBase = TGeneralSerializedSlice;
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Batch);
public:
    TBatchSerializedSlice(const std::shared_ptr<arrow::RecordBatch>& batch, ISchemaDetailInfo::TPtr schema, std::shared_ptr<NColumnShard::TSplitterCounters> counters, const TSplitSettings& settings);

    explicit TBatchSerializedSlice(TVectorView<TBatchSerializedSlice>&& objects) {
        Y_ABORT_UNLESS(objects.size());
        std::swap(*this, objects.front());
        for (ui32 i = 1; i < objects.size(); ++i) {
            MergeSlice(std::move(objects[i]));
        }
    }
    void MergeSlice(TBatchSerializedSlice&& slice) {
        Batch = NArrow::CombineBatches({Batch, slice.Batch});
        TBase::MergeSlice(std::move(slice));
    }
};

}
