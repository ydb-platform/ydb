#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/data_events/write_data.h>

namespace NKikimr::NColumnShard {
class TColumnShard;
}

namespace NKikimr::NOlap {

class TWriteAggregation;

class TWideSerializedBatch {
private:
    NArrow::TSerializedBatch SplittedBlobs;
    YDB_ACCESSOR_DEF(TBlobRange, Range);
    YDB_READONLY(TInstant, StartInstant, AppDataVerified().TimeProvider->Now());
    TWriteAggregation* ParentAggregation;

public:
    const NArrow::TSerializedBatch& GetSplittedBlobs() const {
        return SplittedBlobs;
    }

    const NArrow::TSerializedBatch* operator->() const {
        return &SplittedBlobs;
    }

    TWriteAggregation& MutableAggregation() {
        return *ParentAggregation;
    }

    const TWriteAggregation& GetAggregation() const {
        return *ParentAggregation;
    }

    TWideSerializedBatch(NArrow::TSerializedBatch&& splitted, TWriteAggregation& parentAggregation)
        : SplittedBlobs(std::move(splitted))
        , ParentAggregation(&parentAggregation)
    {
    }
};

class TWriteAggregation {
private:
    std::shared_ptr<NEvWrite::TWriteMeta> WriteMeta;
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY(ui64, Rows, 0);
    YDB_ACCESSOR_DEF(std::vector<TWideSerializedBatch>, SplittedBlobs);
    YDB_READONLY_DEF(std::vector<TInsertWriteId>, InsertWriteIds);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

public:
    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return *WriteMeta;
    }

    const std::shared_ptr<NEvWrite::TWriteMeta>& GetWriteMetaPtr() const {
        return WriteMeta;
    }

    NEvWrite::TWriteMeta& MutableWriteMeta() {
        return *WriteMeta;
    }

    TWriteAggregation(const NEvWrite::TWriteData& writeData)
        : WriteMeta(writeData.GetWriteMetaPtr())
        , SchemaVersion(writeData.GetData()->GetSchemaVersion())
        , Size(writeData.GetSize())
        , BlobsAction(writeData.GetBlobsAction())
    {
        AFL_VERIFY(!writeData.GetSchemaSubset());
    }
};

class TWritingBuffer: public TMoveOnly {
private:
    std::shared_ptr<IBlobsWritingAction> BlobsAction;
    std::shared_ptr<IBlobsDeclareRemovingAction> DeclareRemoveAction;
    YDB_READONLY_DEF(std::vector<std::shared_ptr<TWriteAggregation>>, Aggregations);
    YDB_READONLY(ui64, SumSize, 0);

public:
    TWritingBuffer() = default;

    TWritingBuffer(const std::shared_ptr<IBlobsWritingAction>& action, std::vector<std::shared_ptr<TWriteAggregation>>&& aggregations)
        : BlobsAction(action)
        , Aggregations(std::move(aggregations))
    {
        AFL_VERIFY(BlobsAction);
        for (auto&& aggr : Aggregations) {
            SumSize += aggr->GetSize();
        }
    }

    bool IsEmpty() const {
        return Aggregations.empty();
    }

    void RemoveData(const std::shared_ptr<TWriteAggregation>& data, const std::shared_ptr<IBlobsStorageOperator>& bOperator) {
        THashMap<TUnifiedBlobId, ui32> linksCount;
        for (auto&& a : Aggregations) {
            for (auto&& s : a->GetSplittedBlobs()) {
                ++linksCount[s.GetRange().BlobId];
            }
        }

        for (ui32 i = 0; i < Aggregations.size(); ++i) {
            if (Aggregations[i].get() == data.get()) {
                for (auto&& s : Aggregations[i]->GetSplittedBlobs()) {
                    if (--linksCount[s.GetRange().BlobId] == 0) {
                        if (!DeclareRemoveAction) {
                            DeclareRemoveAction = bOperator->StartDeclareRemovingAction(NBlobOperations::EConsumer::WRITING_BUFFER);
                        }
                        DeclareRemoveAction->DeclareRemove(bOperator->GetSelfTabletId(), s.GetRange().BlobId);
                    }
                }
                Aggregations.erase(Aggregations.begin() + i);
                return;
            }
        }
        AFL_VERIFY(false);
    }
};

}   // namespace NKikimr::NOlap
