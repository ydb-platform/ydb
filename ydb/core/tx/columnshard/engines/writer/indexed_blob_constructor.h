#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/data_events/write_data.h>
#include <ydb/core/tx/columnshard/blobs_action/abstract/write.h>
#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/counters/common/object_counter.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/formats/arrow/size_calcer.h>


namespace NKikimr::NOlap {

class TWriteAggregation;

class TWideSerializedBatch {
private:
    NArrow::TSerializedBatch SplittedBlobs;
    YDB_ACCESSOR_DEF(TBlobRange, Range);
    YDB_READONLY(TInstant, StartInstant, AppDataVerified().TimeProvider->Now());
    TWriteAggregation* ParentAggregation;
public:
    void InitBlobId(const TUnifiedBlobId& id);

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

class TWritingBlob {
private:
    std::vector<TWideSerializedBatch*> Ranges;
    std::vector<TString> BlobData;
    ui64 BlobSize = 0;
    bool Extracted = false;
public:
    TWritingBlob() = default;
    bool AddData(TWideSerializedBatch& batch) {
        AFL_VERIFY(!Extracted);
        if (BlobSize + batch.GetSplittedBlobs().GetSize() < 8 * 1024 * 1024) {
            Ranges.emplace_back(&batch);
            BlobSize += batch.GetSplittedBlobs().GetSize();
            batch.SetRange(TBlobRange(TUnifiedBlobId(0, 0, 0, 0, 0, 0, BlobSize), BlobData.size(), batch.GetSplittedBlobs().GetSize()));
            BlobData.emplace_back(batch.GetSplittedBlobs().GetData());
            return true;
        } else {
            AFL_VERIFY(BlobData.size());
            return false;
        }
    }

    void InitBlobId(const TUnifiedBlobId& blobId) {
        for (auto&& r : Ranges) {
            r->InitBlobId(blobId);
        }
    }

    TString ExtractBlobData();

    ui64 GetSize() const {
        return BlobSize;
    }
};

class TWriteAggregation {
private:
    NEvWrite::TWriteMeta WriteMeta;
    YDB_READONLY(ui64, SchemaVersion, 0);
    YDB_READONLY(ui64, Size, 0);
    YDB_READONLY(ui64, Rows, 0);
    YDB_ACCESSOR_DEF(std::vector<TWideSerializedBatch>, SplittedBlobs);
    YDB_READONLY_DEF(TVector<TWriteId>, WriteIds);
    YDB_READONLY_DEF(std::shared_ptr<NOlap::IBlobsWritingAction>, BlobsAction);
    YDB_READONLY_DEF(NArrow::TSchemaSubset, SchemaSubset);

public:
    const NEvWrite::TWriteMeta& GetWriteMeta() const {
        return WriteMeta;
    }

    NEvWrite::TWriteMeta& MutableWriteMeta() {
        return WriteMeta;
    }

    void AddWriteId(const TWriteId& id) {
        WriteIds.emplace_back(id);
    }

    TWriteAggregation(const NEvWrite::TWriteData& writeData, std::vector<NArrow::TSerializedBatch>&& splittedBlobs)
        : WriteMeta(writeData.GetWriteMeta())
        , SchemaVersion(writeData.GetData()->GetSchemaVersion())
        , Size(writeData.GetSize())
        , BlobsAction(writeData.GetBlobsAction())
        , SchemaSubset(writeData.GetSchemaSubsetVerified())
    {
        for (auto&& s : splittedBlobs) {
            SplittedBlobs.emplace_back(std::move(s), *this);
        }
        for (const auto& batch : SplittedBlobs) {
            Rows += batch->GetRowsCount();
        }
    }

    TWriteAggregation(const NEvWrite::TWriteData& writeData)
        : WriteMeta(writeData.GetWriteMeta())
        , SchemaVersion(writeData.GetData()->GetSchemaVersion()) 
        , Size(writeData.GetSize())
        , BlobsAction(writeData.GetBlobsAction()) {
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

    std::vector<std::shared_ptr<IBlobsWritingAction>> GetAddActions() const {
        return {BlobsAction};
    }

    std::vector<std::shared_ptr<IBlobsDeclareRemovingAction>> GetRemoveActions() const {
        if (DeclareRemoveAction) {
            return {DeclareRemoveAction};
        } else {
            return {};
        }
    }

    void InitReadyInstant(const TMonotonic instant);
    void InitStartSending(const TMonotonic instant);
    void InitReplyReceived(const TMonotonic instant);

    std::vector<TWritingBlob> GroupIntoBlobs();
};

class TIndexedWriteController : public NColumnShard::IWriteController, public NColumnShard::TMonitoringObjectsCounter<TIndexedWriteController, true> {
private:
    TWritingBuffer Buffer;
    TActorId DstActor;
    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;
    virtual void DoOnStartSending() override;

public:
    TIndexedWriteController(const TActorId& dstActor, const std::shared_ptr<IBlobsWritingAction>& action, std::vector<std::shared_ptr<TWriteAggregation>>&& aggregations);

};

}
