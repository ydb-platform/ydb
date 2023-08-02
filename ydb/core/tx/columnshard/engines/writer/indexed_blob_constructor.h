#pragma once

#include "blob_constructor.h"
#include "write_controller.h"

#include <ydb/core/tx/ev_write/write_data.h>
#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>


namespace NKikimr::NOlap {

class TIndexedWriteController : public NColumnShard::IWriteController {
private:
    class TBlobConstructor : public IBlobConstructor {
        TIndexedWriteController& Owner;
        NOlap::ISnapshotSchema::TPtr SnapshotSchema;

        TString DataPrepared;
        std::shared_ptr<arrow::RecordBatch> Batch;

    public:
        TBlobConstructor(NOlap::ISnapshotSchema::TPtr snapshotSchema, TIndexedWriteController& owner);

        const TString& GetBlob() const override;
        EStatus BuildNext() override;
        bool RegisterBlobId(const TUnifiedBlobId& blobId) override;
        const NOlap::TSnapshot& GetSnapshot() const {
            return SnapshotSchema->GetSnapshot();
        }
    };

    NEvWrite::TWriteData WriteData;
    std::shared_ptr<TBlobConstructor> BlobConstructor;
    std::vector<NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData> BlobData;
    TActorId DstActor;

public:
    TIndexedWriteController(const TActorId& dstActor, const NEvWrite::TWriteData& writeData, NOlap::ISnapshotSchema::TPtr snapshotSchema);

    void DoOnReadyResult(const NActors::TActorContext& ctx, const NColumnShard::TBlobPutResult::TPtr& putResult) override;

    NOlap::IBlobConstructor::TPtr GetBlobConstructor() override {
        return BlobConstructor;
    }

public:
    void AddBlob(NColumnShard::TEvPrivate::TEvWriteBlobsResult::TPutBlobData&& data, const ui64 numRows, const ui64 batchSize);
};

}
