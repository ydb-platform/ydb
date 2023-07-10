#pragma once

#include "blob_constructor.h"

#include <ydb/core/tx/columnshard/engines/portion_info.h>
#include <ydb/core/tx/columnshard/columnshard.h>

namespace NKikimr::NOlap {

class TIndexedBlobConstructor : public IBlobConstructor {
    TAutoPtr<TEvColumnShard::TEvWrite> WriteEv;
    NOlap::ISnapshotSchema::TPtr SnapshotSchema;
    NColumnShard::TUsage ResourceUsage;

    TString DataPrepared;
    TString MetaString;
    std::shared_ptr<arrow::RecordBatch> Batch;
    ui64 Iteration = 0;

public:
    TIndexedBlobConstructor(TAutoPtr<TEvColumnShard::TEvWrite> writeEv, NOlap::ISnapshotSchema::TPtr snapshotSchema);

    const TString& GetBlob() const override;
    EStatus BuildNext() override;
    bool RegisterBlobId(const TUnifiedBlobId& blobId) override;

    NColumnShard::TUsage& GetResourceUsage() override {
        return ResourceUsage;
    }

    TAutoPtr<NActors::IEventBase> BuildResult(
        NKikimrProto::EReplyStatus status,
        NColumnShard::TBlobBatch&& blobBatch,
        THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels) override;
};

}
