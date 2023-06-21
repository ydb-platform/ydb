#pragma once

#include "blob_constructor.h"

#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

class TCompactedBlobsConstructor : public IBlobConstructor {
    TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> WriteIndexEv;
    const NOlap::TColumnEngineChanges& IndexChanges;
    const std::vector<TString>& Blobs;

    const bool BlobGrouppingEnabled;
    const bool CacheData;

    TString AccumulatedBlob;
    std::vector<std::pair<size_t, TString>> RecordsInBlob;

    ui64 CurrentPortion = 0;
    ui64 LastPortion = 0;

    ui64 CurrentBlob = 0;
    ui64 CurrentPortionRecord = 0;

    TVector<NOlap::TPortionInfo> PortionUpdates; 

public:
    TCompactedBlobsConstructor(TAutoPtr<NColumnShard::TEvPrivate::TEvWriteIndex> writeIndexEv, bool blobGrouppingEnabled);
    const TString& GetBlob() const override;
    bool RegisterBlobId(const TUnifiedBlobId& blobId) override;
    EStatus BuildNext(NColumnShard::TUsage& resourceUsage, const TAppData& /*appData*/) override;

    TAutoPtr<IEventBase> BuildResult(NKikimrProto::EReplyStatus status, NColumnShard::TBlobBatch&& blobBatch, THashSet<ui32>&& yellowMoveChannels, THashSet<ui32>&& yellowStopChannels, const NColumnShard::TUsage& resourceUsage) override;

private:
    const NOlap::TPortionInfo& GetPortionInfo(const ui64 index) const;
};

}
