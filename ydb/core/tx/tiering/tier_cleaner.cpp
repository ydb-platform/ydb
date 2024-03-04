#include "tier_cleaner.h"
#ifndef KIKIMR_DISABLE_S3_OPS

namespace NKikimr::NColumnShard::NTiers {

void TTierCleaner::Handle(NWrappers::NExternalStorage::TEvListObjectsResponse::TPtr& ev) {
    Truncated = ev->Get()->GetResult().GetIsTruncated();
    auto request = Aws::S3::Model::DeleteObjectsRequest();

    Aws::S3::Model::Delete deleteInfo;
    for (auto&& i : ev->Get()->GetResult().GetContents()) {
        Aws::S3::Model::ObjectIdentifier id;
        id.WithKey(i.GetKey());
        deleteInfo.AddObjects(std::move(id));
    }
    if (ev->Get()->GetResult().GetContents().empty()) {
        Send(OwnerId, new TEvTierCleared(TierName, false));
    } else {
        request.WithDelete(deleteInfo);
        Send(SelfId(), new NWrappers::TEvExternalStorage::TEvDeleteObjectsRequest(request));
    }
}

void TTierCleaner::Handle(NWrappers::NExternalStorage::TEvDeleteObjectsRequest::TPtr& ev) {
    Storage->Execute(ev);
}

void TTierCleaner::Handle(NWrappers::NExternalStorage::TEvDeleteObjectsResponse::TPtr& ev) {
    if (!ev->Get()->IsSuccess()) {
        ALS_ERROR(NKikimrServices::TX_TIERING) << "cannot remove objects pack: " << ev->Get()->GetResult();
        Send(OwnerId, new TEvTierCleared(TierName, true));
    } else {
        Send(OwnerId, new TEvTierCleared(TierName, Truncated));
    }
}

void TTierCleaner::Handle(NWrappers::NExternalStorage::TEvListObjectsRequest::TPtr& ev) {
    Storage->Execute(ev);
}

void TTierCleaner::Bootstrap() {
    Become(&TTierCleaner::StateMain);

    auto request = Aws::S3::Model::ListObjectsRequest()
        .WithPrefix("S3-" + ::ToString(PathId));
    Send(SelfId(), new NWrappers::TEvExternalStorage::TEvListObjectsRequest(request));
}

TTierCleaner::TTierCleaner(const TString& tierName, const TActorId& ownerId,
    const ui64 pathId, NWrappers::IExternalStorageConfig::TPtr storageConfig)
    : TierName(tierName)
    , OwnerId(ownerId)
    , PathId(pathId)
    , StorageConfig(storageConfig) {
    Storage = StorageConfig->ConstructStorageOperator();
    Y_ABORT_UNLESS(Storage);
}
}
#endif
