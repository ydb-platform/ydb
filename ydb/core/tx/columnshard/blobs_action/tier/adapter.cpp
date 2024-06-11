#include "adapter.h"
#include <ydb/core/base/logoblob.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/tx/columnshard/blob_cache.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

std::unique_ptr<NActors::IEventBase> TRepliesAdapter::RebuildReplyEvent(std::unique_ptr<NWrappers::NExternalStorage::TEvGetObjectResponse>&& ev) const {
    TLogoBlobID logoBlobId;
    TString error;
    AFL_VERIFY(TLogoBlobID::Parse(logoBlobId, *ev->Key, error))("error", error)("str_blob_id", *ev->Key);
    TBlobRange bRange(TUnifiedBlobId(Max<ui32>(), logoBlobId), ev->GetReadInterval().first, ev->GetReadIntervalLength());
    if (ev->IsSuccess()) {
        AFL_VERIFY(ev->Body.size() == ev->GetReadIntervalLength())("body_size", ev->Body.size())("result", ev->GetReadIntervalLength());
    }
    if (ev->IsSuccess()) {
        AFL_VERIFY(!!ev->Body)("key", ev->Key)("interval_from", ev->GetReadInterval().first)("interval_to", ev->GetReadInterval().second);
        return std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeResult>(bRange, NKikimrProto::EReplyStatus::OK, ev->Body, false, StorageId);
    } else {
        return std::make_unique<NBlobCache::TEvBlobCache::TEvReadBlobRangeResult>(bRange, NKikimrProto::EReplyStatus::ERROR, TStringBuilder() << ev->Result, false, StorageId);
    }
}

std::unique_ptr<NActors::IEventBase> TRepliesAdapter::RebuildReplyEvent(std::unique_ptr<NWrappers::NExternalStorage::TEvPutObjectResponse>&& ev) const {
    TLogoBlobID logoBlobId;
    TString error;
    Y_ABORT_UNLESS(ev->Key);
    AFL_VERIFY(TLogoBlobID::Parse(logoBlobId, *ev->Key, error))("error", error)("str_blob_id", *ev->Key);
    if (ev->IsSuccess()) {
        return std::make_unique<TEvBlobStorage::TEvPutResult>(NKikimrProto::EReplyStatus::OK, logoBlobId, 0, TGroupId::FromValue(Max<ui32>()), 0, StorageId);
    } else {
        return std::make_unique<TEvBlobStorage::TEvPutResult>(NKikimrProto::EReplyStatus::ERROR, logoBlobId, 0, TGroupId::FromValue(Max<ui32>()), 0, StorageId);
    }
}

}
