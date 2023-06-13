#include "blobs_manager.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>

namespace NKikimr::NColumnShard {

TBlobsManagerCounters::TBlobsManagerCounters(const TString& module)
    : TCommonCountersOwner(module)
{
    SkipCollection = TBase::GetDeriviative("GC/Skip/Count");
    StartCollection = TBase::GetDeriviative("GC/Start/Count");
    CollectDropExplicitBytes = TBase::GetDeriviative("GC/Drop/Explicit/Bytes");
    CollectDropExplicitCount = TBase::GetDeriviative("GC/Drop/Explicit/Count");
    CollectDropImplicitBytes = TBase::GetDeriviative("GC/Drop/Implicit/Bytes");
    CollectDropImplicitCount = TBase::GetDeriviative("GC/Drop/Implicit/Count");
    CollectKeepBytes = TBase::GetDeriviative("GC/Keep/Bytes");
    CollectKeepCount = TBase::GetDeriviative("GC/Keep/Count");
    PutBlobBytes = TBase::GetDeriviative("GC/PutBlob/Bytes");
    PutBlobCount = TBase::GetDeriviative("GC/PutBlob/Count");
    CollectGen = TBase::GetValue("GC/Gen");
    CollectStep = TBase::GetValue("GC/Step");

    DeleteBlobMarkerBytes = TBase::GetDeriviative("GC/MarkerDeleteBlob/Bytes");
    DeleteBlobMarkerCount = TBase::GetDeriviative("GC/MarkerDeleteBlob/Count");
    DeleteBlobDelayedMarkerBytes = TBase::GetDeriviative("GC/MarkerDelayedDeleteBlob/Bytes");
    DeleteBlobDelayedMarkerCount = TBase::GetDeriviative("GC/MarkerDelayedDeleteBlob/Count");
    AddSmallBlobBytes = TBase::GetDeriviative("GC/AddSmallBlob/Bytes");
    AddSmallBlobCount = TBase::GetDeriviative("GC/AddSmallBlob/Count");
    DeleteSmallBlobBytes = TBase::GetDeriviative("GC/DeleteSmallBlob/Bytes");
    DeleteSmallBlobCount = TBase::GetDeriviative("GC/DeleteSmallBlob/Count");

    BlobsKeepCount = TBase::GetValue("GC/BlobsKeep/Count");
    BlobsKeepBytes = TBase::GetValue("GC/BlobsKeep/Bytes");
    BlobsDeleteCount = TBase::GetValue("GC/BlobsDelete/Count");
    BlobsDeleteBytes = TBase::GetValue("GC/BlobsDelete/Bytes");

    BrokenKeepCount = TBase::GetDeriviative("GC/BrokenKeep/Count");
    BrokenKeepBytes = TBase::GetDeriviative("GC/BrokenKeep/Bytes");

    KeepMarkerCount = TBase::GetDeriviative("GC/KeepMarker/Count");
    KeepMarkerBytes = TBase::GetDeriviative("GC/KeepMarker/Bytes");
}

void TBlobsManagerCounters::OnBlobsKeep(const TSet<TLogoBlobID>& blobs) const {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "OnBlobsKeep")("count", blobs.size());
//    BlobsKeepCount->Set(blobs.size());
//    ui64 size = 0;
//    for (auto&& i : blobs) {
//        size += i.BlobSize();
//    }
//    BlobsKeepBytes->Set(size);
}

void TBlobsManagerCounters::OnBlobsDelete(const TSet<TLogoBlobID>& /*blobs*/) const {
    //        BlobsDeleteCount->Set(blobs.size());
    //        ui64 size = 0;
    //        for (auto&& i : blobs) {
    //            size += i.BlobSize();
    //        }
    //        BlobsDeleteBytes->Set(size);
}

}
