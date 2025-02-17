#include "blobs_manager.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NColumnShard {

TBlobsManagerCounters::TBlobsManagerCounters(const TString& module)
    : TCommonCountersOwner(module)
    , BlobsToDeleteCount(TBase::GetValue("BlobsToDelete/Count"))
    , BlobsToDeleteDelayedCount(TBase::GetValue("BlobsToDeleteDelayed/Count"))
    , BlobsToKeepCount(TBase::GetValue("BlobsToKeep/Count"))
    , CurrentGen(TBase::GetValue("CurrentGen"))
    , CurrentStep(TBase::GetValue("CurrentStep"))
    , GCCounters(*this, "GC")

{

}

TBlobsManagerGCCounters::TBlobsManagerGCCounters(const TCommonCountersOwner& sameAs, const TString& componentName)
    : TBase(sameAs, componentName)
    , SkipCollectionEmpty(TBase::GetDeriviative("Skip/Empty/Count"))
    , SkipCollectionThrottling(TBase::GetDeriviative("Skip/Throttling/Count"))
{
    KeepsCountTasks = TBase::GetHistogram("Tasks/Keeps/Count", NMonitoring::ExponentialHistogram(16, 2, 100));
    KeepsCountBlobs = TBase::GetHistogram("Tasks/Keeps/Blobs", NMonitoring::ExponentialHistogram(16, 2, 100));
    KeepsCountBytes = TBase::GetHistogram("Tasks/Keeps/Bytes", NMonitoring::ExponentialHistogram(16, 2, 1024));
    DeletesCountBlobs = TBase::GetHistogram("Tasks/Deletes/Count", NMonitoring::ExponentialHistogram(16, 2, 100));
    DeletesCountTasks = TBase::GetHistogram("Tasks/Deletes/Blobs", NMonitoring::ExponentialHistogram(16, 2, 100));
    DeletesCountBytes = TBase::GetHistogram("Tasks/Deletes/Bytes", NMonitoring::ExponentialHistogram(16, 2, 1024));
    FullGCTasks = TBase::GetDeriviative("Tasks/Full/Count");
    MoveBarriers = TBase::GetDeriviative("Tasks/Barrier/Move");
    DontMoveBarriers = TBase::GetDeriviative("Tasks/Barrier/DontMove");
    GCTasks = TBase::GetDeriviative("Tasks/All/Count");
    EmptyGCTasks = TBase::GetDeriviative("Tasks/Empty/Count");
}

void TBlobsManagerGCCounters::OnGCTask(const ui32 keepsCount, const ui32 keepBytes, const ui32 deleteCount, const ui32 deleteBytes, const bool isFull, const bool moveBarrier) const {
    GCTasks->Add(1);
    if (isFull) {
        FullGCTasks->Add(1);
    }
    KeepsCountTasks->Collect(keepsCount);
    KeepsCountBlobs->Collect((i64)keepsCount, keepsCount);
    KeepsCountBytes->Collect((i64)keepsCount, keepBytes);
    DeletesCountTasks->Collect(deleteCount);
    DeletesCountBlobs->Collect((i64)deleteCount, deleteCount);
    DeletesCountBytes->Collect((i64)deleteCount, deleteBytes);
    if (moveBarrier) {
        MoveBarriers->Add(1);
    } else {
        DontMoveBarriers->Add(1);
    }
}

}
