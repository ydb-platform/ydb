#include "remove_gc.h"
#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

TRemoveGCCounters::TRemoveGCCounters(const TConsumerCounters& owner)
    : TBase(owner, "RemoveGC")
{
    RequestsCount = TBase::GetDeriviative("Requests/Count");
    RequestBytes = TBase::GetDeriviative("Requests/Bytes");

    RepliesCount = TBase::GetDeriviative("Replies/Count");
    ReplyBytes = TBase::GetDeriviative("Replies/Bytes");

    GCFinishedCount = TBase::GetDeriviative("GCFinished/Count");
    GCFinishedBytes = TBase::GetHistogram("GCFinished/Bytes", NMonitoring::ExponentialHistogram(18, 2, 1024));
    GCFinishedBlobsCount = TBase::GetHistogram("GCFinished/Blobs/Count", NMonitoring::ExponentialHistogram(18, 2, 1));

    FailsCount = TBase::GetDeriviative("Fails/Count");
    FailBytes = TBase::GetDeriviative("Fails/Bytes");
}

}
