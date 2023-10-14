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

    FailsCount = TBase::GetDeriviative("Fails/Count");
    FailBytes = TBase::GetDeriviative("Fails/Bytes");
}

}
