#include "write.h"
#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

TWriteCounters::TWriteCounters(const TConsumerCounters& owner)
    : TBase(owner, "Writer")
{
    RequestsCount = TBase::GetDeriviative("Requests/Count");
    RequestBytes = TBase::GetDeriviative("Requests/Bytes");

    RepliesCount = TBase::GetDeriviative("Replies/Count");
    ReplyBytes = TBase::GetDeriviative("Replies/Bytes");
    ReplyDuration = TBase::GetHistogram("Replies/Duration", NMonitoring::ExponentialHistogram(15, 2, 1000));

    FailsCount = TBase::GetDeriviative("Fails/Count");
    FailBytes = TBase::GetDeriviative("Fails/Bytes");
    FailDuration = TBase::GetHistogram("Fails/Duration", NMonitoring::ExponentialHistogram(15, 2, 1000));
}

}
