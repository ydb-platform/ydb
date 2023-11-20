#include "remove_declare.h"
#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

TRemoveDeclareCounters::TRemoveDeclareCounters(const TConsumerCounters& owner)
    : TBase(owner, "RemoveDeclare")
{
    RequestsCount = TBase::GetDeriviative("Requests/Count");
    RequestBytes = TBase::GetDeriviative("Requests/Bytes");

    RepliesCount = TBase::GetDeriviative("Replies/Count");
    ReplyBytes = TBase::GetDeriviative("Replies/Bytes");
    ReplyDurationBySize = TBase::GetHistogram("Replies/Duration/Bytes", NMonitoring::ExponentialHistogram(15, 2, 1));
    ReplyDurationByCount = TBase::GetHistogram("Replies/Duration/Count", NMonitoring::ExponentialHistogram(15, 2, 1));

    FailsCount = TBase::GetDeriviative("Fails/Count");
    FailBytes = TBase::GetDeriviative("Fails/Bytes");
    FailDurationBySize = TBase::GetHistogram("Fails/Duration/Bytes", NMonitoring::ExponentialHistogram(15, 2, 2));
    FailDurationByCount = TBase::GetHistogram("Fails/Duration/Count", NMonitoring::ExponentialHistogram(15, 2, 2));
}

}
