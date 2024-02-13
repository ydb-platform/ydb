#include "subscriber.h"
#include "user_info.h"
#include <ydb/core/protos/counters_pq.pb.h>

namespace NKikimr {
namespace NPQ {

TSubscriberLogic::TSubscriberLogic()
{}

TMaybe<TReadInfo> TSubscriberLogic::ForgetSubscription(const ui64 cookie)
{
    auto it = ReadInfo.find(cookie);
    if (it == ReadInfo.end()) //already answered
        return TMaybe<TReadInfo>();
    TReadInfo res(std::move(it->second));
    ReadInfo.erase(it);
    return res;
}

void TSubscriberLogic::AddSubscription(TReadInfo&& info, const ui64 cookie)
{
    Y_ABORT_UNLESS(WaitingReads.empty() || WaitingReads.back().Offset == info.Offset);
    info.IsSubscription = true;
    WaitingReads.push_back({info.Offset, cookie});
    bool res = ReadInfo.insert({cookie, std::move(info)}).second;
    Y_ABORT_UNLESS(res);
}

TVector<std::pair<TReadInfo, ui64>> TSubscriberLogic::CompleteSubscriptions(const ui64 endOffset)
{
    TVector<std::pair<TReadInfo, ui64>> res;
    while (!WaitingReads.empty()) {
        const ui64& offset = WaitingReads.front().Offset;
        const ui64& cookie = WaitingReads.front().Cookie;
        if (offset >= endOffset)
            break;
        auto it = ReadInfo.find(cookie);
        if (it != ReadInfo.end()) {
            it->second.Timestamp = TAppData::TimeProvider->Now();
            Y_ABORT_UNLESS(it->second.Offset == offset);
            res.emplace_back(std::move(it->second), it->first);
            ReadInfo.erase(it);
        }
        WaitingReads.pop_front();
    }
    return res;
}


TSubscriber::TSubscriber(const TPartitionId& partition, TTabletCountersBase& counters, const TActorId& tablet)
    : Subscriber()
    , Partition(partition)
    , Counters(counters)
    , Tablet(tablet)
{}

TMaybe<TReadInfo> TSubscriber::OnTimeout(TEvPQ::TEvReadTimeout::TPtr& ev) {
    TMaybe<TReadInfo> res = Subscriber.ForgetSubscription(ev->Get()->Cookie);
    if (res) {
        Counters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_TIMEOUT].Increment(1);
    }
    return res;
}

void TSubscriber::AddSubscription(TReadInfo&& info, const ui32 timeout, const ui64 cookie, const TActorContext& ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "waiting read cookie " << cookie << " partition " << Partition << " user " << info.User
                << " offset " << info.Offset << " count " << info.Count << " size " << info.Size << " timeout " << timeout);
    Subscriber.AddSubscription(std::move(info), cookie);
    if (timeout == 0)
        ctx.Send(ctx.SelfID, new TEvPQ::TEvReadTimeout(cookie));
    else
        ctx.Schedule(TDuration::MilliSeconds(timeout), new TEvPQ::TEvReadTimeout(cookie));
}

TVector<std::pair<TReadInfo, ui64>> TSubscriber::GetReads(const ui64 endOffset) {
    auto res = Subscriber.CompleteSubscriptions(endOffset);
    Counters.Cumulative()[COUNTER_PQ_READ_SUBSCRIPTION_OK].Increment(res.size());
    return res;
}

}// NPQ
}// NKikimr

