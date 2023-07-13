#pragma once

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/persqueue/events/internal.h>

#include <library/cpp/actors/core/hfunc.h>


namespace NKikimr {
namespace NPQ {

class TPercentileCounter;

namespace NReadSpeedLimiterEvents {
    struct TEvRequest : public TEventLocal<TEvRequest, TEvPQ::EvReadLimiterRequest> {
        TEvRequest(TEvPQ::TEvRead::TPtr readRequest)
            : ReadRequest(std::move(readRequest))
        {}

        TEvPQ::TEvRead::TPtr ReadRequest;
    };

    struct TEvResponse : public TEventLocal<TEvResponse, TEvPQ::EvReadLimiterResponse> {
        TEvResponse(TEvPQ::TEvRead::TPtr readRequest, TDuration waitTime)
            : ReadRequest(std::move(readRequest))
            , WaitTime(waitTime)
        {}

        TEvPQ::TEvRead::TPtr ReadRequest;
        TDuration WaitTime;
    };

    struct TEvConsumed : public TEventLocal<TEvConsumed, TEvPQ::EvReadLimiterConsumed> {
        TEvConsumed(ui64 readBytes, ui64 readRequestCookie)
            : ReadBytes(readBytes)
            , ReadRequestCookie(readRequestCookie)
        {}

        ui64 ReadBytes;
        ui64 ReadRequestCookie;
    };

    struct TEvCounters : public TEventLocal<TEvCounters, TEvPQ::EvReadLimiterCounters> {
        TEvCounters(const NKikimr::TTabletCountersBase& counters, const TString& user)
            : User(user)
        {
            Counters.Populate(counters);
        }

        NKikimr::TTabletCountersBase Counters;
        const TString User;
    };
}

class TReadSpeedLimiter : public TActorBootstrapped<TReadSpeedLimiter> {
private:
    static const TString READ_QUOTA_ROOT_PATH;

    struct TQueueEvent {
        TQueueEvent(TEvPQ::TEvRead::TPtr&& event, TInstant startWait)
            : Event(event)
            , StartWait(startWait)
        {}

        TEvPQ::TEvRead::TPtr Event;
        TInstant StartWait;
    };

private:
    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::PQ_READ_SPEED_LIMITER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPQ::TEvUpdateCounters, HandleUpdateCounters);
            HFuncTraced(NReadSpeedLimiterEvents::TEvRequest, HandleReadQuotaRequest);
            HFuncTraced(NReadSpeedLimiterEvents::TEvConsumed, HandleReadQuotaConsumed);
            HFuncTraced(TEvQuota::TEvClearance, HandleClearance);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
        default:
            break;
        };
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();

    TReadSpeedLimiter(
        TActorId tabletActor,
        TActorId partitionActor,
        ui64 tabletId,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        ui32 partition,
        const TString& user,
        const TTabletCountersBase& counters
    );

    void Bootstrap(const TActorContext& ctx);
    void InitCounters(const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void HandleUpdateCounters(TEvPQ::TEvUpdateCounters::TPtr& ev, const TActorContext& ctx);
    void HandleReadQuotaRequest(NReadSpeedLimiterEvents::TEvRequest::TPtr& ev, const TActorContext& ctx);
    void HandleReadQuotaConsumed(NReadSpeedLimiterEvents::TEvConsumed::TPtr& ev, const TActorContext& ctx);
    void HandleClearance(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx);

    void ApproveRead(TEvPQ::TEvRead::TPtr ev, TInstant startWait, const TActorContext& ctx);

private:
    TString LimiterDescription() const;

private:
    const TActorId TabletActor;
    const TActorId PartitionActor;
    const ui64 TabletId;
    const NPersQueue::TTopicConverterPtr TopicConverter;
    const ui32 Partition;
    const TString User;
    const TString ConsumerPath;
    const ui64 ReadCreditBytes;

    ui64 ConsumedBytesInCredit = 0;

    TString KesusPath;
    TString QuotaResourcePath;

    TDeque<TQueueEvent> Queue;

    bool QuotaRequestInFlight = false;
    ui64 CurrentQuotaRequestCookie = 0;
    THashSet<ui64> InProcessReadRequestCookies;


    TTabletCountersBase Counters;
    THolder<TPercentileCounter> QuotaWaitCounter;
    bool CountersInited = false;
    TInstant LastReportedErrorTime;
};

}// NPQ
}// NKikimr
