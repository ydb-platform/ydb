#pragma once

#include <ydb/core/quoter/public/quoter.h>
#include <ydb/core/persqueue/events/internal.h>

#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr {
namespace NPQ {

class TPercentileCounter;

struct TQuoterParams {
    TString KesusPath;
    TString ResourcePath;
};

namespace NAccountQuoterEvents {
    struct TEvRequest : public TEventLocal<TEvRequest, TEvPQ::EvAccountQuotaRequest> {
        TEvRequest(ui64 cookie, TAutoPtr<IEventHandle> request)
            : Cookie(cookie)
            , Request(std::move(request))
        {}
        ui64 Cookie;
        TAutoPtr<IEventHandle> Request;
    };

    struct TEvResponse : public TEventLocal<TEvResponse, TEvPQ::EvAccountQuotaResponse> {
        TEvResponse(TAutoPtr<TEvRequest>&& request, const TDuration& waitTime)
            : Request(std::move(request))
            , WaitTime(waitTime)
        {}
        TAutoPtr<TEvRequest> Request;
        TDuration WaitTime;
    };

    struct TEvConsumed : public TEventLocal<TEvConsumed, TEvPQ::EvAccountQuotaConsumed> {
        TEvConsumed(ui64 bytesConsumed, ui64 requestCookie)
            : BytesConsumed(bytesConsumed)
            , RequestCookie(requestCookie)
        {}

        ui64 BytesConsumed;
        ui64 RequestCookie;
    };

    struct TEvCounters : public TEventLocal<TEvCounters, TEvPQ::EvAccountQuotaCounters> {
        TEvCounters(const NKikimr::TTabletCountersBase& counters, bool isReadCounters, const TString& subject)
            : Subject(subject)
            , IsReadCounters(isReadCounters)
        {
            Counters.Populate(counters);
        }

        NKikimr::TTabletCountersBase Counters;
        const TString Subject;
        bool IsReadCounters;
    };
}

class TBasicAccountQuoter : public TActorBootstrapped<TBasicAccountQuoter> {
private:
    struct TQueueEvent {
        TQueueEvent(NAccountQuoterEvents::TEvRequest::TPtr request, TInstant startWait)
            : Request(std::move(request))
            , StartWait(startWait)
        {}

        NAccountQuoterEvents::TEvRequest::TPtr Request;
        TInstant StartWait;
    };

public:
    TBasicAccountQuoter(
        TActorId tabletActor,
        TActorId recepient,
        ui64 tabletId,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const TPartitionId& partition,
        const TQuoterParams& params,
        ui64 quotaCreditBytes,
        const TTabletCountersBase& counters,
        const TDuration& doNotQuoteAfterErrorPeriod
    );

    void Bootstrap(const TActorContext& ctx);

    void InitCounters(const TActorContext& ctx);
    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void HandleQuotaRequest(NAccountQuoterEvents::TEvRequest::TPtr& ev, const TActorContext& ctx);
    void HandleQuotaConsumed(NAccountQuoterEvents::TEvConsumed::TPtr& ev, const TActorContext& ctx);
    void HandleClearance(TEvQuota::TEvClearance::TPtr& ev, const TActorContext& ctx);

    void ApproveQuota(NAccountQuoterEvents::TEvRequest::TPtr& ev, TInstant startWait, const TActorContext& ctx);

    void HandleUpdateCounters(TEvPQ::TEvUpdateCounters::TPtr& ev, const TActorContext& ctx);

protected:
    virtual TString LimiterDescription() const = 0;
    virtual void InitCountersImpl(const TActorContext& ctx) = 0;
    virtual THolder<NAccountQuoterEvents::TEvCounters> MakeCountersUpdateEvent() = 0;

    STFUNC(StateWork)
    {
        TRACE_EVENT(NKikimrServices::PQ_RATE_LIMITER);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvPQ::TEvUpdateCounters, HandleUpdateCounters);
            HFuncTraced(NAccountQuoterEvents::TEvRequest, HandleQuotaRequest);
            HFuncTraced(NAccountQuoterEvents::TEvConsumed, HandleQuotaConsumed);
            HFuncTraced(TEvQuota::TEvClearance, HandleClearance);
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
        default:
            break;
        };
    }

    TString KesusPath;
    TString ResourcePath;
    const NPersQueue::TTopicConverterPtr TopicConverter;
    THolder<TPercentileCounter> QuotaWaitCounter;
    TTabletCountersBase Counters;
    const TPartitionId Partition;

private:
    const TActorId TabletActor;
    const TActorId Recepient;
    const ui64 TabletId;

    const ui64 CreditBytes = 0;

    ui64 ConsumedBytesInCredit = 0;

    TDeque<TQueueEvent> Queue;

    bool QuotaRequestInFlight = false;
    ui64 CurrentQuotaRequestCookie = 0;
    THashSet<ui64> InProcessQuotaRequestCookies;


    bool CountersInited = false;
    TInstant LastReportedErrorTime;
    TDuration DoNotQuoteAfterErrorPeriod;
};

class TAccountReadQuoter : public TBasicAccountQuoter {
private:
    static const TString READ_QUOTA_ROOT_PATH;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();

    TAccountReadQuoter(
        TActorId tabletActor,
        TActorId recepient,
        ui64 tabletId,
        const NPersQueue::TTopicConverterPtr& topicConverter,
        const TPartitionId& partition,
        const TString& user,
        const TTabletCountersBase& counters
    );


protected:
    void InitCountersImpl(const TActorContext& ctx) override;
    TString LimiterDescription() const override;
    THolder<NAccountQuoterEvents::TEvCounters> MakeCountersUpdateEvent() override;

private:
    static TQuoterParams GetQuoterParams(const TString& user);
    const TString User;
    TString ConsumerPath;
};


class TAccountWriteQuoter : public TBasicAccountQuoter {
private:
static const TString WRITE_QUOTA_ROOT_PATH;

static TQuoterParams CreateQuoterParams(const NKikimrPQ::TPQConfig& pqConfig,
                                        NPersQueue::TTopicConverterPtr topicConverter,
                                        ui64 tabletId, const TActorContext& ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType();

    TAccountWriteQuoter(TActorId tabletActor, TActorId recepient, ui64 tabletId,
            const NPersQueue::TTopicConverterPtr& topicConverter, const TPartitionId& partition,
            const TTabletCountersBase& counters, const TActorContext& ctx);

protected:
    void InitCountersImpl(const TActorContext& ctx) override;
    TString LimiterDescription() const override;
    THolder<NAccountQuoterEvents::TEvCounters> MakeCountersUpdateEvent() override;
};

}// NPQ
}// NKikimr
