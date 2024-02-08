#pragma once
#include <ydb/library/actors/core/event_local.h>
#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <ydb/library/time_series_vec/time_series_vec.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr {

struct TEvQuota {

    enum EEventSpaceQuoter {
         ES_QUOTA = 4177  // must be in sync with ydb/core/base/events.h
    };

    enum EEv {
        EvRequest = EventSpaceBegin(EEventSpaceQuoter::ES_QUOTA),
        EvCancelRequest,
        EvClearance,
        EvRpcTimeout,

        EvProxyRequest = EvRequest + 512,
        EvProxyStats,
        EvProxyCloseSession,

        EvProxySession = EvRequest + 2 * 512,
        EvProxyUpdate,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(EEventSpaceQuoter::ES_QUOTA), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_QUOTA)");

    struct TResourceLeaf {
        static constexpr ui64 QuoterSystem = Max<ui64>(); // use as quoter id for some embedded quoters
        static constexpr ui64 ResourceForbid = 0; // never allow, just wait for deadline and forbid
        static constexpr ui64 ResourceNocheck = 1; // allow everything, w/o any check

        static ui64 MakeTaggedRateRes(ui32 tag, ui32 rate);
        // todo: tagged local rate-limiters

        const ui64 QuoterId;
        const ui64 ResourceId;
        const TString Quoter;
        const TString Resource;

        const ui64 Amount;
        const bool IsUsedAmount;

        TResourceLeaf(const TResourceLeaf&) = default;

        TResourceLeaf(ui64 quoterId, ui64 resourceId, ui64 amount, bool isUsedAmount = false)
            : QuoterId(quoterId)
            , ResourceId(resourceId)
            , Amount(amount)
            , IsUsedAmount(isUsedAmount)
        {}

        TResourceLeaf(const TString &quoter, const TString &resource, ui64 amount, bool isUsedAmount = false)
            : QuoterId(0)
            , ResourceId(0)
            , Quoter(quoter)
            , Resource(resource)
            , Amount(amount)
            , IsUsedAmount(isUsedAmount)
        {}
    };

    enum class EResourceOperator {
        Unknown,
        And,
        OrBoth,
        OrAny,
    };

    // event cookie + sender actorid would be used as id for cancel requests
    struct TEvRequest : public NActors::TEventLocal<TEvRequest, EvRequest> {
        const EResourceOperator Operator;
        const TVector<TResourceLeaf> Reqs;
        TDuration Deadline;

        TEvRequest(EResourceOperator op, TVector<TResourceLeaf> &&reqs, TDuration deadline)
            : Operator(op)
            , Reqs(std::move(reqs))
            , Deadline(deadline)
        {}
    };

    struct TEvClearance : public NActors::TEventLocal<TEvClearance, EvClearance> {
        // clearance result, could be success or deadline, or one of error
        enum class EResult {
            GenericError,
            UnknownResource,
            Deadline,
            Success = 128
        } Result;

        TEvClearance(EResult result)
            : Result(result)
        {}
    };

    struct TEvRpcTimeout : public NActors::TEventLocal<TEvRpcTimeout, EvRpcTimeout> {
        const TString Quoter;
        const TString Resource;

        TEvRpcTimeout(TString quoter, TString resource)
            : Quoter(quoter)
            , Resource(resource)
        {}
    };

    // when cookie present - cancel one request
    // when cookie omitted - cancel all requests from sender
    struct TEvCancelRequest : public NActors::TEventLocal<TEvClearance, EvCancelRequest> {};

    // b/w service and quoter proxy

    // initial request
    struct TEvProxyRequest : public NActors::TEventLocal<TEvProxyRequest, EvProxyRequest> {
        const TString Resource;

        TEvProxyRequest(const TString &resource)
            : Resource(resource)
        {}
    };

    enum class EStatUpdatePolicy {
        Never,
        OnActivity, // on queue and on allocation
        EveryTick,
        EveryActiveTick,
    };

    struct TEvProxySession : public NActors::TEventLocal<TEvProxySession, EvProxySession> {
        enum EResult {
            GenericError,
            UnknownResource,
            Success = 128
        } Result;

        const ui64 QuoterId;
        const ui64 ResourceId;
        const TString Resource;

        const TDuration TickSize;
        const EStatUpdatePolicy StatUpdatePolicy;

        TEvProxySession(EResult result, ui64 quoterId, ui64 resourceId, const TString &resource, TDuration tickSize, EStatUpdatePolicy statUpdatePolicy)
            : Result(result)
            , QuoterId(quoterId)
            , ResourceId(resourceId)
            , Resource(resource)
            , TickSize(tickSize)
            , StatUpdatePolicy(statUpdatePolicy)
        {}
    };

    struct TProxyStat {
        const ui64 ResourceId;
        const ui64 Tick;

        const double Consumed;
        const TTimeSeriesMap<double> History;
        const ui64 QueueSize;
        const double QueueWeight;
        const double ExpectedRate;
        const double Cap;

        TProxyStat(ui64 id, ui64 tick, double consumed, const TTimeSeriesMap<double>& history, ui64 queueSize, double queueWeight, double rate, double cap)
            : ResourceId(id)
            , Tick(tick)
            , Consumed(consumed)
            , History(history)
            , QueueSize(queueSize)
            , QueueWeight(queueWeight)
            , ExpectedRate(rate)
            , Cap(cap)
        {}

        TProxyStat(const TProxyStat &x) = default;
    };

    struct TEvProxyStats : public NActors::TEventLocal<TEvProxyStats, EvProxyStats> {
        const TDeque<TProxyStat> Stats;

        TEvProxyStats(TDeque<TProxyStat> &&stats)
            : Stats(std::move(stats))
        {}
    };

    struct TEvProxyCloseSession : public NActors::TEventLocal<TEvProxyCloseSession, EvProxyCloseSession> {
        const TString Resource;
        const ui64 ResourceId;

        TEvProxyCloseSession(const TString &resource, ui64 resourceId)
            : Resource(resource)
            , ResourceId(resourceId)
        {}
    };

    enum class ETickPolicy : ui32 {
        Front, // all quota could be used right from tick start
        Sustained, // quota must be used on sustained rate
        Ahead, // sustained + could go negative
    };

    struct TUpdateTick {
        ui32 Channel;
        ui32 Ticks;
        double Rate;
        ETickPolicy Policy;

        TUpdateTick()
            : Channel(0)
            , Ticks(0)
            , Rate(0.0)
            , Policy(ETickPolicy::Sustained)
        {}

        TUpdateTick(ui32 channel, ui32 ticks, double rate, ETickPolicy policy)
            : Channel(channel)
            , Ticks(ticks)
            , Rate(rate)
            , Policy(policy)
        {}

        TUpdateTick(const TUpdateTick &) = default;
        TUpdateTick& operator=(const TUpdateTick &) noexcept = default;
    };

    enum class EUpdateState {
        Normal,
        Evict,
        Broken
    };

    struct TProxyResourceUpdate {
        const ui64 ResourceId;
        const double SustainedRate;
        const TVector<TUpdateTick> Update;
        const EUpdateState ResourceState;

        TProxyResourceUpdate(ui64 resourceId, double sustainedRate, TVector<TUpdateTick> &&update, EUpdateState resState)
            : ResourceId(resourceId)
            , SustainedRate(sustainedRate)
            , Update(std::move(update))
            , ResourceState(resState)
        {}
    };

    struct TEvProxyUpdate : public NActors::TEventLocal<TEvProxyUpdate, EvProxyUpdate> {
        const ui64 QuoterId;
        TVector<TProxyResourceUpdate> Resources;
        const EUpdateState QuoterState;

        TEvProxyUpdate(ui64 quoterId, EUpdateState quoterState)
            : QuoterId(quoterId)
            , QuoterState(quoterState)
        {}
    };

    // interface b/w proxy and proxy backend is private, so not defined here
};

//
NActors::TActorId MakeQuoterServiceID();

}
