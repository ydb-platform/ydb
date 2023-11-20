#include "quota_requester.h"
#include <ydb/core/quoter/public/quoter.h>

#include <cmath>

namespace NKikimr {

TRequestDistribution::TRequestDistribution(std::seed_seq& seedSeq, size_t requestRate)
    : RandomEngine(seedSeq)
    , Distrib(0.0, 1.0)
    , LambdaCoefficient(-1000000.0 / static_cast<double>(requestRate))
{
}

TDuration TRequestDistribution::GetNextRequestTimeDelta() {
    double rand = Distrib(RandomEngine);
    while (!rand) { // 0.0 is not OK
        rand = Distrib(RandomEngine);
    }
    return TDuration::MicroSeconds(static_cast<ui64>(LambdaCoefficient * std::log(rand)));
}

TBaseQuotaRequester::TBaseQuotaRequester(const TOptions& opts, TRequestStats& stats, TActorId parent)
    : Opts(opts)
    , Stats(stats)
    , Parent(parent)
    , Distribution(Opts.SeedSeq, Opts.RequestRate)
{
}

void TBaseQuotaRequester::Bootstrap(const NActors::TActorContext&) {
    Become(&TBaseQuotaRequester::StateFunc);

    QuotaRequestTime = StartTime = TActivationContext::Now();
    SleepUntilNextRequest(QuotaRequestDelta = Distribution.GetNextRequestTimeDelta());
}

void TBaseQuotaRequester::PassAway() {
    Send(Parent, new TEvents::TEvWakeup());
    TActorBootstrapped::PassAway();
}

void TBaseQuotaRequester::Handle(TEvQuota::TEvClearance::TPtr& ev) {
    ++Stats.ResponsesCount;
    switch (ev->Get()->Result) {
    case TEvQuota::TEvClearance::EResult::Deadline:
        ++Stats.DeadlineResponses;
        break;
    case TEvQuota::TEvClearance::EResult::Success:
        ++Stats.OkResponses;
        break;
    default:
        Y_ABORT("Error result");
    }
}

void TBaseQuotaRequester::Handle(TEvents::TEvWakeup::TPtr&) {
    if (TActivationContext::Now() - StartTime < Opts.TestTime) {
        RequestQuota();
        while (true) {
            QuotaRequestDelta = Distribution.GetNextRequestTimeDelta();
            const TInstant now = TActivationContext::Now();
            const TDuration passed = now - QuotaRequestTime;
            if (passed >= QuotaRequestDelta) {
                RequestQuota();
            } else {
                SleepUntilNextRequest(QuotaRequestDelta - passed);
                break;
            }
        }
    } else {
        if (WaitingForDeadlinedRequests || !Opts.QuotaRequestDeadline) {
            PassAway();
        } else {
            WaitingForDeadlinedRequests = true;
            Schedule(Opts.QuotaRequestDeadline, new TEvents::TEvWakeup());
        }
    }
}

void TBaseQuotaRequester::RequestQuota() {
    Send(MakeQuoterServiceID(), MakeQuoterRequest());
    ++Stats.RequestsCount;
    QuotaRequestTime += QuotaRequestDelta;
}

void TBaseQuotaRequester::SleepUntilNextRequest(TDuration duration) {
    Schedule(duration, new TEvents::TEvWakeup());
}

TKesusQuotaRequester::TKesusQuotaRequester(const NKikimr::TOptions& opts, NKikimr::TRequestStats& stats, TActorId parent, size_t kesusIndex, size_t resourceIndex)
    : TBaseQuotaRequester(opts, stats, parent)
    , KesusPath(TTestServer::GetKesusPath(kesusIndex))
    , ResourcePath(TTestServer::GetKesusResource(resourceIndex))
{
}

THolder<TEvQuota::TEvRequest> TKesusQuotaRequester::MakeQuoterRequest() {
    TVector<TEvQuota::TResourceLeaf> reqs = {
        TEvQuota::TResourceLeaf(KesusPath, ResourcePath, 1.0)
    };
    return MakeHolder<TEvQuota::TEvRequest>(TEvQuota::EResourceOperator::And, std::move(reqs), Opts.QuotaRequestDeadline);
}

TLocalResourceQuotaRequester::TLocalResourceQuotaRequester(const NKikimr::TOptions& opts, NKikimr::TRequestStats& stats, TActorId parent, size_t resourceIndex)
    : TBaseQuotaRequester(opts, stats, parent)
    , ResourceId(TEvQuota::TResourceLeaf::MakeTaggedRateRes(resourceIndex, Opts.LocalResourceQuotaRate))
{
}

THolder<TEvQuota::TEvRequest> TLocalResourceQuotaRequester::MakeQuoterRequest() {
    TVector<TEvQuota::TResourceLeaf> reqs = {
        TEvQuota::TResourceLeaf(TEvQuota::TResourceLeaf::QuoterSystem, ResourceId, 1.0)
    };
    return MakeHolder<TEvQuota::TEvRequest>(TEvQuota::EResourceOperator::And, std::move(reqs), Opts.QuotaRequestDeadline);
}

} // namespace NKikimr
