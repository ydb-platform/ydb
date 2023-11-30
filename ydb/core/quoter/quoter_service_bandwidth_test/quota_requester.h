#pragma once
#include "options.h"
#include "server.h"

#include <ydb/core/quoter/public/quoter.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <random>

namespace NKikimr {

class TRequestDistribution {
public:
    TRequestDistribution(std::seed_seq& seedSeq, size_t requestRate);

    TDuration GetNextRequestTimeDelta();

private:
    std::mt19937_64 RandomEngine;
    std::uniform_real_distribution<> Distrib;
    double LambdaCoefficient;
};

class TBaseQuotaRequester : public NActors::TActorBootstrapped<TBaseQuotaRequester> {
public:
    TBaseQuotaRequester(const TOptions& opts, TRequestStats& stats, TActorId parent);

    void Bootstrap(const NActors::TActorContext& ctx);

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvQuota::TEvClearance, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        }
    }

protected:
    virtual THolder<TEvQuota::TEvRequest> MakeQuoterRequest() = 0;
    void RequestQuota();
    void SleepUntilNextRequest(TDuration duration);

    void Handle(TEvQuota::TEvClearance::TPtr&);
    void Handle(TEvents::TEvWakeup::TPtr&);

    void PassAway() override;

protected:
    const TOptions& Opts;
    TRequestStats& Stats;
    const TActorId Parent;

    TInstant StartTime;
    TRequestDistribution Distribution;
    TInstant QuotaRequestTime;
    TDuration QuotaRequestDelta;
    bool WaitingForDeadlinedRequests = false;
};

class TKesusQuotaRequester : public TBaseQuotaRequester {
public:
    TKesusQuotaRequester(const TOptions& opts, TRequestStats& stats, TActorId parent, size_t kesusIndex, size_t resourceIndex);

    THolder<TEvQuota::TEvRequest> MakeQuoterRequest() override;

private:
    TString KesusPath;
    TString ResourcePath;
};

class TLocalResourceQuotaRequester : public TBaseQuotaRequester {
public:
    TLocalResourceQuotaRequester(const TOptions& opts, TRequestStats& stats, TActorId parent, size_t resourceIndex);

    THolder<TEvQuota::TEvRequest> MakeQuoterRequest() override;

private:
    ui64 ResourceId;
};

} // namespace NKikimr
