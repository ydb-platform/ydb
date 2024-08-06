#pragma once

#include "spilling_counters.h"

#include "compute_storage.h"

#include <ydb/library/yql/minikql/computation/mkql_spiller_factory.h>

namespace NYql::NDq {

using namespace NActors;

class TDqSpillerFactory : public NKikimr::NMiniKQL::ISpillerFactory
{
public:
    TDqSpillerFactory(TTxId txId, TActorSystem* actorSystem, std::function<void()> wakeUpCallback, TIntrusivePtr<TSpillingCountersPerTaskRunner> spillingCounters) 
        : ActorSystem_(actorSystem),
        TxId_(txId),
        WakeUpCallback_(wakeUpCallback),
        SpillingCounters_(spillingCounters)
    {
    }

    NKikimr::NMiniKQL::ISpiller::TPtr CreateSpiller() override {
        return std::make_shared<TDqComputeStorage>(TxId_, WakeUpCallback_, SpillingCounters_, ActorSystem_);
    }

private:
    TActorSystem* ActorSystem_;
    TTxId TxId_;
    std::function<void()> WakeUpCallback_;
    TIntrusivePtr<TSpillingCountersPerTaskRunner> SpillingCounters_;
};

} // namespace NYql::NDq
