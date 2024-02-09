#pragma once

#include "compute_storage.h"

#include <ydb/library/yql/minikql/computation/mkql_spiller_factory.h>

namespace NYql::NDq {

using namespace NActors;

class TDqSpillerFactory : public NKikimr::NMiniKQL::ISpillerFactory
{
public:
    TDqSpillerFactory(TTxId txId, TActorSystem* actorSystem, std::function<void()> wakeUpCallback) 
        : ActorSystem_(actorSystem),
        TxId_(txId),
        WakeUpCallback_(wakeUpCallback)
    {
    }

    NKikimr::NMiniKQL::ISpiller::TPtr CreateSpiller() override {
        return std::make_shared<TDqComputeStorage>(TxId_, WakeUpCallback_, ActorSystem_);
    }

private:
    TActorSystem* ActorSystem_;
    TTxId TxId_;
    std::function<void()> WakeUpCallback_;
};

} // namespace NYql::NDq
