#pragma once

#include "compute_storage.h"

#include <ydb/library/yql/minikql/computation/mkql_spiller_factory.h>

namespace NYql::NDq {

using namespace NActors;

class TDqSpillerFactory : public NKikimr::NMiniKQL::ISpillerFactory
{
public:
    TDqSpillerFactory(TTxId txId, TActorSystem* actorSystem, TWakeUpCallback wakeUpCallback, TErrorCallback errorCallback) 
        : ActorSystem_(actorSystem),
        TxId_(txId),
        WakeUpCallback_(wakeUpCallback),
        ErrorCallback_(errorCallback)
    {
    }

    NKikimr::NMiniKQL::ISpiller::TPtr CreateSpiller() override {
        return std::make_shared<TDqComputeStorage>(TxId_, WakeUpCallback_, ErrorCallback_, ActorSystem_);
    }

private:
    TActorSystem* ActorSystem_;
    TTxId TxId_;
    TWakeUpCallback WakeUpCallback_;
    TErrorCallback ErrorCallback_;
};

} // namespace NYql::NDq
