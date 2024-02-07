#pragma once

#include "ydb/library/yql/dq/common/dq_common.h"

#include <ydb/library/actors/core/actor.h>

namespace NYql::NDq {

struct TDqComputeStorageActorEvents {
    enum {
        EvPut = EventSpaceBegin(NActors::TEvents::EEventSpace::ES_USERSPACE) + 30000,
        EvGet
    };
};

struct TEvPut : NActors::TEventLocal<TEvPut, TDqComputeStorageActorEvents::EvPut> {
    TEvPut(TRope&& blob, NThreading::TPromise<ui64>&& promise)
        : Blob_(std::move(blob))
        , Promise_(std::move(promise))
    { }

    TRope Blob_;
    NThreading::TPromise<ui64> Promise_;
};

struct TEvGet : NActors::TEventLocal<TEvGet, TDqComputeStorageActorEvents::EvGet> {
    TEvGet(ui64 key, NThreading::TPromise<TRope>&& promise)
        : Key_(key)
        , Promise_(std::move(promise))
    { }

    ui64 Key_;
    NThreading::TPromise<TRope> Promise_;
};

// This class will be refactored to be the Actor part of the spiller
class IDqComputeStorageActor
{
public:
    using TPtr = std::shared_ptr<IDqComputeStorageActor>;
    using TKey = ui64;

    virtual ~IDqComputeStorageActor() = default;

    virtual NActors::IActor* GetActor() = 0;
};

IDqComputeStorageActor* CreateDqComputeStorageActor(TTxId txId, const TString& spillerName, std::function<void()> wakeupCallback, NActors::TActorSystem* actorSystem);

} // namespace NYql::NDq
