#pragma once
#include "defs.h"
#include <ydb/core/ymq/actor/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSQS {
    
TDuration RandomRetentionPeriod();

class TRetentionActor : public TActorBootstrapped<TRetentionActor> {
public:
    TRetentionActor(const TQueuePath& queuePath, ui32 tablesFormat, const TActorId& queueLeader, bool useCPUOptimization);

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_RETENTION_BACKGROUND_ACTOR;
    }

private:
    void SetRetentionBoundary();

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandlePoisonPill(TEvPoisonPill::TPtr&);
    void HandleWakeup();

private:
    STATEFN(StateFunc);

private:
    const TQueuePath QueuePath_;
    const ui32 TablesFormat_;
    const TString RequestId_;
    const TActorId QueueLeader_;
    const bool UseCPUOptimization_;
};

} // namespace NKikimr::NSQS
