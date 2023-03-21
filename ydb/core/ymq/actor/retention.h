#pragma once
#include "defs.h"
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/protos/services.pb.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NSQS {

class TRetentionActor : public TActorBootstrapped<TRetentionActor> {
public:
    TRetentionActor(const TQueuePath& queuePath, ui32 tablesFormat, const TActorId& queueLeader);

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_RETENTION_BACKGROUND_ACTOR;
    }

private:
    TDuration RandomRetentionPeriod() const;

    void SetRetentionBoundary();

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void Handle(TSqsEvents::TEvChangeRetentionActiveCheck::TPtr& ev);
    void HandlePoisonPill(TEvPoisonPill::TPtr&);
    void HandleWakeup();

private:
    STATEFN(StateFunc);

private:
    const TQueuePath QueuePath_;
    const ui32 TablesFormat_;
    const TString RequestId_;
    const TActorId QueueLeader_;
    bool Active = true;
};

} // namespace NKikimr::NSQS
