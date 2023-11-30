#pragma once
#include "defs.h"
#include <ydb/core/ymq/actor/events.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSQS {

class TCleanupActor : public TActorBootstrapped<TCleanupActor> {
public:
    enum class ECleanupType {
        Deduplication,
        Reads,
    };

    TCleanupActor(const TQueuePath& queuePath, ui32 tablesFormat, const TActorId& queueLeader, ECleanupType cleanupType);

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_CLEANUP_BACKGROUND_ACTOR;
    }

private:
    TDuration RandomCleanupPeriod();

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandlePoisonPill(TEvPoisonPill::TPtr&);
    void HandleWakeup();

    void RunCleanupQuery();

    EQueryId GetCleanupQueryId() const;

private:
    STATEFN(StateFunc);

private:
    const TQueuePath QueuePath_;
    const ui32 TablesFormat_;
    const TString RequestId_;
    const TActorId QueueLeader_;
    const ECleanupType CleanupType;
    TString KeyRangeStart;
};

} // namespace NKikimr::NSQS
