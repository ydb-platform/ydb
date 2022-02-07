#pragma once
#include "defs.h"
#include <ydb/core/ymq/actor/events.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/public/lib/scheme_types/scheme_type_id.h>

#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NSQS {

class TQueueMigrationActor : public TActorBootstrapped<TQueueMigrationActor> {
public:
    TQueueMigrationActor(const TString& userName, const TString& queueName, const TActorId& queueLeader, const TActorId& schemeCache, TIntrusivePtr<TQueueCounters> counters, TDuration waitBeforeMigration = TDuration::Zero());
    ~TQueueMigrationActor();

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_QUEUE_MIGRATION_ACTOR;
    }

private:
    STATEFN(StateFunc);

    void HandleWakeup(TEvWakeup::TPtr& ev);
    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandleMigrationDone(TSqsEvents::TEvMigrationDone::TPtr& ev);

    void SendReplyAndDie(bool ok);

    void GetQueueParams();
    void OnQueueParams(const TSqsEvents::TEvExecuted::TRecord& ev);

    void StartAltering();

    void RegisterAndWaitChildMigrationActor(IActor* child);

    void CheckAddColumn(const ui64 shard, const TString& tableName, const TString& columnName, NScheme::TTypeId type);
    void CheckAddColumn(const TString& tableName, const TString& columnName, NScheme::TTypeId type);

    TString GetTablePath(const ui64 shard, const TString& tableName);

private:
    const TString UserName;
    const TString QueueName;
    const TActorId QueueLeader;
    const TActorId SchemeCache;
    TIntrusivePtr<TQueueCounters> Counters;
    TDuration WaitBeforeMigration;
    bool IsFifoQueue = false;
    ui64 ShardsCount = 0;
    ui64 QueueVersion = 0;
    size_t WaitChildrenCount = 0;
    bool Answer = true;
};

} // namespace NKikimr::NSQS
