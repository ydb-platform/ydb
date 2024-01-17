#pragma once
#include "defs.h"

#include "events.h"
#include <ydb/core/ymq/base/table_info.h>
#include <ydb/core/protos/sqs.pb.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/kesus/tablet/events.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event_pb.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>

namespace NKikimr::NSQS {

extern const TString QUOTER_KESUS_NAME;
extern const TString RPS_QUOTA_NAME;

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeExecuteEvent(const TString& query);

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeCreateTableEvent(const TString& root,
                         const TTable& table,
                         size_t queueShardsCount = 0);

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeDeleteTableEvent(const TString& root,
                         const TTable& table);

THolder<TEvTxUserProxy::TEvProposeTransaction>
    MakeRemoveDirectoryEvent(const TString& root, const TString& name);

// Create actor that calls AddQuoterResource and handles pipe errors and retries
TActorId RunAddQuoterResource(ui64 quoterSchemeShardId, ui64 quoterPathId, const NKikimrKesus::TEvAddQuoterResource& cmd, const TString& requestId);
TActorId RunAddQuoterResource(const TString& quoterPath, const NKikimrKesus::TEvAddQuoterResource& cmd, const TString& requestId);
TActorId RunDeleteQuoterResource(const TString& quoterPath, const NKikimrKesus::TEvDeleteQuoterResource& cmd, const TString& requestId);

inline TIntrusivePtr<TTransactionCounters> GetTransactionCounters(const TIntrusivePtr<TUserCounters>& userCounters) {
    if (userCounters) {
        return userCounters->GetTransactionCounters();
    }
    return nullptr;
}

class TCreateUserSchemaActor
    : public TActorBootstrapped<TCreateUserSchemaActor>
{
public:
    TCreateUserSchemaActor(const TString& root, const TString& userName, const TActorId& sender, const TString& requestId, TIntrusivePtr<TUserCounters> userCounters);
    ~TCreateUserSchemaActor();

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    void NextAction();

    void Process();

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
            hFunc(NKesus::TEvKesus::TEvAddQuoterResourceResult, HandleAddQuoterResource);
            cFunc(TEvPoisonPill::EventType, PassAway);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);
    void HandleAddQuoterResource(NKesus::TEvKesus::TEvAddQuoterResourceResult::TPtr& ev);

    THolder<TEvTxUserProxy::TEvProposeTransaction> MakeMkDirRequest(const TString& root, const TString& dirName);

    void AddRPSQuota();

    void PassAway() override;

private:
    enum class ECreating : int {
        MakeRootSqsDirectory = -1, // optional state
        MakeDirectory = 0,
        Quoter,
        RPSQuota,
        Finish,
    };

    const TString Root_;
    const TString UserName_;
    const TActorId Sender_;
    int SI_;
    const TString RequestId_;
    bool CreateRootSqsDirAttemptWasMade_ = false;
    TIntrusivePtr<TUserCounters> UserCounters_;
    std::pair<ui64, ui64> KesusPathId_ = {}; // SchemeShardTableId, PathId for quoter kesus
    TActorId AddQuoterResourceActor_;
};

class TDeleteUserSchemaActor
    : public TActorBootstrapped<TDeleteUserSchemaActor>
{
public:
     TDeleteUserSchemaActor(const TString& root, const TString& name, const TActorId& sender, const TString& requestId, TIntrusivePtr<TUserCounters> userCounters);
    ~TDeleteUserSchemaActor();

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    void NextAction();

    void Process();

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

    void SkipQuoterStages();

private:
    enum class EDeleting : ui32 {
        RemoveQuoter = 0,
        RemoveDirectory,
        Finish
    };

    const TString Root_;
    const TString Name_;
    const TActorId Sender_;
    ui32 SI_;
    const TString RequestId_;
    TIntrusivePtr<TUserCounters> UserCounters_;
};

class TAtomicCounterActor
    : public TActorBootstrapped<TAtomicCounterActor>
{
public:
     TAtomicCounterActor(const TActorId& sender, const TString& rootPath, const TString& requestId);
    ~TAtomicCounterActor();

    void Bootstrap();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::SQS_ACTOR;
    }

private:
    STATEFN(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TSqsEvents::TEvExecuted, HandleExecuted);
        }
    }

    void HandleExecuted(TSqsEvents::TEvExecuted::TPtr& ev);

private:
    const TActorId Sender_;
    const TString RootPath_;
    const TString RequestId_;
};

} // namespace NKikimr::NSQS
