#include "gc.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NKikimrServices::STREAMS_STORAGE_SERVICE

namespace NFq {

using namespace NActors;
using namespace NThreading;

using NYql::TIssues;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TContext : public TThrRefBase {
    enum EStage {
        StageOk,
        StageFailed,
    };

    TActorSystem* ActorSystem;
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;

    NActors::TActorId CheckpointCoordinatorId;
    TCoordinatorId CoordinatorId;
    TCheckpointId UpperBound;
    TActorId StorageProxy;
    ui64 Cookie;

    EStage Stage = StageOk;

    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr Success;
    ::NMonitoring::TDynamicCounters::TCounterPtr Inflight;

    TContext(TActorSystem* actorSystem,
            const TCheckpointStoragePtr& checkpointStorage,
            const TStateStoragePtr& stateStorage,
            NActors::TActorId checkpointCoordinatorId,
            const TCoordinatorId& coordinatorId,
            const TCheckpointId& upperBound,
            const TActorId& storageProxy,
            ui64 cookie,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& Errors,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& Success,
            const ::NMonitoring::TDynamicCounters::TCounterPtr& inflight)
        : ActorSystem(actorSystem)
        , CheckpointStorage(checkpointStorage)
        , StateStorage(stateStorage)
        , CheckpointCoordinatorId(checkpointCoordinatorId)
        , CoordinatorId(coordinatorId)
        , UpperBound(upperBound)
        , StorageProxy(storageProxy)
        , Cookie(cookie)
        , Errors(Errors)
        , Success(Success)
        , Inflight(inflight)
    {
    }
};

using TContextPtr = TIntrusivePtr<TContext>;

////////////////////////////////////////////////////////////////////////////////

class TActorGC : public TActorBootstrapped<TActorGC> {
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;
    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr Success;
    ::NMonitoring::TDynamicCounters::TCounterPtr Inflight;

public:
    TActorGC(const TCheckpointStoragePtr& checkpointStorage,
             const TStateStoragePtr& stateStorage,
             const ::NMonitoring::TDynamicCounterPtr& counters);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_GC_ACTOR";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckpointStorage::TEvNewCheckpointSucceeded, Handle);
    )

    void Handle(TEvCheckpointStorage::TEvNewCheckpointSucceeded::TPtr& ev);
    static void SendGcFinished(TIntrusivePtr<TContext> context);
};

TActorGC::TActorGC(const TCheckpointStoragePtr& checkpointStorage,
                   const TStateStoragePtr& stateStorage,
                   const ::NMonitoring::TDynamicCounterPtr& counters)
    : CheckpointStorage(checkpointStorage)
    , StateStorage(stateStorage)
    , Errors(counters->GetCounter("Errors", true))
    , Success(counters->GetCounter("Success", true))
    , Inflight(counters->GetCounter("Inflight"))
{
}

void TActorGC::Bootstrap()
{
    Become(&TActorGC::StateFunc);

    YDB_LOG_INFO("Successfully bootstrapped storage GC",
        {"actorId", SelfId()});
}

void TActorGC::Handle(TEvCheckpointStorage::TEvNewCheckpointSucceeded::TPtr& ev)
{
    const auto* event = ev->Get();
    const auto& graphId = event->CoordinatorId.GraphId;
    const auto& checkpointUpperBound = event->CheckpointId;

    YDB_LOG_DEBUG("GC received upperbound checkpoint for graph",
        {"checkpointUpperBound", checkpointUpperBound},
        {"graphId", graphId});

    const TActorId storageProxy = ev->Sender;

    // we need to:
    // 1. Mark checkpoints as GC and continue only if succeeded
    // 2. Delete states of marked checkpoints and continue only if succeeded
    // 3. Delete marked checkpoints

    auto context = MakeIntrusive<TContext>(
        TActivationContext::ActorSystem(),
        CheckpointStorage,
        StateStorage,
        event->CheckpointCoordinatorId,
        event->CoordinatorId,
        checkpointUpperBound,
        storageProxy,
        event->Cookie,
        Errors,
        Success,
        Inflight);

    Inflight->Inc();

    if (event->Type != NYql::NDqProto::CHECKPOINT_TYPE_SNAPSHOT) {
        YDB_LOG_DEBUG("GC skip increment checkpoint for graph",
            {"graphId", graphId});
        SendGcFinished(context);
        return;
    }

    // 1-2.
    CheckpointStorage->MarkCheckpointsGC(graphId, checkpointUpperBound)
        .Apply(
            [context] (const TFuture<TIssues>& future) {
                auto issues = future.GetValue();
                if (!issues.Empty()) {
                    TStringStream ss;
                    ss << "GC failed to mark checkpoints of graph '" << context->CoordinatorId.GraphId
                        << "' up to " << context->UpperBound << ", issues:";
                    issues.PrintTo(ss);
                    YDB_LOG_DEBUG_CTX(*context->ActorSystem, ss.Str());
                    context->Stage = TContext::StageFailed;
                    return future;
                }

                return context->StateStorage->DeleteCheckpoints(context->CoordinatorId.GraphId, context->UpperBound);
            })
        .Apply(         // 2-3. check StateStorage->DeleteCheckpoints and if OK DeleteMarkedCheckpoints
            [context] (const TFuture<TIssues>& future) {
                if (context->Stage == TContext::StageFailed) {
                    return future;
                }
                auto issues = future.GetValue();
                if (!issues.Empty()) {
                    TStringStream ss;
                    ss << "GC failed to delete states of checkpoints of graph '" << context->CoordinatorId.GraphId
                        << "' up to " << context->UpperBound << ", issues:";
                    issues.PrintTo(ss);
                    YDB_LOG_DEBUG_CTX(*context->ActorSystem, ss.Str());
                    context->Stage = TContext::StageFailed;
                    return future;
                }
                return context->CheckpointStorage->DeleteMarkedCheckpoints(context->CoordinatorId.GraphId, context->UpperBound);
            })
        .Subscribe(         // Final step: log result and always notify StorageProxy that GC cycle is done.
            [context] (const TFuture<TIssues>& future) {
                if (context->Stage == TContext::StageFailed) {
                    context->Errors->Inc();
                    SendGcFinished(context);
                    return;
                }

                auto issues = future.GetValue();
                if (!issues.Empty()) {
                    TStringStream ss;
                    ss << "GC failed to delete marked checkpoints of graph '" << context->CoordinatorId.GraphId
                        << "' up to " << context->UpperBound << ", issues:";
                    issues.PrintTo(ss);
                    YDB_LOG_DEBUG_CTX(*context->ActorSystem, ss.Str());
                    context->Errors->Inc();
                    return;
                }
                YDB_LOG_DEBUG_CTX(*context->ActorSystem, 
                    "GC deleted checkpoints of graph '" << context->CoordinatorId.GraphId << "' up to " << context->UpperBound);
                context->Success->Inc();
                SendGcFinished(context);
            });
}

void TActorGC::SendGcFinished(TIntrusivePtr<TContext> context) {
    YDB_LOG_DEBUG("Send TEvGcFinished to StorageProxy",
        {"graphId", context->CoordinatorId.GraphId});
    context->ActorSystem->Send(context->StorageProxy,
        new TEvCheckpointStorage::TEvGcFinished(
            context->CheckpointCoordinatorId,
            context->CoordinatorId,
            context->UpperBound,
            context->Cookie));
    context->Inflight->Dec();
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewGC(
    const TCheckpointStorageSettings::TGcSettings&,
    const TCheckpointStoragePtr& checkpointStorage,
    const TStateStoragePtr& stateStorage,
    const ::NMonitoring::TDynamicCounterPtr& counters)
{
    return std::unique_ptr<NActors::IActor>(new TActorGC(checkpointStorage, stateStorage, counters));
}

} // namespace NFq
