#include "gc.h"

#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/checkpoint_storage/events/events.h>
#include <ydb/core/fq/libs/actors/logging/log.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

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

    TString GraphId;
    TCheckpointId UpperBound;

    EStage Stage = StageOk;

    TContext(TActorSystem* actorSystem,
             const TCheckpointStoragePtr& checkpointStorage,
             const TStateStoragePtr& stateStorage,
             const TString& graphId,
             const TCheckpointId& upperBound)
        : ActorSystem(actorSystem)
        , CheckpointStorage(checkpointStorage)
        , StateStorage(stateStorage)
        , GraphId(graphId)
        , UpperBound(upperBound)
    {
    }
};

using TContextPtr = TIntrusivePtr<TContext>;

////////////////////////////////////////////////////////////////////////////////

class TActorGC : public TActorBootstrapped<TActorGC> {
    TCheckpointStoragePtr CheckpointStorage;
    TStateStoragePtr StateStorage;

public:
    TActorGC(const TCheckpointStoragePtr& checkpointStorage, const TStateStoragePtr& stateStorage);

    void Bootstrap();

    static constexpr char ActorName[] = "YQ_GC_ACTOR";

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvCheckpointStorage::TEvNewCheckpointSucceeded, Handle);
    )

    void Handle(TEvCheckpointStorage::TEvNewCheckpointSucceeded::TPtr& ev);
};

TActorGC::TActorGC(const TCheckpointStoragePtr& checkpointStorage, const TStateStoragePtr& stateStorage)
    : CheckpointStorage(checkpointStorage)
    , StateStorage(stateStorage)
{
}

void TActorGC::Bootstrap()
{
    Become(&TActorGC::StateFunc);

    LOG_STREAMS_STORAGE_SERVICE_INFO("Successfully bootstrapped storage GC " << SelfId());
}

void TActorGC::Handle(TEvCheckpointStorage::TEvNewCheckpointSucceeded::TPtr& ev)
{
    const auto* event = ev->Get();
    const auto& graphId = event->CoordinatorId.GraphId;
    const auto& checkpointUpperBound = event->CheckpointId;

    LOG_STREAMS_STORAGE_SERVICE_DEBUG("GC received upperbound checkpoint " << checkpointUpperBound
        << " for graph '" << graphId << "'");

    if (event->Type != NYql::NDqProto::TCheckpoint::EType::TCheckpoint_EType_SNAPSHOT) {
        LOG_STREAMS_STORAGE_SERVICE_DEBUG("GC skip increment checkpoint");
        return;
    }

    // we need to:
    // 1. Mark checkpoints as GC and continue only if succeeded
    // 2. Delete states of marked checkpoints and continue only if succeeded
    // 3. Delete marked checkpoints

    auto context = MakeIntrusive<TContext>(
        TActivationContext::ActorSystem(),
        CheckpointStorage,
        StateStorage,
        graphId,
        checkpointUpperBound);

    // 1-2.
    auto future = CheckpointStorage->MarkCheckpointsGC(graphId, checkpointUpperBound).Apply(
        [context] (const TFuture<TIssues>& future) {
            auto issues = future.GetValue();
            if (!issues.Empty()) {
                TStringStream ss;
                ss << "GC failed to mark checkpoints of graph '" << context->GraphId
                   << "' up to " << context->UpperBound << ", issues:";
                issues.PrintTo(ss);
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*context->ActorSystem, ss.Str());
                context->Stage = TContext::StageFailed;
                return future;
            }

            return context->StateStorage->DeleteCheckpoints(context->GraphId, context->UpperBound);
        });

    // 2-3. check StateStorage->DeleteCheckpoints and if OK DeleteMarkedCheckpoints
    future.Apply(
        [context] (const TFuture<TIssues>& future) {
            if (context->Stage == TContext::StageFailed) {
                return future;
            }

            auto issues = future.GetValue();
            if (!issues.Empty()) {
                TStringStream ss;
                ss << "GC failed to delete states of checkpoints of graph '" << context->GraphId
                   << "' up to " << context->UpperBound << ", issues:";
                issues.PrintTo(ss);
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*context->ActorSystem, ss.Str());
                context->Stage = TContext::StageFailed;
                return future;
            }

            return context->CheckpointStorage->DeleteMarkedCheckpoints(context->GraphId, context->UpperBound);
         });

    // this one just to debug log result
    future.Apply(
        [context] (const TFuture<TIssues>& future) {
            if (context->Stage == TContext::StageFailed) {
                return future;
            }

            auto issues = future.GetValue();
            if (!issues.Empty()) {
                TStringStream ss;
                ss << "GC failed to delete marked checkpoints of graph '" << context->GraphId
                   << "' up to " << context->UpperBound << ", issues:";
                issues.PrintTo(ss);
                LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*context->ActorSystem, ss.Str());
                context->Stage = TContext::StageFailed;
                return future;
            }
            TStringStream ss;
            ss << "GC deleted checkpoints of graph '" << context->GraphId
               << "' up to " << context->UpperBound;
            LOG_STREAMS_STORAGE_SERVICE_AS_DEBUG(*context->ActorSystem, ss.Str());
            return future;
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> NewGC(
    const NConfig::TCheckpointGcConfig&,
    const TCheckpointStoragePtr& checkpointStorage,
    const TStateStoragePtr& stateStorage)
{
    return std::unique_ptr<NActors::IActor>(new TActorGC(checkpointStorage, stateStorage));
}

} // namespace NFq
