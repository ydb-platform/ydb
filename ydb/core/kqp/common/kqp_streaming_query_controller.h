#pragma once

#include <ydb/core/kqp/common/kqp_user_request_context.h>
#include <ydb/core/kqp/common/simple/kqp_event_ids.h>
#include <ydb/core/protos/kqp.pb.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

namespace NKikimr::NKqp {

// Contract between the data executer and the streaming query runtime:
// graph patching on restart, checkpointing and nodes monitoring.
//
// The executer owns the call sites and knows nothing about the implementation,
// which lives in kqp/federated_query/streaming and is injected through
// TKqpFederatedQuerySetup::StreamingQueryControllerFactory.
//
// All methods are called from the executer actor context; side actors are
// registered and addressed on behalf of the executer.
class IKqpStreamingQueryController {
public:
    struct TStartedActors {
        NActors::TActorId CheckpointCoordinator;
        NActors::TActorId NodesManager;
    };

    struct TTaskInfo {
        ui64 Id = 0;
        bool IsCheckpointingEnabled = false;
        bool IsIngress = false;
        bool IsEgress = false;
        bool HasState = false;
        NActors::TActorId ActorId;
    };

    virtual ~IKqpStreamingQueryController() = default;

    // Resolve phase: true if the executer has to call StartPrepare() and wait
    // for TEvStreamingQueryPrepared before Execute().
    virtual bool NeedsPrepare() const = 0;

    // Starts asynchronous preparation. secureParams are resolved tokens of
    // external sources.
    virtual void StartPrepare(THashMap<TString, TString> secureParams) = 0;

    // Returns the saved physical graph the executer should restore tasks from.
    // May differ from the graph passed to the factory: it is never patched in place.
    virtual std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> GetGraphToRestore(
        const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, ui32 usableThreadsPerNode) = 0;

    // Starts side actors after the tasks graph is built (and saved, if requested).
    virtual TStartedActors Start(std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> graph) = 0;

    // Called once all compute actors are started. checkpointsEnabled is false
    // if the graph turned out to have no checkpointed ingress.
    virtual void OnComputeActorsStarted(TVector<TTaskInfo> tasks, bool checkpointsEnabled) = 0;

    // Events sent to the executer by the actors started above.
    virtual bool IsOwnEvent(const NActors::IEventHandle& ev) const = 0;
    virtual void Handle(TAutoPtr<NActors::IEventHandle>& ev) = 0;

    // Stops all started actors.
    virtual void Terminate() = 0;
};

class IKqpStreamingQueryControllerFactory {
public:
    using TPtr = std::shared_ptr<IKqpStreamingQueryControllerFactory>;

    struct TSettings {
        NActors::TActorId ExecuterId;
        ui64 TxId = 0;
        TString Database;
        TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
        TIntrusivePtr<TUserRequestContext> UserRequestContext;
        ::NMonitoring::TDynamicCounterPtr KqpCounters;
        // Saved graph of a restarted query, nullptr on the first run.
        std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> Graph;
        bool SaveGraph = false;
        bool HasPqSources = false;
    };

    virtual ~IKqpStreamingQueryControllerFactory() = default;

    // Returns nullptr if the request is not a streaming query.
    virtual std::unique_ptr<IKqpStreamingQueryController> Create(TSettings settings) const = 0;
};

struct TEvStreamingQueryPrepared : public NActors::TEventLocal<TEvStreamingQueryPrepared,
    TKqpExecuterEvents::EvStreamingQueryPrepared>
{
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
    NYql::TIssues Issues;
};

} // namespace NKikimr::NKqp
