#pragma once

#include "common.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>


namespace NKqpRun {

struct TQueryResponse {
    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr Response;
    std::vector<Ydb::ResultSet> ResultSets;
};

struct TQueryRequest {
    std::unique_ptr<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Event;
    ui32 TargetNode;
    ui64 ResultRowsLimit;
    ui64 ResultSizeLimit;
};

struct TEvPrivate {
    enum EEv : ui32 {
        EvStartAsyncQuery = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvAsyncQueryFinished,
        EvFinalizeAsyncQueryRunner,

        EvResourcesInfo,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    struct TEvStartAsyncQuery : public NActors::TEventLocal<TEvStartAsyncQuery, EvStartAsyncQuery> {
        TEvStartAsyncQuery(TQueryRequest request, NThreading::TPromise<void> startPromise)
            : Request(std::move(request))
            , StartPromise(startPromise)
        {}

        TQueryRequest Request;
        NThreading::TPromise<void> StartPromise;
    };

    struct TEvAsyncQueryFinished : public NActors::TEventLocal<TEvAsyncQueryFinished, EvAsyncQueryFinished> {
        TEvAsyncQueryFinished(ui64 requestId, TQueryResponse result)
            : RequestId(requestId)
            , Result(std::move(result))
        {}

        const ui64 RequestId;
        const TQueryResponse Result;
    };

    struct TEvFinalizeAsyncQueryRunner : public NActors::TEventLocal<TEvFinalizeAsyncQueryRunner, EvFinalizeAsyncQueryRunner> {
        explicit TEvFinalizeAsyncQueryRunner(NThreading::TPromise<void> finalizePromise)
            : FinalizePromise(finalizePromise)
        {}

        NThreading::TPromise<void> FinalizePromise;
    };

    struct TEvResourcesInfo : public NActors::TEventLocal<TEvResourcesInfo, EvResourcesInfo> {
        explicit TEvResourcesInfo(i32 nodeCount)
            : NodeCount(nodeCount)
        {}

        const i32 NodeCount;
    };
};

using TProgressCallback = std::function<void(const NKikimrKqp::TEvExecuterProgress&)>;

NActors::IActor* CreateRunScriptActorMock(TQueryRequest request, NThreading::TPromise<TQueryResponse> promise, TProgressCallback progressCallback);

NActors::IActor* CreateAsyncQueryRunnerActor(const TAsyncQueriesSettings& settings);

NActors::IActor* CreateResourcesWaiterActor(NThreading::TPromise<void> promise, i32 expectedNodeCount);

}  // namespace NKqpRun
