#pragma once

#include "common.h"

#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/tests/tools/kqprun/runlib/actors.h>

namespace NKqpRun {

struct TQueryResponse {
    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr Response;
    std::vector<Ydb::ResultSet> ResultSets;

    bool IsSuccess() const;
    Ydb::StatusIds::StatusCode GetStatus() const;
    TString GetError() const;
};

struct TQueryRequest {
    std::unique_ptr<NKikimr::NKqp::TEvKqp::TEvQueryRequest> Event;
    ui32 TargetNode;
    ui64 ResultRowsLimit;
    ui64 ResultSizeLimit;
    size_t QueryId;
};

struct TCreateSessionRequest {
    std::unique_ptr<NKikimr::NKqp::TEvKqp::TEvCreateSessionRequest> Event;
    ui32 TargetNode;
    TYdbSetupSettings::EVerbose VerboseLevel;
};

struct TWaitResourcesSettings {
    i32 ExpectedNodeCount;
    TYdbSetupSettings::EHealthCheck HealthCheckLevel;
    TDuration HealthCheckTimeout;
    TYdbSetupSettings::EVerbose VerboseLevel;
    TString Database;
};

using TProgressCallback = std::function<void(ui64 queryId, const NKikimrKqp::TEvExecuterProgress& executerProgress)>;

NActors::IActor* CreateRunScriptActorMock(TQueryRequest request, NThreading::TPromise<TQueryResponse> promise, TProgressCallback progressCallback);

NActors::IActor* CreateAsyncQueryRunnerActor(const NKikimrRun::TAsyncQueriesSettings& settings);

NActors::IActor* CreateResourcesWaiterActor(NThreading::TPromise<void> promise, const TWaitResourcesSettings& settings);

NActors::IActor* CreateSessionHolderActor(TCreateSessionRequest request, NThreading::TPromise<TString> openPromise, NThreading::TPromise<void> closePromise);

}  // namespace NKqpRun
