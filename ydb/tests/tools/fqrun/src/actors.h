#pragma once

#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/tests/tools/kqprun/runlib/actors.h>

namespace NFqRun {

struct TQueryRequest {
    std::unique_ptr<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest> Event;
    TDuration PingPeriod;
};

struct TQueryResponse {
    FederatedQuery::QueryMeta::ComputeStatus Status;
    NYql::TIssues Issues;
    NYql::TIssues TransientIssues;

    bool IsSuccess() const;
    TString GetStatus() const;
    TString GetError() const;
};

NActors::IActor* CreateAsyncQueryRunnerActor(const NKikimrRun::TAsyncQueriesSettings& settings);

}  // namespace NFqRun
