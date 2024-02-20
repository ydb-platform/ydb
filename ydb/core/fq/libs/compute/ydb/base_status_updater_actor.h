#pragma once

#include "base_compute_actor.h"

#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/compute/common/utils.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

namespace NFq {

template<typename TDerived>
class TBaseStatusUpdaterActor : public TBaseComputeActor<TDerived> {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;

    TBaseStatusUpdaterActor(const NConfig::TCommonConfig& commonConfig, const ::NYql::NCommon::TServiceCounters& queryCounters, const TString& stepName)
        : TBase(queryCounters, stepName)
        , Compressor(commonConfig.GetQueryArtifactsCompressionMethod(), commonConfig.GetQueryArtifactsCompressionMinSize())
    {}

    TBaseStatusUpdaterActor(const NConfig::TCommonConfig& commonConfig, const ::NMonitoring::TDynamicCounterPtr& baseCounters, const TString& stepName)
        : TBase(baseCounters, stepName)
        , Compressor(commonConfig.GetQueryArtifactsCompressionMethod(), commonConfig.GetQueryArtifactsCompressionMinSize())
    {}

    void SetPingCounters(TComputeRequestCountersPtr pingCounters) {
        PingCounters = std::move(pingCounters);
    }

    void OnPingRequestStart() {
        if (!PingCounters) {
            return;
        }

        StartTime = TInstant::Now();
        PingCounters->InFly->Inc();
    }

    void OnPingRequestFinish(bool success) {
        if (!PingCounters) {
            return;
        }

        PingCounters->InFly->Dec();
        PingCounters->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
        if (success) {
            PingCounters->Ok->Inc();
        } else {
            PingCounters->Error->Inc();
        }
    }

    Fq::Private::PingTaskRequest GetPingTaskRequest(std::optional<FederatedQuery::QueryMeta::ComputeStatus> computeStatus, std::optional<NYql::NDqProto::StatusIds::StatusCode> pendingStatusCode, const NYql::TIssues& issues, const Ydb::TableStats::QueryStats& queryStats) const {
        Fq::Private::PingTaskRequest pingTaskRequest;
        NYql::IssuesToMessage(issues, pingTaskRequest.mutable_issues());
        if (computeStatus) {
            pingTaskRequest.set_status(*computeStatus);
        }
        if (pendingStatusCode) {
            pingTaskRequest.set_pending_status_code(*pendingStatusCode);
        }
        PrepareAstAndPlan(pingTaskRequest, queryStats.query_plan(), queryStats.query_ast());
        return pingTaskRequest;
    }

    // Can throw errors
    Fq::Private::PingTaskRequest GetPingTaskRequestStatistics(std::optional<FederatedQuery::QueryMeta::ComputeStatus> computeStatus, std::optional<NYql::NDqProto::StatusIds::StatusCode> pendingStatusCode, const NYql::TIssues& issues, const Ydb::TableStats::QueryStats& queryStats, double* cpuUsage = nullptr) const {
        Fq::Private::PingTaskRequest pingTaskRequest = GetPingTaskRequest(computeStatus, pendingStatusCode, issues, queryStats);
        pingTaskRequest.set_statistics(GetV1StatFromV2Plan(queryStats.query_plan(), cpuUsage));
        return pingTaskRequest;
    }

protected:
    void PrepareAstAndPlan(Fq::Private::PingTaskRequest& request, const TString& plan, const TString& expr) const {
        if (Compressor.IsEnabled()) {
            auto [astCompressionMethod, astCompressed] = Compressor.Compress(expr);
            request.mutable_ast_compressed()->set_method(astCompressionMethod);
            request.mutable_ast_compressed()->set_data(astCompressed);

            auto [planCompressionMethod, planCompressed] = Compressor.Compress(plan);
            request.mutable_plan_compressed()->set_method(planCompressionMethod);
            request.mutable_plan_compressed()->set_data(planCompressed);
        } else {
            request.set_ast(expr);
            request.set_plan(plan);
        }
    }

private:
    TInstant StartTime;
    TComputeRequestCountersPtr PingCounters;
    const TCompressor Compressor;
};

} /* NFq */
