#include "yql_dq_state.h"
#include "yql_dq_provider.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <util/generic/scope.h>

namespace NYql {

using namespace NNodes;
using namespace NKikimr::NMiniKQL;

namespace {

const THashSet<TStringBuf> VALID_SOURCES = {DqProviderName, ConfigProviderName, YtProviderName, ClickHouseProviderName, YdbProviderName, S3ProviderName, PgProviderName, SolomonProviderName};
const THashSet<TStringBuf> VALID_SINKS = {ResultProviderName, YtProviderName, S3ProviderName};

}

namespace NDq {
    bool CheckJoinColumns(const TExprBase& node);
} // namespace NDq

class TDqsRecaptureTransformer : public TSyncTransformerBase {
public:
    TDqsRecaptureTransformer(TDqStatePtr state)
        : State_(state)
    {
    }

    TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) final {
        output = input;
        if (ctx.Step.IsDone(TExprStep::Recapture)) {
            return TStatus::Ok;
        }

        Y_SCOPE_EXIT(&) {
            FlushStatistics();
        };

        if (State_->ExternalUser && !State_->Settings->_EnablePorto.Get().GetOrElse(TDqSettings::TDefault::EnablePorto)) {
            Statistics_["DqExternalUser"]++;
            YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
            State_->AbortHidden();
            return TStatus::Ok;
        }

        if (State_->TypeCtx->ForceDq) {
            Statistics_["DqForce"]++;
        }

        if (!State_->TypeCtx->ForceDq) {
            if (!State_->Settings->AnalyzeQuery.Get().GetOrElse(false)) {
                Statistics_["DqAnalyzerOff"]++;
            }

            if (State_->TypeCtx->PureResultDataSource != DqProviderName) {
                Statistics_["DqPureResultDataSourceMismatch"]++;
            }

            if (State_->TypeCtx->PureResultDataSource != DqProviderName || !State_->Settings->AnalyzeQuery.Get().GetOrElse(false)) {
                YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
                State_->AbortHidden();
                return TStatus::Ok;
            }

            Statistics_["DqAnalyzerOn"]++;

            bool good = true;
            TNodeSet visited;
            Scan(*input, ctx, good, visited);

            if (good) {
                Statistics_["DqAnalyzerOk"]++;
            } else {
                Statistics_["DqAnalyzerFail"] ++;
            }

            if (!good) {
                YQL_CLOG(DEBUG, ProviderDq) << "abort hidden";
                State_->AbortHidden();
                return TStatus::Ok;
            }
        }

        State_->TypeCtx->DqFallbackPolicy = State_->Settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default);

        IGraphTransformer::TStatus status = NDq::DqWrapRead(input, output, ctx, *State_->TypeCtx, *State_->Settings);
        if (input != output) {
            YQL_CLOG(INFO, ProviderDq) << "DqsRecapture";
            // TODO: Add before/after recapture transformers
            State_->TypeCtx->DqCaptured = true;
            // TODO: drop this after implementing DQS ConstraintTransformer
            State_->TypeCtx->ExpectedConstraints.clear();
        }
        return status;
    }

    void Rewind() final {
    }

private:
    void AddInfo(TExprContext& ctx, const TString& message) const {
        YQL_CLOG(DEBUG, ProviderDq) << message;
        TIssue info("DQ cannot execute the query. Cause: " + message);
        info.Severity = TSeverityIds::S_INFO;
        ctx.IssueManager.RaiseIssue(info);
    }

    void Scan(const TExprNode& node, TExprContext& ctx, bool& good, TNodeSet& visited) const {
        if (!visited.insert(&node).second) {
            return;
        }

        TExprBase expr(&node);

        if (TCoCommit::Match(&node)) {
            for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                if (i != TCoCommit::idx_DataSink) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        } else if (auto datasource = TMaybeNode<TCoDataSource>(&node).Category()) {
            if (!VALID_SOURCES.contains(datasource.Cast().Value())) {
                AddInfo(ctx, TStringBuilder() << "source '" << datasource.Cast().Value() << "' is not supported by DQ");
                good = false;
            }
        } else if (auto datasink = TMaybeNode<TCoDataSink>(&node).Category()) {
            if (!VALID_SINKS.contains(datasink.Cast().Value())) {
                AddInfo(ctx, TStringBuilder() << "sink '" << datasink.Cast().Value() << "' is not supported by DQ");
                good = false;
            }
        } else if (TMaybeNode<TCoEquiJoin>(&node) && !NDq::CheckJoinColumns(expr)) {
            AddInfo(ctx, TStringBuilder() << "unsupported join column");
            good = false;
        } else if (node.ChildrenSize() > 1 && TCoDataSource::Match(node.Child(1))) {
            auto dataSourceName = node.Child(1)->Child(0)->Content();
            if (dataSourceName != DqProviderName && !node.IsCallable(ConfigureName)) {
                auto datasource = State_->TypeCtx->DataSourceMap.FindPtr(dataSourceName);
                YQL_ENSURE(datasource);
                auto dqIntegration = (*datasource)->GetDqIntegration();
                if (dqIntegration) {
                    bool pragmas = dqIntegration->CheckPragmas(node, ctx, false);
                    bool canRead = pragmas && dqIntegration->CanRead(node, ctx, /*skipIssues = */ false);

                    if (!pragmas || !canRead) {
                        good = false;
                        if (!pragmas) {
                            State_->TypeCtx->PureResultDataSource.clear();
                            std::erase_if(State_->TypeCtx->AvailablePureResultDataSources,
                                          [&](const auto& name) { return name == DqProviderName; });
                        }
                    }
                } else {
                    AddInfo(ctx, TStringBuilder() << "source '" << dataSourceName << "' is not supported by DQ");
                    good = false;
                }
            }

            if (good) {
                Scan(node.Head(), ctx,good, visited);
            }
        } else if (node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::World
            && !TCoCommit::Match(&node)
            && node.ChildrenSize() > 1
            && TCoDataSink::Match(node.Child(1))) {
            auto dataSinkName = node.Child(1)->Child(0)->Content();
            auto dataSink = State_->TypeCtx->DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(dataSink);
            if (auto dqIntegration = dataSink->Get()->GetDqIntegration()) {
                if (auto canWrite = dqIntegration->CanWrite(node, ctx)) {
                    if (!canWrite.GetRef()) {
                        good = false;
                    } else if (!State_->Settings->EnableInsert.Get().GetOrElse(false)) {
                        AddInfo(ctx, TStringBuilder() << "'insert' support is disabled. Use PRAGMA dq.EnableInsert to explicitly enable it");
                        good = false;
                    }
                }
            }
            if (good) {
                for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        }
        else if (TCoScriptUdf::Match(&node)) {
            if (good && TCoScriptUdf::Match(&node) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(node.Head().Content()))) {
                AddInfo(ctx, TStringBuilder() << "system python udf");
                good = false;
            }
            if (good) {
                for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                    Scan(*node.Child(i), ctx, good, visited);
                }
            }
        }
        else {
            for (size_t i = 0; i != node.ChildrenSize() && good; ++i) {
                Scan(*node.Child(i), ctx, good, visited);
            }
        }
    }

private:
    TDqStatePtr State_;

    THashMap<TString, int> Statistics_;

    void FlushStatistics() {
        TOperationStatistics statistics;
        for (const auto& [k, v] : Statistics_) {
            if (v == 1) {
                statistics.Entries.push_back(TOperationStatistics::TEntry(k, 0, 0, 0, 0, 1));
            }
        }

        TGuard<TMutex> lock(State_->Mutex);
        if (!statistics.Entries.empty()) {
            State_->Statistics[State_->MetricId++] = statistics;
        }
    }
};

THolder<IGraphTransformer> CreateDqsRecaptureTransformer(TDqStatePtr state) {
    return THolder(new TDqsRecaptureTransformer(state));
}

} // NYql
