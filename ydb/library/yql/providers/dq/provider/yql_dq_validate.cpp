#include "yql_dq_validate.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NYql::NNodes;
namespace {
class TDqExecutionValidator {
private:
    static void ReportError(TExprContext& ctx, const TExprNode& where, const TString& err) {
        YQL_CLOG(WARN, ProviderDq) << "Falling back from DQ: " << err;
        ctx.AddError(YqlIssue(ctx.GetPosition(where.Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, err));
    }

    bool ValidateDqStage(const TExprNode& node) {
        if (!Visited_.insert(&node).second) {
            return true;
        }

        bool hasErrors = false;
        if (auto bad = FindNonYieldTransparentNode(TDqStageBase(&node).Program().Ptr(), TypeCtx_)) {
            hasErrors = true;
            ReportError(Ctx_, *bad, TStringBuilder() << "Cannot execute " << bad->Content() << " over stream/flow inside DQ stage");
        }

        VisitExpr(TDqStageBase(&node).Program().Body().Ptr(),
            [](const TExprNode::TPtr& n) {
                return !TDqConnection::Match(n.Get()) && !TDqPhyPrecompute::Match(n.Get()) && !TDqReadWrapBase::Match(n.Get());
            },
            [&readPerProvider_ = ReadsPerProvider_, &hasErrors, &ctx = Ctx_, &typeCtx = TypeCtx_](const TExprNode::TPtr& n) {
                if (TCoScriptUdf::Match(n.Get()) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(n->Head().Content()))) {
                    ReportError(ctx, *n, TStringBuilder() << "Cannot execute system python udf " << n->Content() << " in DQ");
                    hasErrors = true;
                }
                if (!typeCtx.ForceDq && TDqReadWrapBase::Match(n.Get())) {
                    auto readNode = n->Child(0);
                    auto dataSourceName = readNode->Child(1)->Child(0)->Content();
                    if (dataSourceName != DqProviderName) {
                        auto datasource = typeCtx.DataSourceMap.FindPtr(dataSourceName);
                        YQL_ENSURE(datasource);
                        auto dqIntegration = (*datasource)->GetDqIntegration();
                        YQL_ENSURE(dqIntegration);
                        readPerProvider_[dqIntegration].push_back(readNode);
                    }
                }
                return !hasErrors;
            }
        );

        for (auto n: TDqStageBase(&node).Inputs()) {
            hasErrors |= !ValidateDqNode(n.Ref());
        }

        if (auto outs = TDqStageBase(&node).Outputs()) {
            for (auto n: outs.Cast()) {
                hasErrors |= !ValidateDqNode(n.Ref());
            }
        }

        return !hasErrors;

    }

    bool ValidateDqNode(const TExprNode& node) {
        if (node.GetState() == TExprNode::EState::ExecutionComplete) {
            return true;
        }

        if (TDqStageBase::Match(&node)) {
            // visited will be updated inside ValidateDqStage
            return ValidateDqStage(node);
        }

        if (!Visited_.insert(&node).second) {
            return true;
        }

        if (TDqCnResult::Match(&node)) {
            ReportError(Ctx_, node, TStringBuilder() << TDqCnResult::CallableName() << " connection cannot be used inside graph");
            return false;
        }

        if (TDqConnection::Match(&node)) {
            return ValidateDqStage(TDqConnection(&node).Output().Stage().Ref());
        }
        if (TDqPhyPrecompute::Match(&node)) {
            return ValidateDqNode(TDqPhyPrecompute(&node).Connection().Ref());
        }

        if (TDqSource::Match(&node) || TDqTransform::Match(&node) || TDqSink::Match(&node)) {
            return true;
        }

        ReportError(Ctx_, node, TStringBuilder() << "Failed to execute callable with name: " << node.Content() << " in DQ");
        return false;
    }

public:
    TDqExecutionValidator(const TTypeAnnotationContext& typeCtx, TExprContext& ctx, const TDqState::TPtr state)
        : TypeCtx_(typeCtx)
        , Ctx_(ctx)
        , State_(state)
    {}

    bool ValidateDqExecution(const TExprNode& node) {
        YQL_LOG_CTX_SCOPE(__FUNCTION__);

        TNodeSet dqNodes;

        bool hasJoin = false;
        if (TDqCnResult::Match(&node)) {
            dqNodes.insert(TDqCnResult(&node).Output().Stage().Raw());
        } else if (TDqQuery::Match(&node)) {
            for (auto st: TDqQuery(&node).SinkStages()) {
                dqNodes.insert(st.Raw());
            }
        } else {
            VisitExpr(node, [&dqNodes](const TExprNode& n) {
                if (TDqStageBase::Match(&n)) {
                    dqNodes.insert(&n);
                    return false;
                } else if (TDqConnection::Match(&n)) {
                    dqNodes.insert(&n);
                    return false;
                } else if (TDqReadWrapBase::Match(&n)) {
                    return false;
                }
                return true;
            });
        }

        VisitExpr(node, [&hasJoin](const TExprNode& n) {
            if (TMaybeNode<TDqPhyMapJoin>(&n)) {
                hasJoin = true;
            }
            return true;
        });

        bool hasError = false;

        for (const auto n: dqNodes) {
            hasError |= !ValidateDqNode(*n);
            if (hasError) {
                break;
            }
        }

        for (auto& [integration, nodes]: ReadsPerProvider_) {
            TMaybe<ui64> size;
            hasError |= !(size = integration->EstimateReadSize(State_->Settings->DataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::DataSizePerJob),
                State_->Settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage), nodes, Ctx_));
            if (hasError) {
                break;
            }
            DataSize_ += *size;
        }

        if (!hasError && hasJoin && DataSize_ > State_->Settings->MaxDataSizePerQuery.Get().GetOrElse(10_GB)) {
            ReportError(Ctx_, node, TStringBuilder() << "too big join input: " << DataSize_);
            return false;
        }
        return !hasError;
    }
private:

    const TTypeAnnotationContext& TypeCtx_;
    TExprContext& Ctx_;
    TNodeSet Visited_;
    THashMap<IDqIntegration*, TVector<const TExprNode*>> ReadsPerProvider_;
    size_t DataSize_ = 0;
    const TDqState::TPtr State_;

};
}

bool ValidateDqExecution(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx, const TDqState::TPtr state) {
    return TDqExecutionValidator(typeCtx, ctx, state).ValidateDqExecution(node);
}

} // namespace NYql
