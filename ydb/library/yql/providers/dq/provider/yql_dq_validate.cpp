#include "yql_dq_validate.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
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

bool ValidateDqNode(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx, TNodeSet& visited);

bool ValidateDqStage(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx, TNodeSet& visited) {
    if (!visited.insert(&node).second) {
        return true;
    }

    bool hasErrors = false;
    if (auto bad = FindNonYieldTransparentNode(TDqStageBase(&node).Program().Ptr(), typeCtx)) {
        hasErrors = true;
        YQL_CLOG(WARN, ProviderDq) << "Cannot execute " << bad->Content() << " over stream/flow inside DQ stage";
        ctx.AddError(YqlIssue(ctx.GetPosition(bad->Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, TStringBuilder() << "Cannot execute " << bad->Content() << " over stream/flow inside DQ stage"));
    }
    VisitExpr(TDqStageBase(&node).Program().Body().Ptr(),
        [](const TExprNode::TPtr& n) {
            return !TDqConnection::Match(n.Get()) && !TDqPhyPrecompute::Match(n.Get()) && !TDqReadWrapBase::Match(n.Get());
        },
        [&hasErrors, &ctx](const TExprNode::TPtr& n) {
            if (TCoScriptUdf::Match(n.Get()) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(n->Head().Content()))) {
                YQL_CLOG(WARN, ProviderDq) << "Cannot execute system python udf " << n->Content() << " in DQ";
                ctx.AddError(YqlIssue(ctx.GetPosition(n->Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, TStringBuilder() << "Cannot execute system python udf " << n->Content() << " in DQ"));
                hasErrors = true;
            }
            return !hasErrors;
        }
    );

    for (auto n: TDqStageBase(&node).Inputs()) {
        hasErrors = !ValidateDqNode(n.Ref(), typeCtx, ctx, visited) || hasErrors;
    }
    if (auto outs = TDqStageBase(&node).Outputs()) {
        for (auto n: outs.Cast()) {
            hasErrors = !ValidateDqNode(n.Ref(), typeCtx, ctx, visited) || hasErrors;
        }
    }

    return !hasErrors;

}

bool ValidateDqNode(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx, TNodeSet& visited) {
    if (node.GetState() == TExprNode::EState::ExecutionComplete) {
        return true;
    }

    if (TDqStageBase::Match(&node)) {
        // visited will be updated inside ValidateDqStage
        return ValidateDqStage(node, typeCtx, ctx, visited);
    }

    if (!visited.insert(&node).second) {
        return true;
    }

    if (TDqCnResult::Match(&node)) {
        YQL_CLOG(WARN, ProviderDq) << TDqCnResult::CallableName() << " connection cannot be used inside graph";
        ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, TStringBuilder() << TDqCnResult::CallableName() << " connection cannot be used inside graph"));
        return false;
    }
    if (TDqConnection::Match(&node)) {
        return ValidateDqStage(TDqConnection(&node).Output().Stage().Ref(), typeCtx, ctx, visited);
    }
    if (TDqPhyPrecompute::Match(&node)) {
        return ValidateDqNode(TDqPhyPrecompute(&node).Connection().Ref(), typeCtx, ctx, visited);
    }

    if (TDqSource::Match(&node) || TDqTransform::Match(&node) || TDqSink::Match(&node)) {
        return true;
    }

    YQL_CLOG(WARN, ProviderDq) << "Failed to execute callable with name: " << node.Content() << " in DQ";
    ctx.AddError(YqlIssue(ctx.GetPosition(node.Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, TStringBuilder() << "Failed to execute callable with name: " << node.Content() << " in DQ"));
    return false;
}

}

bool ValidateDqExecution(const TExprNode& node, const TTypeAnnotationContext& typeCtx, TExprContext& ctx) {
    YQL_LOG_CTX_SCOPE(__FUNCTION__);

    TNodeSet dqNodes;
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
    TNodeSet visited;
    bool hasError = false;
    for (const auto n: dqNodes) {
        hasError = !ValidateDqNode(*n, typeCtx, ctx, visited) || hasError;
    }
    return !hasError;
}

} // namespace NYql
