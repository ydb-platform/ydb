#include "yql_dq_validate.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/utils/log/log.h>

#include <util/string/builder.h>

namespace NYql {

using namespace NYql::NNodes;

namespace {
    TIssue MakeError(TExprContext& ctx, const TExprNode& where, const TString& err) {
        return YqlIssue(ctx.GetPosition(where.Pos()), TIssuesIds::DQ_OPTIMIZE_ERROR, err);
    }

    std::shared_ptr<TIssue> MakeErrorPtr(TExprContext& ctx, const TExprNode& where, const TString& err) {
        return std::make_shared<TIssue>(MakeError(ctx, where, err));
    }
}

void TDqExecutionValidator::Rewind() {
    Visited_.clear();
}

std::shared_ptr<TIssue> TDqExecutionValidator::ValidateDqStage(const TExprNode& node, TExprContext& ctx, TValidateInfo* collect) {
    auto res = Visited_.emplace(&node, TValidateInfo{});
    TValidateInfo& stageInfo = res.first->second;
    if (res.second) {
        if (auto bad = FindNonYieldTransparentNode(TDqStageBase(&node).Program().Ptr(), *State_->TypeCtx)) {
            return (stageInfo.Issue = MakeErrorPtr(ctx, *bad, TStringBuilder() << "Cannot execute " << bad->Content() << " over stream/flow inside DQ stage"));
        }

        bool hasJoin = false;
        bool hasErrors = false;
        THashMap<IDqIntegration*, TVector<const TExprNode*>> readsPerProvider;
        VisitExpr(TDqStageBase(&node).Program().Body().Ptr(),
            [&hasErrors](const TExprNode::TPtr& n) {
                return !hasErrors && !TDqConnection::Match(n.Get()) && !TDqPhyPrecompute::Match(n.Get()) && !TDqReadWrapBase::Match(n.Get());
            },
            [&stageInfo, &hasErrors, &hasJoin, &ctx, typeCtx = State_->TypeCtx](const TExprNode::TPtr& n) {
                if (TDqPhyMapJoin::Match(n.Get()) || TDqPhyGraceJoin::Match(n.Get())) {
                    hasJoin = stageInfo.HasMapJoin = true;
                } else if (TCoGraceJoinCore::Match(n.Get()) || TCoGraceSelfJoinCore::Match(n.Get())) {
                    hasJoin = true;
                }

                if (TCoScriptUdf::Match(n.Get()) && NKikimr::NMiniKQL::IsSystemPython(NKikimr::NMiniKQL::ScriptTypeFromStr(n->Head().Content()))) {
                    stageInfo.Issue = MakeErrorPtr(ctx, *n, TStringBuilder() << "Cannot execute system python udf " << n->Content() << " in DQ");
                    hasErrors = true;
                }
                if (!typeCtx->ForceDq && TDqReadWrapBase::Match(n.Get())) {
                    auto readNode = n->Child(0);
                    auto dataSourceName = readNode->Child(1)->Child(0)->Content();
                    if (dataSourceName != DqProviderName) {
                        auto datasource = typeCtx->DataSourceMap.FindPtr(dataSourceName);
                        YQL_ENSURE(datasource);
                        auto dqIntegration = (*datasource)->GetDqIntegration();
                        YQL_ENSURE(dqIntegration);
                        stageInfo.ReadsPerProvider[dqIntegration].insert(readNode);
                    }
                }
                return !hasErrors;
            }
        );
        if (hasErrors) {
            YQL_ENSURE(stageInfo.Issue);
            return stageInfo.Issue;
        }

        const bool checkSelfJoin = hasJoin && !State_->TypeCtx->ForceDq
            && !State_->Settings->SplitStageOnDqReplicate.Get().GetOrElse(TDqSettings::TDefault::SplitStageOnDqReplicate)
            && !State_->Settings->IsSpillingInChannelsEnabled();

        for (auto n: TDqStageBase(&node).Inputs()) {
            TValidateInfo vi;
            if (auto err = ValidateDqNode(n.Ref(), ctx, &vi)) {
                return (stageInfo.Issue = err);
            }
            const size_t expectedSize = stageInfo.DependsOnStages.size() + vi.DependsOnStages.size();
            stageInfo.DependsOnStages.insert(vi.DependsOnStages.begin(), vi.DependsOnStages.end());
            if (checkSelfJoin && expectedSize != stageInfo.DependsOnStages.size()) { // Found duplicates - some stage was visited twice from different inputs
                return (stageInfo.Issue = MakeErrorPtr(ctx, node, TStringBuilder() << "Cannot execute self join in DQ"));
            }
            for (auto& p: vi.ReadsPerProvider) {
                stageInfo.ReadsPerProvider[p.first].insert(p.second.begin(), p.second.end());
            }
            stageInfo.HasMapJoin |= vi.HasMapJoin;
        }

        if (auto outs = TDqStageBase(&node).Outputs()) {
            for (auto n: outs.Cast()) {
                if (auto err = ValidateDqNode(n.Ref(), ctx, nullptr)) {
                    return (stageInfo.Issue = err);
                }
            }
        }
    }
    else if (stageInfo.Issue) {
        return stageInfo.Issue;
    }

    if (collect) {
        for (auto& p: stageInfo.ReadsPerProvider) {
            collect->ReadsPerProvider[p.first].insert(p.second.begin(), p.second.end());
        }
        collect->DependsOnStages.insert(stageInfo.DependsOnStages.begin(), stageInfo.DependsOnStages.end());
        collect->DependsOnStages.insert(&node);
        collect->HasMapJoin |= stageInfo.HasMapJoin;
    }

    return {};
}

std::shared_ptr<TIssue> TDqExecutionValidator::ValidateDqNode(const TExprNode& node, TExprContext& ctx, TValidateInfo* collect) {
    if (TDqStageBase::Match(&node)) {
        return ValidateDqStage(node, ctx, collect);
    }

    if (TDqCnResult::Match(&node)) {
        return MakeErrorPtr(ctx, node, TStringBuilder() << TDqCnResult::CallableName() << " connection cannot be used inside graph");
    }

    if (TDqConnection::Match(&node)) {
        return ValidateDqStage(TDqConnection(&node).Output().Stage().Ref(), ctx, TDqCnValue::Match(&node) ? nullptr : collect);
    }
    if (TDqPhyPrecompute::Match(&node)) {
        return ValidateDqNode(TDqPhyPrecompute(&node).Connection().Ref(), ctx, nullptr);
    }

    if (TDqSource::Match(&node) || TDqTransform::Match(&node) || TDqSink::Match(&node)) {
        return {};
    }

    return MakeErrorPtr(ctx, node, TStringBuilder() << "Failed to execute callable with name: " << node.Content() << " in DQ");
}

TDqExecutionValidator::TDqExecutionValidator(const TDqState::TPtr state)
    : State_(state)
{}

bool TDqExecutionValidator::ValidateDqExecution(const TExprNode& node, TExprContext& ctx) {
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

    TValidateInfo vi;
    for (const auto n: dqNodes) {
        if (auto err = ValidateDqNode(*n, ctx, &vi)) {
            YQL_CLOG(WARN, ProviderDq) << "Falling back from DQ: " << err->GetMessage();
            ctx.AddError(*err);
            return false;
        }
    }

    if (vi.HasMapJoin && !State_->TypeCtx->ForceDq) {
        size_t dataSize = 0;
        for (auto& [integration, nodes]: vi.ReadsPerProvider) {
            TVector<const TExprNode*> reads(nodes.begin(), nodes.end());
            TMaybe<ui64> size = integration->EstimateReadSize(State_->Settings->DataSizePerJob.Get().GetOrElse(TDqSettings::TDefault::DataSizePerJob),
                State_->Settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage), reads, ctx);
            if (!size) {
                return false;
            }
            dataSize += *size;
            if (dataSize > State_->Settings->MaxDataSizePerQuery.Get().GetOrElse(10_GB)) {
                auto err = MakeError(ctx, node, TStringBuilder() << "too big join input: " << dataSize);
                YQL_CLOG(WARN, ProviderDq) << "Falling back from DQ: " << err.GetMessage();
                ctx.AddError(err);
                return false;
            }
        }
    }
    return true;
}

} // namespace NYql
