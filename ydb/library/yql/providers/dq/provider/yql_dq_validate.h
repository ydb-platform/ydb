#pragma once

#include "yql_dq_state.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>

#include <memory>

namespace NYql {

class IDqIntegration;

class TDqExecutionValidator {
public:
    TDqExecutionValidator(const TDqState::TPtr state);

    bool ValidateDqExecution(const TExprNode& node, TExprContext& ctx);

    void Rewind();

private:
    struct TValidateInfo {
        THashMap<IDqIntegration*, TNodeSet> ReadsPerProvider;
        TNodeSet DependsOnStages;
        bool HasMapJoin = false;
        std::shared_ptr<TIssue> Issue;
    };

    std::shared_ptr<TIssue> ValidateDqStage(const TExprNode& node, TExprContext& ctx, TValidateInfo* inner);
    std::shared_ptr<TIssue> ValidateDqNode(const TExprNode& node, TExprContext& ctx, TValidateInfo* inner);

private:
    const TDqState::TPtr State_;

    TNodeMap<TValidateInfo> Visited_;
};

} // namespace NYql
