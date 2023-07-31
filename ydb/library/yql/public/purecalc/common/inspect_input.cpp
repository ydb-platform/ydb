#include "inspect_input.h"

#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql::NPureCalc {
    bool TryFetchInputIndexFromSelf(const TExprNode& node, TExprContext& ctx, ui32 inputsCount, ui32& result) {
        TIssueScopeGuard issueSope(ctx.IssueManager, [&]() {
            return MakeIntrusive<TIssue>(ctx.GetPosition(node.Pos()), TStringBuilder() << "At function: " << node.Content());
        });

        if (!EnsureArgsCount(node, 1, ctx)) {
            return false;
        }

        if (!EnsureAtom(*node.Child(0), ctx)) {
            return false;
        }

        if (!TryFromString(node.Child(0)->Content(), result)) {
            auto message = TStringBuilder() << "Index " << TString{node.Child(0)->Content()}.Quote() << " isn't UI32";
            ctx.AddError(TIssue(ctx.GetPosition(node.Child(0)->Pos()), std::move(message)));
            return false;
        }

        if (result >= inputsCount) {
            auto message = TStringBuilder() << "Invalid input index: " << result << " is out of range [0;" << inputsCount << ")";
            ctx.AddError(TIssue(ctx.GetPosition(node.Child(0)->Pos()), std::move(message)));
            return false;
        }

        return true;
    }
}
