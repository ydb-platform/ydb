#include "yql_exec.h"

#include <ydb/library/yql/core/yql_execution.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/string/builder.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>


namespace NYql {

using namespace NNodes;

TExecTransformerBase::TStatusCallbackPair TExecTransformerBase::CallbackTransform(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) {
    YQL_ENSURE(input->Type() == TExprNode::Callable);
    output = input;
    if (auto handlerInfo = Handlers.FindPtr(input->Content())) {
        auto status = (handlerInfo->Prerequisite)(input);
        if (status.Level != TStatus::Ok) {
            return SyncStatus(status);
        }
        TString uniqId = TStringBuilder() << '#' << input->UniqueId();
        YQL_LOG_CTX_SCOPE(uniqId);

        return (handlerInfo->Handler)(input, output, ctx);
    }

    ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), TStringBuilder() << "Don't know how to execute node: " << input->Content()));
    return SyncError();
}

void TExecTransformerBase::AddHandler(std::initializer_list<TStringBuf> names, TPrerequisite prerequisite, THandler handler) {
    THandlerInfo info;
    info.Handler = std::move(handler);
    info.Prerequisite = std::move(prerequisite);
    for (auto name: names) {
        YQL_ENSURE(Handlers.emplace(name, info).second, "Duplicate execution handler for " << name);
    }
}

TExecTransformerBase::THandler TExecTransformerBase::Pass() {
    return [] (const TExprNode::TPtr& input, TExprNode::TPtr& /*output*/, TExprContext& ctx) {
        return ExecPass(input, ctx);
    };
}

TExecTransformerBase::TStatusCallbackPair TExecTransformerBase::ExecPass(const TExprNode::TPtr& input, TExprContext& ctx) {
    input->SetState(TExprNode::EState::ExecutionComplete);
    input->SetResult(ctx.NewWorld(input->Pos()));
    return SyncOk();
}

TExecTransformerBase::TPrerequisite TExecTransformerBase::RequireAll() {
    return [] (const TExprNode::TPtr& input) {
        TStatus combinedStatus = TStatus::Ok;
        for (size_t i = 0; i < input->ChildrenSize(); ++i) {
            combinedStatus = combinedStatus.Combine(RequireChild(*input, (ui32)i));
        }
        return combinedStatus;
    };
}

TExecTransformerBase::TPrerequisite TExecTransformerBase::RequireNone() {
    return [] (const TExprNode::TPtr& /*input*/) {
        return TStatus::Ok;
    };
}

TExecTransformerBase::TPrerequisite TExecTransformerBase::RequireFirst() {
    return [] (const TExprNode::TPtr& input) {
        return RequireChild(*input, 0);
    };
}

TExecTransformerBase::TPrerequisite TExecTransformerBase::RequireAllOf(std::initializer_list<size_t> children) {
    return [required = TVector<size_t>(children)] (const TExprNode::TPtr& input) {
        TStatus combinedStatus = TStatus::Ok;
        for (size_t i: required) {
            YQL_ENSURE(i < input->ChildrenSize());
            combinedStatus = combinedStatus.Combine(RequireChild(*input, (ui32)i));
        }
        return combinedStatus;
    };
}

TExecTransformerBase::TPrerequisite TExecTransformerBase::RequireSequenceOf(std::initializer_list<size_t> children) {
    return [required = TVector<size_t>(children)] (const TExprNode::TPtr& input) -> TStatus {
        for (size_t i: required) {
            YQL_ENSURE(i < input->ChildrenSize());
            auto status = RequireChild(*input, (ui32)i);
            if (status.Level != TStatus::Ok) {
                return status;
            }
        }
        return TStatus::Ok;
    };
}

}
