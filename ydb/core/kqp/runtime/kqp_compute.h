#pragma once

#include <ydb/core/kqp/runtime/common/kqp_compute_context.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetKqpBaseComputeFactory(const TKqpComputeContextBase* computeCtx);

class TKqpEnsureFail : public yexception {
public:
    TKqpEnsureFail(ui32 code, TString&& message)
        : Code(code)
        , Message(std::move(message)) {}

    ui32 GetCode() const {
        return Code;
    }

    const TString& GetMessage() const {
        return Message;
    }

private:
    ui32 Code;
    TString Message;
};

IComputationNode* WrapKqpEnsure(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapKqpIndexLookupJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapFulltextAnalyze(TCallable& callable, const TComputationNodeFactoryContext& ctx);
IComputationNode* WrapKqpStreamEnumerate(TCallable& callable, const TComputationNodeFactoryContext& ctx);

} // namespace NKikimr::NMiniKQL
