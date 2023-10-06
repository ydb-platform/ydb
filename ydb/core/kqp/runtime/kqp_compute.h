#pragma once

#include <ydb/library/yql/dq/runtime/dq_compute.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>

// TODO rename file to runtime_compute_context.h

namespace NKikimr {
namespace NMiniKQL {

class TKqpComputeContextBase : public NYql::NDq::TDqComputeContextBase {
public:
    struct TColumn {
        NTable::TTag Tag;
        NScheme::TTypeInfo Type;
        TString TypeMod;
    };

    // used only at then building of a computation graph, to inject taskId in runtime nodes
    void SetCurrentTaskId(ui64 taskId) { CurrentTaskId = taskId; }
    ui64 GetCurrentTaskId() const { return CurrentTaskId; }

private:
    ui64 CurrentTaskId = 0;
};

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

} // namespace NMiniKQL
} // namespace NKikimr
