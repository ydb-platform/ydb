#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/system/types.h>
#include <cstddef>

namespace NKikimr::NMiniKQL {

enum class EExecutionWaitKind { Input, SpillRead, SpillWrite, AsyncIo };
enum class EExecutionInputSide { None, Left, Right, Build, Probe };
enum class EExecutionWorkKind { Unknown, Consume, Compute, Emit, Control };

struct TExecutionInputWaitScope {
    size_t Probes = 0;
    size_t Reports = 0;
};

// Optional per-execution observer. Fetch statuses retain their existing meaning;
// operators report why they yielded through this side channel. An input scope
// preserves a more precise wait reported by a child (e.g. a spill read).
class IExecutionWaitObserver {
public:
    virtual ~IExecutionWaitObserver() = default;
    virtual TExecutionInputWaitScope BeginInput() const = 0;
    virtual void AwaitInput(TExecutionInputWaitScope scope, ui64 operatorId, EExecutionInputSide side) = 0;
    // operation identifies a stable future slot in the operator's state. Its
    // contents must not be replaced while the previous operation is pending.
    virtual void AwaitFuture(ui64 operatorId, const void* operation, EExecutionWaitKind kind,
                             const NThreading::TFuture<void>& future) = 0;
    virtual void AwaitCallback(ui64 operatorId, EExecutionWaitKind kind) = 0;
    virtual void MakeRunnable() = 0;
    // Optional extensions keep observers and opaque/third-party nodes source
    // compatible. Work is a phase hint, not a promise that Fetch can run now.
    virtual void ReportWork(ui64, const char*, EExecutionWorkKind, const char*) {}
    virtual void AwaitNamedInput(TExecutionInputWaitScope scope, ui64 operatorId,
                                 EExecutionInputSide side, const char*) {
        AwaitInput(scope, operatorId, side);
    }
};

class TExecutionInputScope {
public:
    TExecutionInputScope(IExecutionWaitObserver* observer, const void* operation, EExecutionInputSide side,
                         const char* name = "")
        : Observer(observer), OperatorId(reinterpret_cast<ui64>(operation)), Side(side)
        , Scope(observer ? observer->BeginInput() : TExecutionInputWaitScope{}), Name(name) {
        if (Observer && *Name) {
            const char* phase = side == EExecutionInputSide::Build ? "build"
                : side == EExecutionInputSide::Probe ? "probe"
                : side == EExecutionInputSide::Left ? "left"
                : side == EExecutionInputSide::Right ? "right" : "accumulate";
            Observer->ReportWork(OperatorId, Name, EExecutionWorkKind::Consume, phase);
        }
    }
    void Yielded() const {
        if (Observer) {
            Observer->AwaitNamedInput(Scope, OperatorId, Side, Name);
        }
    }
private:
    IExecutionWaitObserver* const Observer;
    const ui64 OperatorId;
    const EExecutionInputSide Side;
    const TExecutionInputWaitScope Scope;
    const char* const Name;
};

} // namespace NKikimr::NMiniKQL
