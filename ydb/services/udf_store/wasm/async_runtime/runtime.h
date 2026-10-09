#pragma once

#include "../abi/async_abi.h"
#include "../compartment_manager.h"

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>
#include <memory>
#include <mutex>

namespace NKikimr::NUdfStore::NWasm::NAsync {

using NYdb::NWasm::NAsync::EOperationKind;
using NYdb::NWasm::NAsync::EOperationStatus;
using NYdb::NWasm::NAsync::THandle;

class ITransport {
public:
    using TCompletion = std::function<bool(EOperationStatus, TString)>;
    using TCancel = std::function<void()>;
    virtual ~ITransport() = default;
    // Transport owns its request and buffers. Completion may run on any thread.
    // Timer requests contain an ABI ui64 delay in microseconds.
    virtual TCancel Start(THandle operation, EOperationKind kind, TString request, TInstant deadline, TCompletion completion) = 0;
};

enum class ECallStatus {
    Runnable,
    Waiting,
    Completed,
    Failed,
    Cancelled,
};

struct TCallResult {
    ECallStatus Status;
    TString Data;
};

struct TLimits {
    ui64 MaxCalls = 64;
    ui64 MaxOperations = 256;
    ui64 MaxPayloadBytes = 1 << 20;
    ui64 MaxBufferedBytes = 8 << 20;
    TDuration CpuBudget = TDuration::MilliSeconds(100);
};

struct TStats {
    ui64 Calls = 0;
    ui64 Operations = 0;
    ui64 BufferedBytes = 0;
};

// All guest entries are serialized by the owner. Completion only updates host
// state; TakeReady is the handoff to an actor event loop, never a guest callback.
class TRuntime {
public:
    TRuntime(TQueryCompartmentHandlePtr query, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
             std::shared_ptr<ITransport> transport, TLimits limits = {}, std::function<void()> wakeup = {});
    ~TRuntime();

    THandle Start(TStringBuf arguments, TInstant deadline);
    TCallResult Poll(THandle call);
    void Cancel(THandle call);
    void Drop(THandle call);
    TVector<THandle> TakeReady();
    void Expire(TInstant now);
    TInstant NextDeadline() const;
    TStats Stats() const;
    bool IsPoisoned() const;

    // Host imports require an active short-lived entry scope.
    THandle StartOperation(THandle call, TString request, EOperationKind kind = EOperationKind::Request);
    THandle StartTimer(THandle call, ui64 delayMicroseconds);
    EOperationStatus PollOperation(THandle call, THandle operation) const;
    TString ReadOperation(THandle call, THandle operation) const;
    void CancelOperation(THandle call, THandle operation);
    void DropOperation(THandle call, THandle operation);
    void Wait(THandle call, THandle operation);
    void Complete(THandle call, TString result, EOperationStatus status);
    TString ReadGuest(ui64 offset, ui64 size) const;
    void WriteGuest(ui64 offset, ui64 size, TStringBuf data) const;

private:
    struct TState;
    struct TCall;
    class TEntry;

    void Enter(THandle call, const std::function<void()>& fn, bool cleanup = false);
    void DropImpl(THandle call);
    void CancelAllOperations(THandle call);
    void Poison();

    TQueryCompartmentHandlePtr Query_;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc_;
    std::shared_ptr<ITransport> Transport_;
    std::shared_ptr<TState> State_;
    std::mutex EntryMutex_;
};

struct TInvocation {
    TRuntime* Runtime;
    THandle Call;
};

TInvocation* GetCurrentAsyncInvocation();
void KeepAsyncHostIntrinsicsLinked();

} // namespace NKikimr::NUdfStore::NWasm::NAsync
