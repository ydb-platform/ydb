#include "runtime.h"

#include "../bridge_resident.h"
#include "../invocation_context.h"
#include "../registry_helpers.h"

#include <ydb/library/wasm/api/function.h>
#include <ydb/library/wasm/engine/wavm_private_imports.h>

#include <util/generic/hash.h>
#include <util/generic/scope.h>
#include <util/generic/yexception.h>
#include <util/system/datetime.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>

namespace NKikimr::NUdfStore::NWasm::NAsync {
namespace {

thread_local TInvocation* CurrentInvocation = nullptr;
std::atomic<ui64> NextHandle{1};

THandle NewHandle() {
    const auto handle = NextHandle.fetch_add(1);
    Y_ENSURE(handle && handle != std::numeric_limits<ui64>::max(), "Async handle space exhausted");
    return handle;
}

bool IsTerminal(ECallStatus status) {
    return status == ECallStatus::Completed || status == ECallStatus::Failed || status == ECallStatus::Cancelled;
}

void RunCancel(ITransport::TCancel cancel) noexcept {
    if (cancel) {
        try {
            cancel();
        } catch (...) {
            // A transport cleanup failure must not prevent other operations' cleanup.
        }
    }
}

} // namespace

TInvocation* GetCurrentAsyncInvocation() {
    return CurrentInvocation;
}

struct TRuntime::TCall {
    ECallStatus Status = ECallStatus::Runnable;
    TInstant Deadline;
    TDuration CpuSpent;
    THandle WaitingFor = 0;
    ui64 Frame = 0;
    ui64 Arguments = 0;
    ui64 ArgumentBytes = 0;
    TString Result;
    bool Queued = false;
    bool TimedOut = false;
};

struct TRuntime::TState {
    struct TOperation {
        THandle Call;
        EOperationStatus Status = EOperationStatus::Pending;
        TString Response;
        ui64 RequestBytes;
        ITransport::TCancel Cancel;
    };

    mutable std::mutex Mutex;
    THashMap<THandle, std::shared_ptr<TCall>> Calls;
    THashMap<THandle, TOperation> Operations;
    TVector<THandle> Ready;
    TLimits Limits;
    ui64 Bytes = 0;
    bool Poisoned = false;
    bool Closed = false;
    std::function<void()> Wakeup;

    void Reserve(ui64 size) {
        Y_ENSURE(size <= Limits.MaxPayloadBytes && size <= Limits.MaxBufferedBytes - Bytes, "Async buffer quota exhausted");
        Bytes += size;
    }

    std::shared_ptr<TCall> Call(THandle handle) const {
        auto it = Calls.find(handle);
        Y_ENSURE(it != Calls.end(), "Unknown async call handle");
        return it->second;
    }

    TOperation& Operation(THandle call, THandle handle) {
        auto it = Operations.find(handle);
        Y_ENSURE(it != Operations.end() && it->second.Call == call, "Invalid async operation owner or handle");
        return it->second;
    }

    bool Schedule(THandle handle, TCall& call) {
        call.Status = ECallStatus::Runnable;
        if (!call.Queued) {
            call.Queued = true;
            Ready.push_back(handle);
            return true;
        }
        return false;
    }
};

TRuntime::TRuntime(TQueryCompartmentHandlePtr query, std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> alloc,
                   std::shared_ptr<ITransport> transport, TLimits limits, std::function<void()> wakeup)
    : Query_(std::move(query)), Alloc_(std::move(alloc)), Transport_(std::move(transport)), State_(std::make_shared<TState>())
{
    Y_ENSURE(Query_ && Query_->Compartment && Query_->BridgeNodes && Alloc_ && Transport_,
             "Async runtime requires a compartment, allocator and transport");
    Y_ENSURE(limits.CpuBudget > TDuration::Zero(), "Async CPU budget must be positive");
    State_->Limits = limits;
    State_->Wakeup = std::move(wakeup);
    KeepAsyncHostIntrinsicsLinked();
    auto guard = Guard(*Alloc_);
    Query_->Compartment->SetTimeout(limits.CpuBudget);
    Query_->Compartment->StartDeadlineTimer();
    TCurrentCompartmentGuard compartmentGuard(Query_->Compartment.get());
    NYdb::NWasm::TCompartmentFunction<ui32()> version(Query_->Compartment.get(), "WasmAsyncAbiVersion");
    try {
        Y_ENSURE(version() == NYdb::NWasm::NAsync::AbiVersion, "Unsupported async WASM ABI version");
    } catch (WAVM::Runtime::Exception* exception) {
        WAVM::Runtime::destroyException(exception);
        ythrow yexception() << "Async WASM ABI version export trapped";
    }
    for (const auto* name : {"WasmAsyncCallStart", "WasmAsyncCallPoll", "WasmAsyncCallCancel", "WasmAsyncCallDrop"}) {
        Y_ENSURE(Query_->Compartment->GetFunction(name), "Missing async WASM export");
    }
}

TRuntime::~TRuntime() {
    std::lock_guard entry(EntryMutex_);
    TVector<THandle> calls;
    {
        std::lock_guard lock(State_->Mutex);
        State_->Closed = true;
        for (const auto& [handle, _] : State_->Calls) {
            calls.push_back(handle);
        }
    }
    for (const auto handle : calls) {
        try {
            DropImpl(handle);
        } catch (...) {
            Poison();
        }
    }
    auto guard = Guard(*Alloc_);
    Query_.reset();
}

void TRuntime::Enter(THandle handle, const std::function<void()>& fn, bool cleanup) {
    std::shared_ptr<TCall> call;
    {
        std::lock_guard lock(State_->Mutex);
        Y_ENSURE(!State_->Poisoned && Query_, "Async WASM instance is poisoned");
        call = State_->Call(handle);
    }
    const auto remaining = cleanup ? TDuration::MilliSeconds(10) : State_->Limits.CpuBudget - call->CpuSpent;
    Y_ENSURE(remaining > TDuration::Zero(), "Async CPU budget exhausted");
    const auto started = ThreadCPUTime();
    try {
        auto allocGuard = Guard(*Alloc_);
        TCurrentQueryCompartmentGuard queryGuard(Query_.get());
        Query_->Compartment->SetDeadline(cleanup ? TInstant::Now() + remaining : std::min(call->Deadline, TInstant::Now() + remaining));
        Query_->Compartment->StartDeadlineTimer();
        TCurrentCompartmentGuard compartmentGuard(Query_->Compartment.get());
        TWasmUdfInvocationContext context(Query_->Compartment.get());
        TCurrentInvocationContextGuard invocationGuard(&context);
        TBridgeRunScopeGuard runScope(*Query_->BridgeNodes);
        if (Query_->Resident) {
            Query_->Resident->BeginRun();
        }
        TInvocation invocation{this, handle};
        const auto previous = std::exchange(CurrentInvocation, &invocation);
        Y_DEFER {
            CurrentInvocation = previous;
        };
        fn();
    } catch (WAVM::Runtime::Exception* exception) {
        const auto message = WAVM::Runtime::describeException(exception);
        WAVM::Runtime::destroyException(exception);
        Poison();
        ythrow yexception() << "Async WASM trap: " << message;
    } catch (...) {
        // A trap can corrupt shared guest state: retire the entire compartment.
        Poison();
        throw;
    }
    call->CpuSpent += TDuration::MicroSeconds(ThreadCPUTime() - started);
}

THandle TRuntime::Start(TStringBuf arguments, TInstant deadline) {
    std::unique_lock entry(EntryMutex_, std::try_to_lock);
    Y_ENSURE(entry.owns_lock(), "Concurrent async guest entry");
    Y_ENSURE(deadline > TInstant::Now(), "Async call deadline expired");
    const auto handle = NewHandle();
    auto call = std::make_shared<TCall>();
    call->Deadline = deadline;
    {
        std::lock_guard lock(State_->Mutex);
        Y_ENSURE(!State_->Closed && !State_->Poisoned, "Async runtime is closed");
        Y_ENSURE(State_->Calls.size() < State_->Limits.MaxCalls, "Async call quota exhausted");
        State_->Reserve(arguments.size());
        call->ArgumentBytes = arguments.size();
        State_->Calls.emplace(handle, call);
    }
    try {
        Enter(handle, [&] {
            if (!arguments.empty()) {
                call->Arguments = Query_->Compartment->AllocateBytes(arguments.size());
                Y_ENSURE(call->Arguments, "Async argument allocation failed");
                std::memcpy(Query_->Compartment->GetHostPointer(call->Arguments, arguments.size()), arguments.data(), arguments.size());
            }
            NYdb::NWasm::TCompartmentFunction<ui64(ui64, ui64)> start(Query_->Compartment.get(), "WasmAsyncCallStart");
            call->Frame = start(call->Arguments, call->ArgumentBytes);
            Y_ENSURE(call->Frame, "Async guest did not create a call frame");
        });
    } catch (...) {
        DropImpl(handle);
        throw;
    }
    return handle;
}

TCallResult TRuntime::Poll(THandle handle) {
    std::unique_lock entry(EntryMutex_, std::try_to_lock);
    Y_ENSURE(entry.owns_lock(), "Concurrent async guest entry");
    std::shared_ptr<TCall> call;
    {
        std::lock_guard lock(State_->Mutex);
        call = State_->Call(handle);
        if (IsTerminal(call->Status)) {
            return {call->Status, call->Result};
        }
        if (call->TimedOut || TInstant::Now() >= call->Deadline || call->CpuSpent >= State_->Limits.CpuBudget) {
            call->Status = ECallStatus::Cancelled;
        } else if (call->Status == ECallStatus::Waiting) {
            return {call->Status, call->Result};
        }
    }
    if (call->Status == ECallStatus::Cancelled) {
        CancelAllOperations(handle);
        Enter(handle, [&] {
            NYdb::NWasm::TCompartmentFunction<void(ui64)> cancel(Query_->Compartment.get(), "WasmAsyncCallCancel");
            cancel(call->Frame);
        }, true);
    } else {
        Enter(handle, [&] {
            NYdb::NWasm::TCompartmentFunction<void(ui64)> poll(Query_->Compartment.get(), "WasmAsyncCallPoll");
            poll(call->Frame);
        });
        std::lock_guard lock(State_->Mutex);
        Y_ENSURE(call->Status != ECallStatus::Runnable || call->Queued, "Async guest returned without waiting or completing");
    }
    std::lock_guard lock(State_->Mutex);
    return {call->Status, call->Result};
}

void TRuntime::CancelAllOperations(THandle call) {
    TVector<THandle> operations;
    {
        std::lock_guard lock(State_->Mutex);
        for (const auto& [handle, op] : State_->Operations) {
            if (op.Call == call) {
                operations.push_back(handle);
            }
        }
    }
    for (const auto handle : operations) {
        CancelOperation(call, handle);
    }
}

void TRuntime::Cancel(THandle handle) {
    std::unique_lock entry(EntryMutex_, std::try_to_lock);
    Y_ENSURE(entry.owns_lock(), "Concurrent async guest entry");
    std::shared_ptr<TCall> call;
    {
        std::lock_guard lock(State_->Mutex);
        call = State_->Call(handle);
        if (IsTerminal(call->Status)) {
            return;
        }
        call->Status = ECallStatus::Cancelled;
    }
    CancelAllOperations(handle);
    Enter(handle, [&] {
        NYdb::NWasm::TCompartmentFunction<void(ui64)> cancel(Query_->Compartment.get(), "WasmAsyncCallCancel");
        cancel(call->Frame);
    }, true);
}

void TRuntime::DropImpl(THandle handle) {
    std::shared_ptr<TCall> call;
    {
        std::lock_guard lock(State_->Mutex);
        call = State_->Call(handle);
        call->Status = ECallStatus::Cancelled;
    }
    // Release host ownership even if guest cleanup traps and retires the compartment.
    Y_DEFER {
        std::lock_guard lock(State_->Mutex);
        for (auto it = State_->Operations.begin(); it != State_->Operations.end();) {
            if (it->second.Call == handle) {
                State_->Bytes -= it->second.RequestBytes + it->second.Response.size();
                State_->Operations.erase(it++);
            } else {
                ++it;
            }
        }
        State_->Bytes -= call->ArgumentBytes + call->Result.size();
        State_->Calls.erase(handle);
        State_->Ready.erase(std::remove(State_->Ready.begin(), State_->Ready.end(), handle), State_->Ready.end());
    };
    CancelAllOperations(handle);
    if (Query_) {
        Enter(handle, [&] {
            NYdb::NWasm::TCompartmentFunction<void(ui64)> drop(Query_->Compartment.get(), "WasmAsyncCallDrop");
            drop(call->Frame);
            if (call->Arguments) {
                Query_->Compartment->FreeBytes(call->Arguments);
            }
        }, true);
    }
}

void TRuntime::Drop(THandle handle) {
    std::unique_lock entry(EntryMutex_, std::try_to_lock);
    Y_ENSURE(entry.owns_lock(), "Concurrent async guest entry");
    DropImpl(handle);
}

void TRuntime::Poison() {
    TVector<ITransport::TCancel> cancels;
    bool wake = false;
    {
        std::lock_guard lock(State_->Mutex);
        State_->Poisoned = true;
        for (auto& [_, op] : State_->Operations) {
            cancels.push_back(std::move(op.Cancel));
        }
        State_->Operations.clear();
        State_->Bytes = 0;
        for (auto& [handle, call] : State_->Calls) {
            if (!IsTerminal(call->Status)) {
                wake |= State_->Schedule(handle, *call);
                call->Status = ECallStatus::Failed;
            }
            call->Arguments = call->ArgumentBytes = call->Frame = 0;
            State_->Bytes += call->Result.size();
        }
    }
    for (auto& cancel : cancels) {
        RunCancel(std::move(cancel));
    }
    auto allocGuard = Guard(*Alloc_);
    Query_.reset();
    if (wake) {
        RunCancel(State_->Wakeup);
    }
}

TVector<THandle> TRuntime::TakeReady() {
    std::lock_guard lock(State_->Mutex);
    TVector<THandle> ready;
    for (const auto handle : State_->Ready) {
        auto it = State_->Calls.find(handle);
        if (it != State_->Calls.end()) {
            it->second->Queued = false;
            ready.push_back(handle);
        }
    }
    State_->Ready.clear();
    return ready;
}

void TRuntime::Expire(TInstant now) {
    bool wake = false;
    {
        std::lock_guard lock(State_->Mutex);
        for (const auto& [handle, call] : State_->Calls) {
            if (!IsTerminal(call->Status) && call->Deadline <= now) {
                call->TimedOut = true;
                wake |= State_->Schedule(handle, *call);
            }
        }
    }
    if (wake) {
        RunCancel(State_->Wakeup);
    }
}

TInstant TRuntime::NextDeadline() const {
    std::lock_guard lock(State_->Mutex);
    auto deadline = TInstant::Max();
    for (const auto& [_, call] : State_->Calls) {
        if (!IsTerminal(call->Status)) {
            deadline = std::min(deadline, call->Deadline);
        }
    }
    return deadline;
}

TStats TRuntime::Stats() const {
    std::lock_guard lock(State_->Mutex);
    return {State_->Calls.size(), State_->Operations.size(), State_->Bytes};
}

bool TRuntime::IsPoisoned() const {
    std::lock_guard lock(State_->Mutex);
    return State_->Poisoned;
}

THandle TRuntime::StartTimer(THandle call, ui64 delayMicroseconds) {
    Y_ENSURE(delayMicroseconds <= (TInstant::Max() - TInstant::Now()).MicroSeconds(), "Async timer delay overflow");
    return StartOperation(call, TString(reinterpret_cast<const char*>(&delayMicroseconds), sizeof(delayMicroseconds)),
                          EOperationKind::Timer);
}

THandle TRuntime::StartOperation(THandle handle, TString request, EOperationKind kind) {
    const auto operation = NewHandle();
    std::shared_ptr<TCall> call;
    {
        std::lock_guard lock(State_->Mutex);
        call = State_->Call(handle);
        Y_ENSURE(!IsTerminal(call->Status) && !State_->Closed && TInstant::Now() < call->Deadline, "Async call cannot start an operation");
        Y_ENSURE(State_->Operations.size() < State_->Limits.MaxOperations, "Async operation quota exhausted");
        State_->Reserve(request.size());
        State_->Operations.emplace(operation, TState::TOperation{handle, EOperationStatus::Pending, {}, request.size(), {}});
    }
    const std::weak_ptr<TState> weakState = State_;
    auto cancel = Transport_->Start(
        operation, kind, std::move(request), call->Deadline, [weakState, handle, operation](EOperationStatus status, TString response) {
            auto state = weakState.lock();
            if (!state || status == EOperationStatus::Pending) {
                return false;
            }
            bool wake = false;
            {
                std::lock_guard lock(state->Mutex);
                auto it = state->Operations.find(operation);
                if (state->Closed || state->Poisoned || it == state->Operations.end() || it->second.Status != EOperationStatus::Pending) {
                    return false;
                }
                auto call = state->Call(handle);
                if (IsTerminal(call->Status)) {
                    return false;
                }
                if (response.size() > state->Limits.MaxPayloadBytes || response.size() > state->Limits.MaxBufferedBytes - state->Bytes) {
                    status = EOperationStatus::Failed;
                    response.clear();
                }
                state->Bytes += response.size();
                it->second.Status = status;
                it->second.Response = std::move(response);
                if (call->Status == ECallStatus::Waiting && call->WaitingFor == operation) {
                    wake = state->Schedule(handle, *call);
                }
            }
            if (wake) {
                RunCancel(state->Wakeup);
            }
            return true;
        });
    bool keep;
    {
        std::lock_guard lock(State_->Mutex);
        auto it = State_->Operations.find(operation);
        keep = it != State_->Operations.end() && it->second.Status != EOperationStatus::Cancelled;
        if (keep) {
            it->second.Cancel = std::move(cancel);
        }
    }
    if (!keep) {
        RunCancel(std::move(cancel));
    }
    return operation;
}

EOperationStatus TRuntime::PollOperation(THandle call, THandle operation) const {
    std::lock_guard lock(State_->Mutex);
    return State_->Operation(call, operation).Status;
}

TString TRuntime::ReadOperation(THandle call, THandle operation) const {
    std::lock_guard lock(State_->Mutex);
    auto& op = State_->Operation(call, operation);
    Y_ENSURE(op.Status != EOperationStatus::Pending, "Async operation is not ready");
    return op.Response;
}

void TRuntime::CancelOperation(THandle call, THandle operation) {
    ITransport::TCancel cancel;
    bool wake = false;
    {
        std::lock_guard lock(State_->Mutex);
        auto& op = State_->Operation(call, operation);
        if (op.Status == EOperationStatus::Pending) {
            op.Status = EOperationStatus::Cancelled;
        }
        cancel = std::move(op.Cancel);
        auto owner = State_->Call(call);
        if (owner->Status == ECallStatus::Waiting && owner->WaitingFor == operation) {
            wake = State_->Schedule(call, *owner);
        }
    }
    RunCancel(std::move(cancel));
    if (wake) {
        RunCancel(State_->Wakeup);
    }
}

void TRuntime::DropOperation(THandle call, THandle operation) {
    CancelOperation(call, operation);
    std::lock_guard lock(State_->Mutex);
    const auto& op = State_->Operation(call, operation);
    State_->Bytes -= op.RequestBytes + op.Response.size();
    State_->Operations.erase(operation);
}

void TRuntime::Wait(THandle handle, THandle operation) {
    bool wake = false;
    {
        std::lock_guard lock(State_->Mutex);
        const auto& op = State_->Operation(handle, operation);
        auto call = State_->Call(handle);
        Y_ENSURE(!IsTerminal(call->Status), "Cannot suspend a terminal async call");
        call->WaitingFor = operation;
        call->Status = ECallStatus::Waiting;
        // Same lock as completion: a response between poll and wait is latched.
        if (op.Status != EOperationStatus::Pending) {
            wake = State_->Schedule(handle, *call);
        }
    }
    if (wake) {
        RunCancel(State_->Wakeup);
    }
}

void TRuntime::Complete(THandle handle, TString result, EOperationStatus status) {
    std::lock_guard lock(State_->Mutex);
    auto call = State_->Call(handle);
    Y_ENSURE(!IsTerminal(call->Status) && status != EOperationStatus::Pending, "Invalid async terminal transition");
    State_->Reserve(result.size());
    call->Result = std::move(result);
    call->Status = status == EOperationStatus::Ready ? ECallStatus::Completed : ECallStatus::Failed;
}

TString TRuntime::ReadGuest(ui64 offset, ui64 size) const {
    Y_ENSURE(size <= State_->Limits.MaxPayloadBytes, "Async guest payload exceeds quota");
    return TString(static_cast<const char*>(Query_->Compartment->GetHostPointer(offset, size)), size);
}

void TRuntime::WriteGuest(ui64 offset, ui64 size, TStringBuf data) const {
    Y_ENSURE(data.size() == size && size <= State_->Limits.MaxPayloadBytes, "Invalid async response buffer");
    std::memcpy(Query_->Compartment->GetHostPointer(offset, size), data.data(), size);
}

} // namespace NKikimr::NUdfStore::NWasm::NAsync
