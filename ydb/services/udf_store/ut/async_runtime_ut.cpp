#include <ydb/services/udf_store/wasm/async_runtime/runtime.h>
#include <ydb/services/udf_store/wasm/bridge_resident.h>
#include <ydb/services/udf_store/wasm/compile.h>
#include <ydb/services/udf_store/wasm/host.h>
#include <ydb/services/udf_store/wasm/invocation_context.h>
#include <ydb/services/udf_store/wasm/registry_helpers.h>

#include <ydb/library/wasm/api/function.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <thread>

using namespace NKikimr::NUdfStore::NWasm;
using namespace NKikimr::NUdfStore::NWasm::NAsync;
using namespace NYdb::NWasm;

namespace {

TString Encode(ui64 value) {
    return TString(reinterpret_cast<const char*>(&value), sizeof(value));
}

ui64 Decode(TStringBuf value) {
    UNIT_ASSERT_VALUES_EQUAL(value.size(), sizeof(ui64));
    ui64 result;
    std::memcpy(&result, value.data(), sizeof(result));
    return result;
}

class TFakeTransport : public ITransport {
public:
    struct TRequest {
        THandle Handle;
        EOperationKind Kind;
        TString Payload;
        TInstant Deadline;
        TCompletion Completion;
        bool Cancelled = false;
    };

    TVector<std::shared_ptr<TRequest>> Requests;
    bool Immediate = false;

    TCancel Start(THandle handle, EOperationKind kind, TString request, TInstant deadline, TCompletion completion) override {
        auto item = std::make_shared<TRequest>(TRequest{handle, kind, std::move(request), deadline, std::move(completion)});
        Requests.push_back(item);
        if (Immediate) {
            item->Completion(EOperationStatus::Ready, item->Payload);
        }
        return [item] {
            item->Cancelled = true;
            item->Payload.clear();
        };
    }

    bool Reply(size_t index, ui64 value, EOperationStatus status = EOperationStatus::Ready) {
        return Requests.at(index)->Completion(status, Encode(value));
    }
};

struct TEnv {
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc =
        std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), /*initiallyAcquired=*/false);
    std::shared_ptr<TFakeTransport> Transport = std::make_shared<TFakeTransport>();
    std::shared_ptr<std::atomic<ui64>> Wakeups = std::make_shared<std::atomic<ui64>>(0);
    TQueryCompartmentHandle* Query = nullptr;
    std::unique_ptr<TRuntime> Runtime;

    explicit TEnv(TLimits limits = {}, TStringBuf wat = {}) {
        EnsureUdfHostIntrinsicsRegistered();
        KeepAsyncHostIntrinsicsLinked();
        auto query = std::make_unique<TQueryCompartmentHandle>();
        query->Generation = 17;
        query->BridgeNodes = std::make_unique<TWasmBridgeNodeTable>(query->Generation);
        query->Compartment = CreateRegistryCompartment({});
        const auto bytes = wat.empty() ? NResource::Find("/async_coroutine.wasm") : TString(wat);
        UNIT_ASSERT(!bytes.empty());
        const auto format = wat.empty() ? EBytecodeFormat::Binary : EBytecodeFormat::HumanReadable;
        const auto object = CompileModuleObjectCode(bytes, format);
        AddPrecompiledModule(query->Compartment.get(), MakeModuleBytecode(bytes, object, format), "AsyncFixture");
        query->Resident = std::make_unique<TCompartmentResidentCache>(query->Compartment.get());
        Query = query.get();
        Runtime = std::make_unique<TRuntime>(std::move(query), Alloc, Transport, limits, [wakeups = Wakeups] { ++*wakeups; });
    }

    THandle Start(ui64 mode, ui64 value = 10, TInstant deadline = TInstant::Now() + TDuration::Seconds(30)) {
        TString arguments = Encode(mode) + Encode(value);
        return Runtime->Start(arguments, deadline);
    }

    ui64 LiveObjects() {
        auto guard = Guard(*Alloc);
        TCurrentCompartmentGuard compartmentGuard(Query->Compartment.get());
        TCompartmentFunction<ui64()> live(Query->Compartment.get(), "WasmAsyncLiveObjects");
        return live();
    }

    void AssertClean() {
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().Calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().Operations, 0);
        UNIT_ASSERT_VALUES_EQUAL(Runtime->Stats().BufferedBytes, 0);
        UNIT_ASSERT(Runtime->TakeReady().empty());
        UNIT_ASSERT_VALUES_EQUAL(LiveObjects(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Query->BridgeNodes->DebugSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Query->BridgeNodes->DebugRunScopeDepth(), 0);
        UNIT_ASSERT(!Alloc->IsAttached());
        UNIT_ASSERT(!GetCurrentAsyncInvocation());
        UNIT_ASSERT(!GetCurrentInvocationContext());
        UNIT_ASSERT(!GetCurrentQueryCompartment());
        UNIT_ASSERT(!GetCurrentCompartment());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TWasmAsyncRuntimeTest) {
    Y_UNIT_TEST(ImmediateGuestResultAndTerminalPoll) {
        TEnv env;
        const auto call = env.Start(0, 42);
        auto result = env.Runtime->Poll(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Decode(result.Data), 42);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Poll(call).Data, result.Data);
        UNIT_ASSERT(env.Transport->Requests.empty());
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(SequentialNestedSuspendsKeepArgumentsAndDeadline) {
        TEnv env;
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        const auto call = env.Start(1, 11, deadline);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        const auto suspendedObjects = env.LiveObjects();
        UNIT_ASSERT(suspendedObjects > 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Requests[0]->Deadline, deadline);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), deadline);

        const auto other = env.Start(0, 99);
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(other).Data), 99);
        env.Runtime->Drop(other);
        UNIT_ASSERT_VALUES_EQUAL(env.LiveObjects(), suspendedObjects);
        UNIT_ASSERT(env.Transport->Reply(0, 20));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(env.LiveObjects(), suspendedObjects);
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Transport->Requests[1]->Payload), 31);
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Requests[1]->Deadline, deadline);
        UNIT_ASSERT(env.Transport->Reply(1, 50));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(call).Data), 50);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(ParallelOperationsCompleteOutOfOrder) {
        TEnv env;
        const auto call = env.Start(2);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Requests.size(), 2);
        UNIT_ASSERT(env.Transport->Reply(1, 30));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Transport->Reply(0, 12));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(call).Data), 42);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(SeveralCallsInOneCompartment) {
        TEnv env;
        const auto a = env.Start(3, 1);
        const auto b = env.Start(3, 2);
        UNIT_ASSERT(env.Runtime->Poll(a).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Runtime->Poll(b).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Transport->Reply(1, 22));
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(b).Data), 22);
        UNIT_ASSERT(env.Transport->Reply(0, 11));
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(a).Data), 11);
        env.Runtime->Drop(a);
        env.Runtime->Drop(b);
        env.AssertClean();
    }

    Y_UNIT_TEST(CompletionBeforeWaitAndDuplicateCompletion) {
        TEnv env;
        const auto call = env.Start(0);
        const auto operation = env.Runtime->StartOperation(call, Encode(1));
        UNIT_ASSERT(env.Runtime->PollOperation(call, operation) == EOperationStatus::Pending);
        UNIT_ASSERT(env.Transport->Reply(0, 123));
        UNIT_ASSERT(!env.Transport->Reply(0, 999));
        env.Runtime->Wait(call, operation);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT_VALUES_EQUAL(env.Wakeups->load(), 1);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(SynchronousTransportCompletion) {
        TEnv env;
        env.Transport->Immediate = true;
        const auto call = env.Start(1, 7);
        const auto result = env.Runtime->Poll(call);
        UNIT_ASSERT(result.Status == ECallStatus::Completed);
        UNIT_ASSERT_VALUES_EQUAL(Decode(result.Data), 14);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Operations, 0);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(CallbackThreadNeverEntersGuest) {
        TEnv env;
        const auto call = env.Start(3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        const auto live = env.LiveObjects();
        std::atomic<bool> accepted{false};
        std::atomic<bool> cleanTls{false};
        std::thread thread([&] {
            accepted = env.Transport->Reply(0, 42);
            cleanTls = !GetCurrentAsyncInvocation() && !GetCurrentQueryCompartment() && !GetCurrentCompartment();
        });
        thread.join();
        UNIT_ASSERT(accepted && cleanTls);
        UNIT_ASSERT_VALUES_EQUAL(env.LiveObjects(), live);
        UNIT_ASSERT_VALUES_EQUAL(env.Wakeups->load(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(call).Data), 42);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(CancelDropsNestedFramesAndRejectsLateReply) {
        TEnv env;
        const auto call = env.Start(3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        env.Runtime->Cancel(call);
        env.Runtime->Cancel(call);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
        UNIT_ASSERT(env.Transport->Requests[0]->Cancelled);
        UNIT_ASSERT(!env.Transport->Reply(0, 42));
        UNIT_ASSERT_VALUES_EQUAL(env.LiveObjects(), 1);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(TimeoutWakesWaitingCallWithoutResettingDeadline) {
        TEnv env;
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        const auto call = env.Start(3, 10, deadline);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        env.Runtime->Expire(deadline);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Cancelled);
        UNIT_ASSERT(!env.Transport->Reply(0, 42));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->NextDeadline(), TInstant::Max());
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(TransportErrorIsAResult) {
        TEnv env;
        const auto call = env.Start(3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Transport->Reply(0, 0, EOperationStatus::Failed));
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        UNIT_ASSERT(!env.Runtime->IsPoisoned());
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(ControlledTimerSuspendsAndResumes) {
        TEnv env;
        const auto call = env.Start(6, 42);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Transport->Requests[0]->Kind == EOperationKind::Timer);
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Transport->Requests[0]->Payload), 1000);
        UNIT_ASSERT(env.Transport->Reply(0, 0));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->TakeReady(), TVector<THandle>{call});
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(call).Data), 42);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(StaleHandlesAndDifferentOwnerRejected) {
        TEnv env;
        const auto a = env.Start(3);
        const auto b = env.Start(0);
        UNIT_ASSERT(env.Runtime->Poll(a).Status == ECallStatus::Waiting);
        const auto operation = env.Transport->Requests[0]->Handle;
        UNIT_ASSERT_EXCEPTION(env.Runtime->PollOperation(b, operation), yexception);
        env.Runtime->Drop(a);
        UNIT_ASSERT_EXCEPTION(env.Runtime->Poll(a), yexception);
        UNIT_ASSERT_EXCEPTION(env.Runtime->PollOperation(a, operation), yexception);
        const auto c = env.Start(0);
        UNIT_ASSERT(a != c);
        TEnv other;
        UNIT_ASSERT_EXCEPTION(other.Runtime->Poll(c), yexception);
        env.Runtime->Drop(b);
        env.Runtime->Drop(c);
        env.AssertClean();
        other.AssertClean();
    }

    Y_UNIT_TEST(TrapRetiresSharedCompartmentAndCancelsOtherCalls) {
        TEnv env;
        const auto completed = env.Start(0, 17);
        UNIT_ASSERT(env.Runtime->Poll(completed).Status == ECallStatus::Completed);
        const auto waiting = env.Start(3);
        const auto trapping = env.Start(4);
        UNIT_ASSERT(env.Runtime->Poll(waiting).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Runtime->Poll(trapping).Status == ECallStatus::Waiting);
        UNIT_ASSERT(env.Transport->Reply(1, 42));
        UNIT_ASSERT_EXCEPTION(env.Runtime->Poll(trapping), yexception);
        UNIT_ASSERT(env.Runtime->IsPoisoned());
        UNIT_ASSERT(env.Runtime->Poll(waiting).Status == ECallStatus::Failed);
        UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(completed).Data), 17);
        UNIT_ASSERT(env.Runtime->Poll(completed).Status == ECallStatus::Completed);
        const auto ready = env.Runtime->TakeReady();
        UNIT_ASSERT(std::find(ready.begin(), ready.end(), waiting) != ready.end());
        UNIT_ASSERT(env.Transport->Requests[0]->Cancelled);
        UNIT_ASSERT(!env.Transport->Reply(0, 7));
        env.Runtime->Drop(waiting);
        env.Runtime->Drop(trapping);
        env.Runtime->Drop(completed);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().BufferedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Operations, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Calls, 0);
        UNIT_ASSERT(!GetCurrentQueryCompartment());
        UNIT_ASSERT(!GetCurrentCompartment());
    }

    Y_UNIT_TEST(CpuBoundGuestIsInterrupted) {
        TLimits limits;
        limits.CpuBudget = TDuration::MilliSeconds(20);
        TEnv env(limits);
        const auto call = env.Start(5);
        UNIT_ASSERT_EXCEPTION(env.Runtime->Poll(call), yexception);
        UNIT_ASSERT(env.Runtime->IsPoisoned());
        env.Runtime->Drop(call);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().BufferedBytes, 0);
    }

    Y_UNIT_TEST(ForcedTeardownRejectsCallbackAfterOwnerDestruction) {
        TEnv env;
        const auto call = env.Start(3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        env.Runtime.reset();
        UNIT_ASSERT(env.Transport->Requests[0]->Cancelled);
        UNIT_ASSERT(!env.Transport->Reply(0, 42));
        UNIT_ASSERT(!GetCurrentAsyncInvocation());
        UNIT_ASSERT(!GetCurrentQueryCompartment());
        UNIT_ASSERT(!GetCurrentCompartment());
    }

    Y_UNIT_TEST(RepeatedCreationReturnsAllCountersToBaseline) {
        TEnv env;
        env.Transport->Immediate = true;
        for (unsigned i = 0; i < 100; ++i) {
            const auto call = env.Start(1, i);
            UNIT_ASSERT_VALUES_EQUAL(Decode(env.Runtime->Poll(call).Data), i * 2);
            env.Runtime->Drop(call);
            env.AssertClean();
        }
    }

    Y_UNIT_TEST(CallAndPayloadQuotasCheckedBeforeGuestEntry) {
        TLimits limits;
        limits.MaxCalls = 1;
        limits.MaxPayloadBytes = 16;
        TEnv env(limits);
        const auto call = env.Start(0);
        const auto live = env.LiveObjects();
        UNIT_ASSERT_EXCEPTION(env.Start(0), yexception);
        UNIT_ASSERT_VALUES_EQUAL(env.LiveObjects(), live);
        env.Runtime->Drop(call);
        UNIT_ASSERT_EXCEPTION(env.Runtime->Start(TString(17, 'x'), TInstant::Now() + TDuration::Seconds(30)), yexception);
        env.AssertClean();
    }

    Y_UNIT_TEST(OversizedResponseFailsWithoutGrowingBuffers) {
        TLimits limits;
        limits.MaxPayloadBytes = 16;
        TEnv env(limits);
        const auto call = env.Start(3);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Waiting);
        const auto bytes = env.Runtime->Stats().BufferedBytes;
        UNIT_ASSERT(env.Transport->Requests[0]->Completion(EOperationStatus::Ready, TString(17, 'x')));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().BufferedBytes, bytes);
        UNIT_ASSERT(env.Runtime->Poll(call).Status == ECallStatus::Failed);
        env.Runtime->Drop(call);
        env.AssertClean();
    }

    Y_UNIT_TEST(AbiVersionIsRejectedBeforeAnyCall) {
        constexpr TStringBuf wat = R"((module
            (func (export "WasmAsyncAbiVersion") (result i32) (i32.const 2))
        ))";
        UNIT_ASSERT_EXCEPTION(TEnv({}, wat), yexception);
        UNIT_ASSERT(!GetCurrentCompartment());
    }

    Y_UNIT_TEST(GuestCleanupTrapStillReleasesHostOwnership) {
        constexpr TStringBuf wat = R"((module
            (func (export "WasmAsyncAbiVersion") (result i32) (i32.const 1))
            (func (export "WasmAsyncCallStart") (param i64 i64) (result i64) (i64.const 1))
            (func (export "WasmAsyncCallPoll") (param i64))
            (func (export "WasmAsyncCallCancel") (param i64))
            (func (export "WasmAsyncCallDrop") (param i64) unreachable)
        ))";
        TEnv env({}, wat);
        const auto call = env.Runtime->Start({}, TInstant::Now() + TDuration::Seconds(30));
        env.Runtime->StartOperation(call, Encode(1));
        UNIT_ASSERT_EXCEPTION(env.Runtime->Drop(call), yexception);
        UNIT_ASSERT(env.Runtime->IsPoisoned());
        UNIT_ASSERT(env.Transport->Requests[0]->Cancelled);
        UNIT_ASSERT(!env.Transport->Reply(0, 42));
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Calls, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Operations, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().BufferedBytes, 0);
        UNIT_ASSERT(env.Runtime->TakeReady().empty());
        UNIT_ASSERT(!GetCurrentAsyncInvocation());
        UNIT_ASSERT(!GetCurrentQueryCompartment());
        UNIT_ASSERT(!GetCurrentCompartment());
    }

    Y_UNIT_TEST(OperationQuotaFailureRetiresAndCleansInstance) {
        TLimits limits;
        limits.MaxOperations = 1;
        TEnv env(limits);
        const auto call = env.Start(2);
        UNIT_ASSERT_EXCEPTION(env.Runtime->Poll(call), yexception);
        UNIT_ASSERT(env.Runtime->IsPoisoned());
        UNIT_ASSERT_VALUES_EQUAL(env.Transport->Requests.size(), 1);
        UNIT_ASSERT(env.Transport->Requests[0]->Cancelled);
        env.Runtime->Drop(call);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().BufferedBytes, 0);
        UNIT_ASSERT_VALUES_EQUAL(env.Runtime->Stats().Operations, 0);
    }
}
