#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints_states.h>
#include <ydb/library/yql/minikql/computation/mkql_value_builder.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/retry/retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <queue>

namespace NYql::NDq {

class TFakeActor;

using TRuntimePtr = std::unique_ptr<NActors::TTestActorRuntime>;
using TCallback = std::function<void(TFakeActor&)>;
template<typename T>
using TReadValueParser = std::function<std::vector<T>(const NUdf::TUnboxedValue&)>;
using TWriteValueProducer = std::function<NKikimr::NMiniKQL::TUnboxedValueBatch(NKikimr::NMiniKQL::THolderFactory&)>;

namespace {
    struct TEvPrivate {
        // Event ids
        enum EEv : ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

            EvExecute = EvBegin,

            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        // Events

        struct TEvExecute : public NActors::TEventLocal<TEvExecute, EvExecute> {
            TEvExecute(NThreading::TPromise<void>& promise, TCallback callback, std::exception_ptr& resultException)
                : Promise(promise)
                , Callback(callback)
                , ResultException(resultException)
            {}

            NThreading::TPromise<void> Promise;
            TCallback Callback;
            std::exception_ptr& ResultException;
        };
    };
}

struct TAsyncInputPromises {
    NThreading::TPromise<void> NewAsyncInputDataArrived = NThreading::NewPromise();
    NThreading::TPromise<TIssues> FatalError = NThreading::NewPromise<TIssues>();
};

struct TAsyncOutputPromises {
    NThreading::TPromise<void> ResumeExecution = NThreading::NewPromise();
    NThreading::TPromise<TIssues> Issue = NThreading::NewPromise<TIssues>();
    NThreading::TPromise<TSinkState> StateSaved = NThreading::NewPromise<TSinkState>();
};

NYql::NDqProto::TCheckpoint CreateCheckpoint(ui64 id = 0);

class TFakeActor : public NActors::TActor<TFakeActor> {
    struct TAsyncInputEvents {
        explicit TAsyncInputEvents(TFakeActor& parent) : Parent(parent) {}

        void OnNewAsyncInputDataArrived(ui64) {
            Parent.AsyncInputPromises.NewAsyncInputDataArrived.SetValue();
            Parent.AsyncInputPromises.NewAsyncInputDataArrived = NThreading::NewPromise();
        }

        void OnAsyncInputError(ui64, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) {
            Y_UNUSED(fatalCode);
            Parent.AsyncInputPromises.FatalError.SetValue(issues);
            Parent.AsyncInputPromises.FatalError = NThreading::NewPromise<TIssues>();
        }

        TFakeActor& Parent;
    };

    struct TAsyncOutputCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
        explicit TAsyncOutputCallbacks(TFakeActor& parent) : Parent(parent) {}

        void ResumeExecution(EResumeSource) override {
            Parent.AsyncOutputPromises.ResumeExecution.SetValue();
            Parent.AsyncOutputPromises.ResumeExecution = NThreading::NewPromise();
        };

        void OnAsyncOutputError(ui64, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override {
            Y_UNUSED(fatalCode);
            Parent.AsyncOutputPromises.Issue.SetValue(issues);
            Parent.AsyncOutputPromises.Issue = NThreading::NewPromise<TIssues>();
        };

        void OnAsyncOutputStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint&) override {
            Y_UNUSED(outputIndex);
            Parent.AsyncOutputPromises.StateSaved.SetValue(state);
            Parent.AsyncOutputPromises.StateSaved = NThreading::NewPromise<TSinkState>();
        };

        void OnAsyncOutputFinished(ui64 outputIndex) override {
            Y_UNUSED(outputIndex);
        }

        TFakeActor& Parent;
    };

public:
    TFakeActor(TAsyncInputPromises& sourcePromises, TAsyncOutputPromises& asyncOutputPromises);
    ~TFakeActor();

    void InitAsyncOutput(IDqComputeActorAsyncOutput* dqAsyncOutput, IActor* dqAsyncOutputAsActor);
    void InitAsyncInput(IDqComputeActorAsyncInput* dqAsyncInput, IActor* dqAsyncInputAsActor);
    void Terminate(std::shared_ptr<std::atomic<bool>> done);

    TAsyncOutputCallbacks& GetAsyncOutputCallbacks();
    NKikimr::NMiniKQL::THolderFactory& GetHolderFactory();

public:
    IDqComputeActorAsyncInput* DqAsyncInput = nullptr;
    IDqComputeActorAsyncOutput* DqAsyncOutput = nullptr;
    std::optional<NActors::TActorId> DqAsyncInputActorId;

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvExecute, Handle);
        hFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived, Handle);
        hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, Handle);
    )

    void Handle(TEvPrivate::TEvExecute::TPtr& ev) {
        TGuard<NKikimr::NMiniKQL::TScopedAlloc> guard(Alloc);
        try {
            ev->Get()->Callback(*this);
        } catch (...) {
            ev->Get()->ResultException = std::current_exception();
        }
        ev->Get()->Promise.SetValue();
    }

    void Handle(const IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr& ev) {
        AsyncInputEvents.OnNewAsyncInputDataArrived(ev->Get()->InputIndex);
    }

    void Handle(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        AsyncInputEvents.OnAsyncInputError(ev->Get()->InputIndex, ev->Get()->Issues, ev->Get()->FatalCode);
    }

public:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemoryInfo;
    NKikimr::NMiniKQL::THolderFactory HolderFactory;

    NKikimr::NMiniKQL::TTypeEnvironment TypeEnv;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry;
    NKikimr::NMiniKQL::TProgramBuilder ProgramBuilder;
    NKikimr::NMiniKQL::TDefaultValueBuilder ValueBuilder;

private:
    IActor* DqAsyncInputAsActor = nullptr;

    std::optional<NActors::TActorId> DqAsyncOutputActorId;
    IActor* DqAsyncOutputAsActor = nullptr;

    TAsyncInputEvents AsyncInputEvents;
    TAsyncOutputCallbacks AsyncOutputCallbacks;

    TAsyncInputPromises& AsyncInputPromises;
    TAsyncOutputPromises& AsyncOutputPromises;
};

struct TFakeCASetup {
    TFakeCASetup();
    ~TFakeCASetup();

    template<typename T>
    std::vector<std::variant<T, TInstant>> AsyncInputRead(
        const TReadValueParser<T> parser,
        NThreading::TFuture<void>& nextDataFutureOut,
        i64 freeSpace = 12345)
    {
        std::vector<std::variant<T, TInstant>> result;
        NThreading::TFuture<bool> nextDataFuture;
        Execute([&result, &parser, freeSpace, &nextDataFutureOut, this](TFakeActor& actor) {
            TMaybe<TInstant> watermark;
            NKikimr::NMiniKQL::TUnboxedValueBatch buffer;
            bool finished = false;
            actor.DqAsyncInput->GetAsyncInputData(buffer, watermark, finished, freeSpace);

            buffer.ForEachRow([&](const NUdf::TUnboxedValue& value) {
                for (const auto item : parser(value)) {
                    result.emplace_back(item);
                }
            });

            if (watermark) {
                result.emplace_back(*watermark);
            }

            nextDataFutureOut = AsyncInputPromises.NewAsyncInputDataArrived.GetFuture();
        });

        return result;
    }

    template<typename T>
    std::vector<std::variant<T, TInstant>> AsyncInputReadUntil(
        const TReadValueParser<T> parser,
        ui64 size,
        i64 eachReadFreeSpace = 1000,
        TDuration timeout = TDuration::Seconds(30),
        bool onlyData = false)
    {
        ui32 dataItemsCt = 0;
        std::vector<std::variant<T, TInstant>> result;
        TInstant startedAt = TInstant::Now();
        DoWithRetry([&](){
                NThreading::TFuture<void> nextDataFuture;
                auto batch = AsyncInputRead<T>(parser, nextDataFuture, eachReadFreeSpace);
                for (const auto& item : batch) {
                    result.emplace_back(item);
                    if (!onlyData || std::holds_alternative<T>(item)) {
                        dataItemsCt++;
                    }
                }

                if (TInstant::Now() > startedAt + timeout) {
                    return;
                }

                if (dataItemsCt < size) {
                    nextDataFuture.Wait(timeout);
                    ythrow yexception() << "Not enough items";
                }
            },
            TRetryOptions(std::numeric_limits<ui32>::max()),
            true);

        UNIT_ASSERT_C(dataItemsCt >= size, "Waited for " << size << " items but only " << dataItemsCt << " received");

        return result;
    }

    void AsyncOutputWrite(const TWriteValueProducer valueProducer, TMaybe<NDqProto::TCheckpoint> checkpoint = Nothing(), bool finish = false);

    void SaveSourceState(NDqProto::TCheckpoint checkpoint, TSourceState& state);

    void LoadSource(const TSourceState& state);
    void LoadSink(const TSinkState& state);

    void Execute(TCallback callback);

public:
    TRuntimePtr Runtime;
    NActors::TActorId FakeActorId;
    TAsyncInputPromises AsyncInputPromises;
    TAsyncOutputPromises AsyncOutputPromises;
};

} // namespace NKikimr::NMiniKQL
