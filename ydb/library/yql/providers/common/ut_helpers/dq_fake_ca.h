#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/proto/dq_checkpoint.pb.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>

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
using TWriteValueProducer = std::function<NKikimr::NMiniKQL::TUnboxedValueVector(NKikimr::NMiniKQL::THolderFactory&)>;

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

struct TSourcePromises {
    NThreading::TPromise<void> NewSourceDataArrived = NThreading::NewPromise();
    NThreading::TPromise<TIssues> FatalError = NThreading::NewPromise<TIssues>();
};

struct TSinkPromises {
    NThreading::TPromise<void> ResumeExecution = NThreading::NewPromise();
    NThreading::TPromise<TIssues> Issue = NThreading::NewPromise<TIssues>();
    NThreading::TPromise<NDqProto::TSinkState> StateSaved = NThreading::NewPromise<NDqProto::TSinkState>(); 
};

NYql::NDqProto::TCheckpoint CreateCheckpoint(ui64 id = 0);

class TFakeActor : public NActors::TActor<TFakeActor> {
    struct TSourceCallbacks : public IDqSourceActor::ICallbacks {
        explicit TSourceCallbacks(TFakeActor& parent) : Parent(parent) {}

        void OnNewSourceDataArrived(ui64) override {
            Parent.SourcePromises.NewSourceDataArrived.SetValue();
            Parent.SourcePromises.NewSourceDataArrived = NThreading::NewPromise();
        }

        void OnSourceError(ui64, const TIssues& issues, bool isFatal) override {
            Y_UNUSED(isFatal);
            Parent.SourcePromises.FatalError.SetValue(issues);
            Parent.SourcePromises.FatalError = NThreading::NewPromise<TIssues>();
        }

        TFakeActor& Parent;
    };

    struct TSinkCallbacks : public IDqSinkActor::ICallbacks {
        explicit TSinkCallbacks(TFakeActor& parent) : Parent(parent) {}

        void ResumeExecution() override {
            Parent.SinkPromises.ResumeExecution.SetValue();
            Parent.SinkPromises.ResumeExecution = NThreading::NewPromise();
        };

        void OnSinkError(ui64, const TIssues& issues, bool isFatal) override {
            Y_UNUSED(isFatal);
            Parent.SinkPromises.Issue.SetValue(issues);
            Parent.SinkPromises.Issue = NThreading::NewPromise<TIssues>();
        };

        void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint&) override { 
            Y_UNUSED(outputIndex);
            Parent.SinkPromises.StateSaved.SetValue(state);
            Parent.SinkPromises.StateSaved = NThreading::NewPromise<NDqProto::TSinkState>(); 
        };

        TFakeActor& Parent;
    };

public:
    TFakeActor(TSourcePromises& sourcePromises, TSinkPromises& sinkPromises);
    ~TFakeActor();

    void InitSink(IDqSinkActor* dqSink, IActor* dqSinkAsActor);
    void InitSource(IDqSourceActor* dqSource, IActor* dqSourceAsActor);
    void Terminate();

    TSourceCallbacks& GetSourceCallbacks();
    TSinkCallbacks& GetSinkCallbacks();
    NKikimr::NMiniKQL::THolderFactory& GetHolderFactory();

public:
    IDqSourceActor* DqSourceActor = nullptr;
    IDqSinkActor* DqSinkActor = nullptr;

private:
    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvExecute, Handle);
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

private:
    NKikimr::NMiniKQL::TScopedAlloc Alloc;
    NKikimr::NMiniKQL::TMemoryUsageInfo MemoryInfo;
    NKikimr::NMiniKQL::THolderFactory HolderFactory;

    std::optional<NActors::TActorId> DqSourceActorId;
    IActor* DqSourceActorAsActor = nullptr;

    std::optional<NActors::TActorId> DqSinkActorId;
    IActor* DqSinkActorAsActor = nullptr;

    TSourceCallbacks SourceCallbacks;
    TSinkCallbacks SinkCallbacks;

    TSourcePromises& SourcePromises;
    TSinkPromises& SinkPromises;
};

struct TFakeCASetup {
    TFakeCASetup();
    ~TFakeCASetup();

    template<typename T>
    std::vector<T> SourceRead(const TReadValueParser<T> parser, i64 freeSpace = 12345) {
        std::vector<T> result;
        Execute([&result, &parser, freeSpace](TFakeActor& actor) {
            NKikimr::NMiniKQL::TUnboxedValueVector buffer;
            bool finished = false;
            actor.DqSourceActor->GetSourceData(buffer, finished, freeSpace);

            for (const auto& uv : buffer) {
                for (const auto item : parser(uv)) {
                    result.emplace_back(item);
                }
            }
        });

        return result;
    }

    template<typename T>
    std::vector<T> SourceReadUntil(
        const TReadValueParser<T> parser,
        ui64 size,
        i64 eachReadFreeSpace = 1000,
        TDuration timeout = TDuration::Seconds(10))
    {
        std::vector<T> result;
        DoWithRetry([&](){
                auto batch = SourceRead<T>(parser, eachReadFreeSpace);
                for (const auto& item : batch) {
                    result.emplace_back(item);
                }

                if (result.size() < size) {
                    SourcePromises.NewSourceDataArrived.GetFuture().Wait(timeout);
                    ythrow yexception() << "Not enough data";
                }
            },
            TRetryOptions(3),
            false);

        return result;
    }

    void SinkWrite(const TWriteValueProducer valueProducer, TMaybe<NDqProto::TCheckpoint> checkpoint = Nothing());

    void SaveSourceState(NDqProto::TCheckpoint checkpoint, NDqProto::TSourceState& state); 

    void LoadSource(const NDqProto::TSourceState& state); 
    void LoadSink(const NDqProto::TSinkState& state); 

    void Execute(TCallback callback);

public:
    TRuntimePtr Runtime;
    NActors::TActorId FakeActorId;
    TSourcePromises SourcePromises;
    TSinkPromises SinkPromises;
};

} // namespace NKikimr::NMiniKQL
