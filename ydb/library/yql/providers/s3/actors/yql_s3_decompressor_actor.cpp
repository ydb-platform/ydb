#include <queue>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_coroutine.h>
#include <ydb/library/yql/dq/actors/compute/dq_schedulable.h>
#include <ydb/library/yql/providers/s3/compressors/factory.h>
#include <ydb/library/yql/providers/s3/events/events.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>

#if defined(_linux_) || defined(_darwin_)
#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>
#endif

namespace NYql::NDq {

using namespace ::NActors;

namespace {

class TS3DecompressorCoroImpl : public TActorCoroImpl {
public:
    TS3DecompressorCoroImpl(const TActorId& parent, const TString& compression, IDqSchedulableWorkFactoryPtr workFactory)
        : TActorCoroImpl(256_KB)
        , Compression(compression)
        , Parent(parent)
        , WorkFactory(std::move(workFactory))
        , Work(WorkFactory ? WorkFactory->CreateSchedulableWork() : nullptr)
    {}

private:
    class TCoroReadBuffer : public NDB::ReadBuffer {
    public:
        explicit TCoroReadBuffer(TS3DecompressorCoroImpl* coro)
            : NDB::ReadBuffer(nullptr, 0)
            , Coro(coro)
        {}

    private:
        bool nextImpl() final {
            while (true) {
                if (Coro->InputBuffer) {
                    RawDataBuffer.swap(Coro->InputBuffer);
                    Coro->InputBuffer.clear();
                    auto rawData = const_cast<char*>(RawDataBuffer.data());
                    working_buffer = NDB::BufferBase::Buffer(rawData, rawData + RawDataBuffer.size());
                    return true;
                }
                if (Coro->InputFinished && Coro->Requests.empty()) {
                    break;
                }
                Coro->CpuTime += Coro->GetCpuTimeDelta();
                Coro->ProcessOneEvent();
                Coro->StartCycleCount = GetCycleCountFast();
            }
            return false;
        }

        TS3DecompressorCoroImpl* const Coro;
        TString RawDataBuffer;
    };

    STRICT_STFUNC(StateFunc,
        hFunc(TEvS3Provider::TEvDecompressDataRequest, Handle);
        hFunc(NActors::TEvents::TEvPoison, Handle);
        sFunc(NActors::TEvents::TEvWakeup, HandleWakeup);
    )

    // CPU scheduler (TQuery::ResumeTasks) may send multiple TEvWakeups while we
    // are throttled — one per peer's StopExecution. StartUnit's WaitForSpecificEvent
    // consumes only the first; extras leak into the general event flow and reach
    // StateFunc via WaitForEvent in ProcessOneEvent. Ignore them here; StartUnit
    // re-checks TryStartExecution on every wakeup anyway.
    void HandleWakeup() {}

    void Handle(TEvS3Provider::TEvDecompressDataRequest::TPtr& ev) {
        Requests.push(std::move(ev->Release()));
    }

    void Handle(NActors::TEvents::TEvPoison::TPtr& ev) {
        if (ev->Cookie) {
            ythrow yexception() << "S3 decompressor actor abort";
        }
        InputFinished = true;
    }

    void StartUnit() {
        if (!Work || Working) {
            return;
        }
        for (;;) {
            const auto now = TMonotonic::Now();
            const auto delay = Work->TryStartExecution(now);
            if (!delay) {
                break;
            }
            // A delivered event means the scheduler woke us up.
            const auto resumeEv = WaitForSpecificEvent<NActors::TEvents::TEvWakeup>(
                &TS3DecompressorCoroImpl::ProcessUnexpectedEvent,
                now + *delay);
            Work->NotifyResumed(/* byScheduler = */ static_cast<bool>(resumeEv));
        }
        Working = true;
    }

    void ProcessUnexpectedEvent(TAutoPtr<::NActors::IEventHandle> ev) {
        StateFunc(ev);
    }

    void StopUnit() {
        if (!Work || !Working) {
            return;
        }
        Work->StopExecution();
        Working = false;
    }

    void SetUpstreamPause(bool paused) {
        if (UpstreamPaused == paused) {
            return;
        }
        UpstreamPaused = paused;
        if (UpstreamPaused) {
            StopUnit();
        } else {
            StartUnit();
        }
    }

    void Run() final {
        StartCycleCount = GetCycleCountFast();

        if (Work) {
            Work->RegisterForResume(SelfActorId);
        }

        try {
            std::unique_ptr<NDB::ReadBuffer> coroBuffer = std::make_unique<TCoroReadBuffer>(this);
            NDB::ReadBuffer* buffer = coroBuffer.get();
            auto decompressorBuffer = MakeDecompressor(*buffer, Compression);
            YQL_ENSURE(decompressorBuffer, "Unsupported " << Compression << " compression.");
            while (!decompressorBuffer->eof()) {
                decompressorBuffer->nextIfAtEnd();
                StartUnit();
                Y_DEFER { StopUnit(); };
                TString data{decompressorBuffer->available(), ' '};
                decompressorBuffer->read(&data.front(), decompressorBuffer->available());
                Send(Parent, new TEvS3Provider::TEvDecompressDataResult(std::move(data), TakeCpuTimeDelta()));
            }
        } catch (const TDtorException&) {
            // Stop any activity instantly
            return;
        } catch (...) {
            Send(Parent, new TEvS3Provider::TEvDecompressDataResult(std::current_exception(), TakeCpuTimeDelta()));
        }
        Send(Parent, new TEvS3Provider::TEvDecompressDataFinish(TakeCpuTimeDelta()));
    }

    void ProcessOneEvent() {
        if (!Requests.empty()) {
            ExtractDataPart(*Requests.front());
            Requests.pop();
            return;
        }

        SetUpstreamPause(true);

        TAutoPtr<::NActors::IEventHandle> ev(WaitForEvent().Release());
        StateFunc(ev);

        SetUpstreamPause(false);
    }

    void ExtractDataPart(TEvS3Provider::TEvDecompressDataRequest& event) {
        InputBuffer = std::move(event.Data);
    }

    TDuration GetCpuTimeDelta() const {
        return TDuration::Seconds(NHPTimer::GetSeconds(GetCycleCountFast() - StartCycleCount));
    }

    TDuration TakeCpuTimeDelta() {
        auto currentCpuTime = CpuTime;
        CpuTime = TDuration::Zero();
        return currentCpuTime;
    }

private:
    TDuration CpuTime;
    ui64 StartCycleCount = 0;
    TString InputBuffer;
    TString Compression;
    TActorId Parent;
    bool InputFinished = false;
    std::queue<THolder<TEvS3Provider::TEvDecompressDataRequest>> Requests;
    const IDqSchedulableWorkFactoryPtr WorkFactory;
    std::unique_ptr<IDqSchedulableWork> Work;
    bool Working = false;            // holds HDRF slot — allowed to consume CPU
    bool UpstreamPaused = false;     // waiting on decompress input / HDRF admission — stop consuming
};

class TS3DecompressorCoroActor : public TActorCoro {
public:
    explicit TS3DecompressorCoroActor(THolder<TS3DecompressorCoroImpl> impl)
        : TActorCoro(std::move(impl))
    {}

private:
    void Registered(TActorSystem* actorSystem, const TActorId& parent) override {
        TActorCoro::Registered(actorSystem, parent); // Calls TActorCoro::OnRegister and sends bootstrap event to ourself.
    }
};

} // anonymous namespace

NActors::IActor* CreateS3DecompressorActor(const NActors::TActorId& parent, const TString& compression, IDqSchedulableWorkFactoryPtr workFactory) {
    return new TS3DecompressorCoroActor(MakeHolder<TS3DecompressorCoroImpl>(parent, compression, std::move(workFactory)));
}

} // namespace NYql::NDq
