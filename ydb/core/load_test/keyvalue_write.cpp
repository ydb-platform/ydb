#include "service_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/keyvalue/keyvalue_events.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/random/fast.h>
#include <util/generic/queue.h>
#include <util/random/shuffle.h>

namespace NKikimr {
class TKeyValueWriterLoadTestActor;

#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }

class TWorker {
    friend class TKeyValueWriterLoadTestActor;

    TString KeyPrefix;
    TControlWrapper MaxInFlight;
    ui32 Size;
    bool IsInline = false;
    ui64 LoopAtKeyCount = 0;

    ui64 OperationIdx = 1;
    ui32 Idx;
    ui32 ItemsInFlight = 0;
    ui64 BytesInFlight = 0;
    TString DataBuffer;
    TReallyFastRng32 *Gen;
    bool IsDying = false;
public:

    TWorker(const NKikimr::TEvLoadTestRequest::TKeyValueLoad::TWorkerConfig& cmd,
            ui32 idx, TReallyFastRng32 *gen)
        : MaxInFlight(1, 0, 65536)
        , Idx(idx)
        , Gen(gen)
    {
        Y_UNUSED(Gen);

        VERIFY_PARAM(KeyPrefix);
        KeyPrefix = cmd.GetKeyPrefix();
        VERIFY_PARAM(MaxInFlight);
        MaxInFlight = cmd.GetMaxInFlight();
        VERIFY_PARAM(Size);
        Size = cmd.GetSize();
        DataBuffer = TString::TUninitialized(Size);
        ::memset(DataBuffer.Detach(), 0, Size);
        VERIFY_PARAM(IsInline);
        IsInline = cmd.GetIsInline();
        VERIFY_PARAM(LoopAtKeyCount);
        LoopAtKeyCount = cmd.GetLoopAtKeyCount();
    }

    std::unique_ptr<TEvKeyValue::TEvRequest> TrySend() {
        if (IsDying) {
            return {};
        }

        std::unique_ptr<TEvKeyValue::TEvRequest> ev;

        if (ItemsInFlight < MaxInFlight) {
            ev = std::make_unique<TEvKeyValue::TEvRequest>();
            auto write = ev->Record.AddCmdWrite();
            write->SetKey(Sprintf("%s%08" PRIu64,
                        KeyPrefix.c_str(),
                        LoopAtKeyCount ? (OperationIdx % LoopAtKeyCount) : OperationIdx));
            write->SetValue(DataBuffer);
            write->SetStorageChannel(
                    IsInline ?  NKikimrClient::TKeyValueRequest::INLINE : NKikimrClient::TKeyValueRequest::MAIN);
            write->SetPriority(NKikimrClient::TKeyValueRequest::REALTIME);
            BytesInFlight += DataBuffer.Size();
            ++ItemsInFlight;
            ++OperationIdx;
        }
        return ev;
    }

    void OnResult(ui32 size) {
        --ItemsInFlight;
        BytesInFlight -= size;
    }

    ~TWorker() {
    }
};

class TKeyValueWriterLoadTestActor : public TActorBootstrapped<TKeyValueWriterLoadTestActor> {
    struct TRequestInfo {
        ui32 Size;
        TInstant LogStartTime;
    };

    struct TRequestStat {
        ui64 BytesWrittenTotal;
        ui32 Size;
        TDuration Latency;
    };

    struct TLogWriteCookie {
        ui32 WorkerIdx;
        TInstant SentTime;
        ui64 Size;
    };

    TVector<std::unique_ptr<TWorker>> Workers;

    ui64 WrittenBytes = 0;

    const TActorId Parent;
    ui64 Tag;
    ui64 ReqIdx = 0;
    ui32 DurationSeconds;
    i32 OwnerInitInProgress = 0;

    TReallyFastRng32 Rng;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    TInstant TestStartTime;
    bool EarlyStop = false;

    ui64 TabletId;
    TActorId Pipe;

    ::NMonitoring::TDynamicCounters::TCounterPtr KeyValueBytesWritten;
    TMap<ui64, TLogWriteCookie> InFlightWrites;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PDISK_LOG_WRITE;
    }

    TKeyValueWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TKeyValueLoad& cmd,
            const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , Rng(Now().GetValue())
    {
        Y_UNUSED(index);
        VERIFY_PARAM(TargetTabletId);
        TabletId = cmd.GetTargetTabletId();

        ui32 idx = 0;
        for (const auto& workerCmd : cmd.GetWorkers()) {
            Workers.push_back(std::make_unique<TWorker>(workerCmd, idx, &Rng));
            ++idx;
        }

        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());
        // Report->Duration = TDuration::Seconds(DurationSeconds);

        // Monitoring initialization
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                                 GetSubgroup("tablet", Sprintf("%09" PRIu64, TabletId));
        KeyValueBytesWritten = LoadCounters->GetCounter("KeyValueBytesWritten", true);
        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorLogWriteDuration", "Time in microseconds", percentiles);
    }

    ~TKeyValueWriterLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Connect(const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " TKeyValueWriterLoadTestActor Connect called");
        Pipe = Register(NTabletPipe::CreateClient(SelfId(), TabletId));
        for (auto& worker : Workers) {
            worker->ItemsInFlight = 0;
        }
        SendWriteRequests(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TKeyValueWriterLoadTestActor Bootstrap called");
        Become(&TKeyValueWriterLoadTestActor::StateFunc);
        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");
        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);

        TestStartTime = TAppData::TimeProvider->Now();
        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " Bootstrap, Workers.size# " << Workers.size());
        for (auto& worker : Workers) {
            AppData(ctx)->Icb->RegisterLocalControl(worker->MaxInFlight,
                    Sprintf("KeyValueWriteLoadActor_MaxInFlight_%04" PRIu64 "_%04" PRIu32, Tag, worker->Idx));
            LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " last TEvKeyValueResult, "
                    << " all workers is initialized, start test");
        }
        EarlyStop = false;
        Connect(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        EarlyStop = (TAppData::TimeProvider->Now() - TestStartTime).Seconds() < DurationSeconds;
        if (OwnerInitInProgress) {
            LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill, "
                    << "not all workers is initialized, so wait them to end initialization");
        } else {
            LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " HandlePoisonPill, "
                    << "all workers is initialized, so starting death process");
            StartDeathProcess(ctx);
        }
    }

    void StartDeathProcess(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TKeyValueWriterLoadTestActor StartDeathProcess called");
        Become(&TKeyValueWriterLoadTestActor::StateEndOfWork);
        TIntrusivePtr<TEvLoad::TLoadReport> report = nullptr;
        if (!EarlyStop) {
            report.Reset(new TEvLoad::TLoadReport());
            report->Duration = TDuration::Seconds(DurationSeconds);
        }
        const TString errorReason = EarlyStop ?
            "Abort, stop signal received" : "OK, called StartDeathProcess";
        ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, report, errorReason));
        NTabletPipe::CloseClient(SelfId(), Pipe);
        Die(ctx);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // State Dying
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Monitoring
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvUpdateMonitoring::TPtr& /*ev*/, const TActorContext& ctx) {
        ResponseTimes.Update();
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Log writing
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendWriteRequests(const TActorContext& ctx) {
        ui64 sent = 0;
        for (auto& worker : Workers) {
            auto now = TAppData::TimeProvider->Now();
            while (std::unique_ptr<TEvKeyValue::TEvRequest> ev = worker->TrySend()) {
                ui64 size = ev->Record.GetCmdWrite(0).GetValue().size();
                *KeyValueBytesWritten += size;
                ev->Record.SetCookie(ReqIdx);
                InFlightWrites.insert({ReqIdx, {worker->Idx, now, size}});
                ++ReqIdx;
                NTabletPipe::SendData(ctx, Pipe, ev.release());
                ++sent;
            }
        }
        LOG_TRACE_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " SendWriteRequests sent# " << sent);
    }

    void Handle(TEvKeyValue::TEvResponse::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        auto record = msg->Record;
        if (record.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            TStringStream str;
            str << " TEvKeyValue::TEvResponse is not OK, msg.ToString()# " << msg->ToString();
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, str.Str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            NTabletPipe::CloseClient(SelfId(), Pipe);
            Die(ctx);
            return;
        }

        auto now = TAppData::TimeProvider->Now();
        auto it = InFlightWrites.find(record.GetCookie());
        Y_ABORT_UNLESS(it != InFlightWrites.end());
        const auto& stats = it->second;
        ResponseTimes.Increment((now - stats.SentTime).MicroSeconds());
        auto& worker = Workers[stats.WorkerIdx];

        worker->OnResult(stats.Size);
        WrittenBytes = WrittenBytes + stats.Size;
        LOG_TRACE_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " EvResult, "
                << " WrittenBytes# " << WrittenBytes);
        InFlightWrites.erase(it);

        SendWriteRequests(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
        HTML(str) {
            TABLE() {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Parameter"; }
                        TABLEH() { str << "Value"; }
                    }
                }
                TABLEBODY() {

                    PARAM("Elapsed time / Duration", (TAppData::TimeProvider->Now() - TestStartTime).Seconds() << "s / "
                            << DurationSeconds << "s");
                    for (auto& worker : Workers) {
                        PARAM("Worker idx", worker->Idx);
                        PARAM("Worker next OperationIdx", worker->OperationIdx);
                    }
                }
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected *msg = ev->Get();

        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TKeyValueWriterLoadTestActor Handle TEvClientConnected called, Status# " << msg->Status);

        if (msg->Status != NKikimrProto::OK) {
            if (msg->ClientId == Pipe) {
                Pipe = TActorId();
                // TODO(cthulhu): Reconnect
                Connect(ctx);
            }
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev, const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TKeyValueWriterLoadTestActor Handle TEvClientDestroyed called");
        TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        if (msg->ClientId == Pipe) {
            Pipe = TActorId();
            // TODO(cthulhu): Reconnect
            Connect(ctx);
        }
    }


    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(TEvUpdateMonitoring, Handle)

        HFunc(TEvKeyValue::TEvResponse, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(TEvTabletPipe::TEvClientConnected, Handle)
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
    )

    STRICT_STFUNC(StateEndOfWork,
        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(TEvKeyValue::TEvResponse, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
        HFunc(TEvTabletPipe::TEvClientConnected, Handle)
        HFunc(TEvTabletPipe::TEvClientDestroyed, Handle)
    )
};

IActor * CreateKeyValueWriterLoadTest(const NKikimr::TEvLoadTestRequest::TKeyValueLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TKeyValueWriterLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
