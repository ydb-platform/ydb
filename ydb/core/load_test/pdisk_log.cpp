#include <util/random/shuffle.h>
#include "service_actor.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/random/fast.h>
#include <util/generic/queue.h>


namespace NKikimr {
class TPDiskLogWriterLoadTestActor;

#define VAR_OUT(x) #x "# " << x << "; "

#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }

class TWorker {
    friend class TPDiskLogWriterLoadTestActor;

    TVDiskID VDiskId;
    ui32 Idx;
    NPDisk::TOwnerRound OwnerRound = 0;

    ui64 Lsn = 1;

    TControlWrapper MaxInFlight;
    ui32 LogInFlight = 0;

    ui64 BurstWrittenBytes = 0;
    ui64 BytesInFlight = 0;
    bool IsDying = false;
    bool IsHarakiriSent = false;
    bool IsStartingPointWritten = false;

    ui32 SizeMin;
    ui32 SizeMax;

    //    | <-- BurstSize --> |
    //    | <-- BurstInterval ------------------> |
    //    ********************_____________________********************_____________________
    // '*' Data writing
    // '_' Idle
    ui64 BurstInterval;
    ui64 BurstSize;
    ui64 BurstIdx;

    ui64 StorageDuration;
    ui64 CutLogLsn = Lsn;
    ui64 CutLogBytesWritten = 0;
    TMaybe<ui64> NextCutLogLsn = Lsn;
    ui64 NextCutLogBytesWritten = 0;

    ui64 StartingPoint = 0;
    ui64 NextStartingPoint = Lsn;

    TRcBuf DataBuffer;
    TReallyFastRng32 *Gen;

    TIntrusivePtr<TPDiskParams> PDiskParams;

    ::NMonitoring::TDynamicCounters::TCounterPtr LogEntriesWritten;

    NPDisk::TLogPosition LogReadPosition{0, 0};

    ui32 GenSize() const {
        return Gen->Uniform(SizeMin, SizeMax + 1);
    }

public:

    TWorker(const NKikimr::TEvLoadTestRequest::TPDiskLogLoad::TWorkerConfig& cmd,
            ui32 idx, TReallyFastRng32 *gen)
        : Idx(idx)
        , MaxInFlight(1, 0, 65536)
        , Gen(gen)
    {

        VERIFY_PARAM(MaxInFlight);
        MaxInFlight = cmd.GetMaxInFlight();

        VERIFY_PARAM(SizeIntervalMin);
        SizeMin = cmd.GetSizeIntervalMin();
        VERIFY_PARAM(SizeIntervalMax);
        SizeMax = cmd.GetSizeIntervalMax();
        DataBuffer = TRcBuf::Uninitialized(SizeMax);
        ::memset(DataBuffer.UnsafeGetDataMut(), 0, SizeMax);

        VERIFY_PARAM(VDiskId);
        VDiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

        // Burst control
        VERIFY_PARAM(BurstInterval);
        BurstInterval = cmd.GetBurstInterval();
        VERIFY_PARAM(BurstSize);
        BurstSize = cmd.GetBurstSize();

        BurstIdx = 0;

        VERIFY_PARAM(StorageDuration);
        StorageDuration = cmd.GetStorageDuration();
    }

    std::unique_ptr<NPDisk::TEvYardInit> GetYardInit(ui64 pDiskGuid) const {
        return std::make_unique<NPDisk::TEvYardInit>(OwnerRound, VDiskId, pDiskGuid);
    }


    std::unique_ptr<NPDisk::TEvLog> TrySend(const ui64 globalWrittenBytes) {
        if (IsDying || IsHarakiriSent) {
            return {};
        }

        if (BurstInterval * (BurstIdx + 1) <= globalWrittenBytes && BurstWrittenBytes >= BurstSize) {
            BurstWrittenBytes = 0;
            ++BurstIdx;
        }

        std::unique_ptr<NPDisk::TEvLog> ev;

        if (BurstWrittenBytes + BytesInFlight < BurstSize
                && BurstInterval * BurstIdx <= globalWrittenBytes
                && globalWrittenBytes < BurstInterval * (BurstIdx + 1)
                && LogInFlight < MaxInFlight) {
            TLsnSeg seg(Lsn, Lsn);
            ++Lsn;
            if (NextCutLogLsn) {
                NPDisk::TCommitRecord record;
                record.FirstLsnToKeep = CutLogLsn;
                record.IsStartingPoint = true;
                IsStartingPointWritten = true;
                NextStartingPoint = Lsn - 1;

                CutLogLsn = *NextCutLogLsn;
                CutLogBytesWritten = NextCutLogBytesWritten;
                ev = std::make_unique<NPDisk::TEvLog>(PDiskParams->Owner, OwnerRound, TLogSignature(),
                        record, DataBuffer, seg, nullptr);
            } else {
                ev = std::make_unique<NPDisk::TEvLog>(PDiskParams->Owner, OwnerRound, TLogSignature(),
                        DataBuffer, seg, nullptr);
            }
            BytesInFlight += DataBuffer.GetSize();
            ++LogInFlight;
        }
        return ev;
    }

    void OnLogResult(const NPDisk::TEvLogResult::TRecord& rec, ui32 size) {
        --LogInFlight;
        BytesInFlight -= size;
        BurstWrittenBytes += size;

        if (rec.Lsn == NextStartingPoint) {
            StartingPoint = NextStartingPoint;
            NextCutLogLsn.Clear();
        }

        if (!NextCutLogLsn && GetReallyWrittenBytes() - CutLogBytesWritten >= StorageDuration) {
            NextCutLogLsn = Lsn - 1;
            NextCutLogBytesWritten = GetReallyWrittenBytes();
        }
    }

    void PoisonPill() {
        MaxInFlight = 0;
    }

    bool CheckDie() {
        if (!MaxInFlight && !LogInFlight && !IsDying) {
            if (PDiskParams) {
                IsDying = true;
            }
            return true;
        }
        return false;
    }

    std::unique_ptr<NPDisk::TEvHarakiri> GetHarakiri() {
        Y_ABORT_UNLESS(IsDying);
        Y_ABORT_UNLESS(LogReadPosition == NPDisk::TLogPosition::Invalid());
        return std::make_unique<NPDisk::TEvHarakiri>(PDiskParams->Owner, OwnerRound);
    }

    std::unique_ptr<NPDisk::TEvReadLog> GetLogRead() {
        if (LogReadPosition == NPDisk::TLogPosition::Invalid()) {
            return {};
        } else {
            return std::make_unique<NPDisk::TEvReadLog>(PDiskParams->Owner, OwnerRound, LogReadPosition);
        }
    }

    void CheckStartingPoints(const TMap<TLogSignature, NPDisk::TLogRecord>& startingPoints) {
        if (!IsStartingPointWritten) {
            return;
        }

        auto it = startingPoints.find(TLogSignature());
        Y_VERIFY_S(it != startingPoints.end(),
                VAR_OUT((ui32)PDiskParams->Owner) <<
                VAR_OUT(StartingPoint) <<
                VAR_OUT(NextStartingPoint));
        const ui64 realStartingPoint = it->second.Lsn;
        Y_ABORT_UNLESS(realStartingPoint == StartingPoint || realStartingPoint == NextStartingPoint);
        // Set StartingPoint to real point to start check from it
        StartingPoint = realStartingPoint;
    }

    bool FindLastWrittenLsn(const NPDisk::TEvReadLogResult* msg) {
        for (const auto& res : msg->Results) {
            Lsn = Max(Lsn, res.Lsn + 1);
        }

        if (msg->IsEndOfLog) {
            LogReadPosition = NPDisk::TLogPosition{0, 0};
        } else {
            LogReadPosition = msg->NextPosition;
        }
        return msg->IsEndOfLog;
    }

    void CheckLogRecords(const NPDisk::TEvReadLogResult* msg) {
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        for (const auto& res : msg->Results) {
            if (res.Lsn < StartingPoint) {
                continue;
            }
            Y_VERIFY_S(StartingPoint == res.Lsn,
                    VAR_OUT(StartingPoint) <<
                    VAR_OUT(res.Lsn));
            ++StartingPoint;
        }

        if (msg->IsEndOfLog) {
            Y_VERIFY_S(!IsStartingPointWritten || StartingPoint == Lsn,
                    VAR_OUT(StartingPoint) <<
                    VAR_OUT(Lsn));
            LogReadPosition = NPDisk::TLogPosition::Invalid();
        } else {
            LogReadPosition = msg->NextPosition;
        }
    }

    ui64 GetReallyWrittenBytes() const {
        return BurstSize * BurstIdx + BurstWrittenBytes;
    }

    ui64 GetGlobalWrittenBytes() const {
        return BurstInterval * BurstIdx + BurstWrittenBytes;
    }

    ~TWorker() {
    }
};

class TPDiskLogWriterLoadTestActor : public TActorBootstrapped<TPDiskLogWriterLoadTestActor> {
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
    ui32 DurationSeconds;
    i32 OwnerInitInProgress = 0;
    ui32 HarakiriInFlight = 0;

    TReallyFastRng32 Rng;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    TInstant TestStartTime;

    ui32 PDiskId;
    ui64 PDiskGuid;

    bool IsWardenlessTest = false;
    bool IsDying = false;

    ::NMonitoring::TDynamicCounters::TCounterPtr LogBytesWritten;
    ui64 ReqIdx = 0;
    TMap<ui64, TLogWriteCookie> InFlightLogWrites;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> LogResponseTimes;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PDISK_LOG_WRITE;
    }

    TPDiskLogWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TPDiskLogLoad& cmd,
            const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , Rng(Now().GetValue())
        // , Report(new TEvLoad::TLoadReport())
    {
        VERIFY_PARAM(PDiskId);
        PDiskId = cmd.GetPDiskId();

        VERIFY_PARAM(PDiskGuid);
        PDiskGuid = cmd.GetPDiskGuid();

        VERIFY_PARAM(IsWardenlessTest);
        IsWardenlessTest = cmd.GetIsWardenlessTest();


        ui32 idx = 0;
        for (const auto& workerCmd : cmd.GetWorkers()) {
            Workers.push_back(std::make_unique<TWorker>(workerCmd, idx, &Rng));
            if (IsWardenlessTest) {
                Workers.back()->OwnerRound = 1000 + index + idx;
            }
            ++idx;
        }

        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());
        // Report->Duration = TDuration::Seconds(DurationSeconds);

        // Monitoring initialization
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                                 GetSubgroup("pdisk", Sprintf("%09" PRIu32, PDiskId));
        LogBytesWritten = LoadCounters->GetCounter("LogBytesWritten", true);
        LogResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorLogWriteDuration", "Time in microseconds", percentiles);
    }

    ~TPDiskLogWriterLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPDiskLogWriterLoadTestActor::StateFunc);
        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " Schedule PoisonPill");
        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);

        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " Bootstrap, Workers.size# " << Workers.size());
        if (IsWardenlessTest) {
            for (auto& worker : Workers) {
                AppData(ctx)->Icb->RegisterLocalControl(worker->MaxInFlight,
                        Sprintf("PDiskWriteLoadActor_MaxInFlight_%04" PRIu64 "_%04" PRIu32, Tag, worker->Idx));
                SendRequest(ctx, worker->GetYardInit(PDiskGuid));
            }
        } else {
            LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " Send TEvRegisterPDiskLoadActor");
            Send(MakeBlobStorageNodeWardenID(ctx.SelfID.NodeId()), new TEvRegisterPDiskLoadActor());
        }
        OwnerInitInProgress = Workers.size();
    }

    void Handle(TEvRegisterPDiskLoadActorResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();

        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TEvRegisterPDiskLoadActorResult recieved, ownerRound# " << (ui32)msg->OwnerRound);
        for (auto& worker : Workers) {
            worker->OwnerRound = msg->OwnerRound + 1;
            SendRequest(ctx, worker->GetYardInit(PDiskGuid));
        }
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << "TEvYardInitResult is not OK, msg.ToString()# " << msg->ToString();
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, str.Str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " TEvYardInitResult, "
                << " Owner# " << (ui32)msg->PDiskParams->Owner
                << " OwnerRound# " << msg->PDiskParams->OwnerRound);

        for (auto& worker : Workers) {
            if (!worker->PDiskParams) {
                worker->PDiskParams = std::move(msg->PDiskParams);
                worker->OwnerRound = Max(worker->OwnerRound, worker->PDiskParams->OwnerRound);
                auto logRead = worker->GetLogRead();
                Y_ABORT_UNLESS(logRead);
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " owner# "
                        << (ui32)worker->PDiskParams->Owner << " going to send first TEvLogRead# " << logRead->ToString());
                SendRequest(ctx, std::move(logRead));
                break;
            }
        }
    }

    void Handle(NPDisk::TEvReadLogResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        for (auto& worker : Workers) {
            if (worker->PDiskParams && worker->PDiskParams->Owner == msg->Owner) {
                if (worker->FindLastWrittenLsn(msg)) {
                    // Lsn is found
                    LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " owner# " << (ui32)msg->Owner
                            << " found first Lsn to write# " << worker->Lsn);

                    --OwnerInitInProgress;
                } else {
                    auto logRead = worker->GetLogRead();
                    Y_ABORT_UNLESS(logRead);
                    SendRequest(ctx, std::move(logRead));
                }
                break;
            }
        }

        // All workers is initialized
        if (!OwnerInitInProgress) {
            TestStartTime = TAppData::TimeProvider->Now();
            if (IsDying) {
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " last TEvReadLogResult, "
                        << " all workers is initialized, but IsDying# true, so starting death process");
                StartDeathProcess(ctx);
            } else {
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " last TEvReadLogResult, "
                        << " all workers is initialized, start test");
                SendWriteRequests(ctx);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        IsDying = true;
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
        Become(&TPDiskLogWriterLoadTestActor::StateEndOfWork);
        if (IsWardenlessTest) {
            for (auto& worker : Workers) {
                ++worker->OwnerRound;
                worker->PoisonPill();
            }
            CheckDie(ctx);
        } else {
            Send(MakeBlobStorageNodeWardenID(ctx.SelfID.NodeId()), new TEvRegisterPDiskLoadActor());
        }
    }

    void HandleEnd(TEvRegisterPDiskLoadActorResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();

        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag
                << " TEvRegisterPDiskLoadActorResult recieved, ownerRound# " << msg->OwnerRound);
        for (auto& worker : Workers) {
            worker->OwnerRound = msg->OwnerRound + 1;
            worker->PoisonPill();
        }
        CheckDie(ctx);
    }


    void CheckDie(const TActorContext& ctx) {
        for (auto& worker : Workers) {
            if (worker->CheckDie()) {
                SendRequest(ctx, worker->GetYardInit(PDiskGuid));
            }
        }
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // State Dying
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleEnd(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << "TEvYardInitResult is not OK, msg.ToString()# " << msg->ToString();
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, str.Str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }

        LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " end of work, TEvYardInitResult, "
                << " Owner# " << (ui32)msg->PDiskParams->Owner
                << " OwnerRound# " << msg->PDiskParams->OwnerRound);

        for (auto& worker : Workers) {
            if (worker->PDiskParams->Owner == msg->PDiskParams->Owner) {
                worker->PDiskParams = std::move(msg->PDiskParams);
                worker->OwnerRound = worker->PDiskParams->OwnerRound;
                worker->CheckStartingPoints(msg->StartingPoints);
                SendRequest(ctx, worker->GetLogRead());
                break;
            }
        }
    }

    void HandleEnd(NPDisk::TEvReadLogResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);

        for (auto& worker : Workers) {
            if (worker->PDiskParams->Owner == msg->Owner) {
                worker->CheckLogRecords(msg);
                if (auto logRead = worker->GetLogRead()) {
                    SendRequest(ctx, std::move(logRead));
                } else if (auto harakiri = worker->GetHarakiri()) {
                    ++HarakiriInFlight;
                    SendRequest(ctx, std::move(harakiri));
                }
                break;
            }
        }
    }

    void HandleEnd(NPDisk::TEvHarakiriResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << "TEvHarakiriResult is not OK, msg.ToString()# " << msg->ToString();
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, str.Str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }

        Y_ABORT_UNLESS(HarakiriInFlight);

        if (!--HarakiriInFlight) {
            for (auto& worker : Workers) {
                LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " End of work,"
                        << " owner# " << (ui32)worker->PDiskParams->Owner
                        << " GetReallyWrittenBytes()# " << worker->GetReallyWrittenBytes()
                        << " GetGlobalWrittenBytes()# " << worker->GetGlobalWrittenBytes());
            }
            TIntrusivePtr<TEvLoad::TLoadReport> report = new TEvLoad::TLoadReport();
            report->LoadType = TEvLoad::TLoadReport::LOAD_LOG_WRITE;
            report->Duration = TAppData::TimeProvider->Now() - TestStartTime;
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, report, "OK"));
            LOG_INFO_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " End of work, TEvLoadTestFinished is sent");
            Die(ctx);
        }

        CheckDie(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Monitoring
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvUpdateMonitoring::TPtr& /*ev*/, const TActorContext& ctx) {
        LogResponseTimes.Update();
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Log writing
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendWriteRequests(const TActorContext& ctx) {
        LOG_TRACE_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " SendWriteRequests");
        for (auto& worker : Workers) {
            auto now = TAppData::TimeProvider->Now();
            while (std::unique_ptr<NPDisk::TEvLog> ev = worker->TrySend(WrittenBytes)) {
                *LogBytesWritten += ev->Data.size();
                ev->Cookie = reinterpret_cast<void*>(ReqIdx);
                InFlightLogWrites.insert({ReqIdx, {worker->Idx, now, ev->Data.size()}});
                ++ReqIdx;
                SendRequest(ctx, std::move(ev));
            }
        }

        CheckDie(ctx);
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << " TEvLogResult is not OK, msg.ToString()# " << msg->ToString();
            LOG_ERROR_S(ctx, NKikimrServices::BS_LOAD_TEST, str.Str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }

        auto now = TAppData::TimeProvider->Now();
        for (const auto& res : msg->Results) {
            auto it = InFlightLogWrites.find(reinterpret_cast<ui64>(res.Cookie));
            Y_ABORT_UNLESS(it != InFlightLogWrites.end());
            const auto& stats = it->second;
            LogResponseTimes.Increment((now - stats.SentTime).MicroSeconds());
            auto& worker = Workers[stats.WorkerIdx];

            worker->OnLogResult(res, stats.Size);
            WrittenBytes = Max(WrittenBytes, worker->GetGlobalWrittenBytes());
            LOG_TRACE_S(ctx, NKikimrServices::BS_LOAD_TEST, "Tag# " << Tag << " TEvLogResult, "
                    << " WrittenBytes# " << WrittenBytes);
            InFlightLogWrites.erase(it);
        }

        SendWriteRequests(ctx);
    }

    template<typename TRequest>
    void SendRequest(const TActorContext& ctx, std::unique_ptr<TRequest>&& request) {
        ctx.Send(MakeBlobStoragePDiskID(ctx.ExecutorThread.ActorSystem->NodeId, PDiskId), request.release());
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
                        PARAM("Worker ReallyWrittenBytes", worker->GetReallyWrittenBytes());
                        PARAM("Worker GlobalWrittenBytes", worker->GetGlobalWrittenBytes());
                        PARAM("Worker BurstSize", worker->BurstSize);
                        PARAM("Worker BurstInterval", worker->BurstInterval);
                        PARAM("Worker BurstIdx", worker->BurstIdx);
                        PARAM("Worker BurstWrittenBytes", worker->BurstWrittenBytes);
                        PARAM("Worker next Lsn", worker->Lsn);
                        PARAM("Worker CutLogLsn", worker->CutLogLsn);
                        PARAM("Worker StartingPoint", worker->StartingPoint);

                    }
                }
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(TEvRegisterPDiskLoadActorResult, Handle)
        HFunc(NPDisk::TEvYardInitResult, Handle)
        HFunc(NPDisk::TEvReadLogResult, Handle)
        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(NPDisk::TEvLogResult, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )

    STRICT_STFUNC(StateEndOfWork,
        HFunc(NPDisk::TEvYardInitResult, HandleEnd)
        HFunc(NPDisk::TEvHarakiriResult, HandleEnd)
        HFunc(NPDisk::TEvReadLogResult, HandleEnd)
        HFunc(TEvRegisterPDiskLoadActorResult, HandleEnd)

        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(NPDisk::TEvLogResult, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

IActor *CreatePDiskLogWriterLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskLogLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TPDiskLogWriterLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
