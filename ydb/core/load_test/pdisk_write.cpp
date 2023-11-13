#include <util/random/shuffle.h>
#include "service_actor.h"
#include <ydb/core/base/counters.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/random/fast.h>
#include <util/generic/queue.h>
#include <ydb/core/control/immediate_control_board_impl.h>

namespace NKikimr {

class TPDiskWriterLoadTestActor : public TActorBootstrapped<TPDiskWriterLoadTestActor> {
    struct TChunkInfo {
        TDeque<std::pair<TChunkIdx, ui32>> WriteQueue;
        ui32 NumSlots;
        ui32 SlotSizeBlocks;
        ui32 Weight;
        ui64 AccumWeight;

        struct TFindByWeight {
            bool operator ()(ui64 left, const TChunkInfo& right) const {
                return left < right.AccumWeight;
            }
        };
    };

    struct TParts : public NPDisk::TEvChunkWrite::IParts {
        const void *Buffer;
        ui32 Len;

        TParts(const void *buffer, ui32 len)
            : Buffer(buffer)
            , Len(len)
        {}

        TDataRef operator [](ui32 index) const override {
            Y_ABORT_UNLESS(index == 0);
            return std::make_pair(Buffer, Len);
        }

        ui32 Size() const override {
            return 1;
        }
    };

    struct TRequestInfo {
        ui32 Size;
        TChunkIdx ChunkIdx;
        TInstant StartTime;
        TInstant LogStartTime;
        bool DataWritten;
        bool LogWritten;

        TRequestInfo(ui32 size, TChunkIdx chunkIdx, TInstant startTime, TInstant logStartTime, bool dataWritten,
                bool logWritten)
            : Size(size)
            , ChunkIdx(chunkIdx)
            , StartTime(startTime)
            , LogStartTime(logStartTime)
            , DataWritten(dataWritten)
            , LogWritten(logWritten)
        {}

        TRequestInfo(const TRequestInfo &) = default;
        TRequestInfo() = default;
        TRequestInfo &operator=(const TRequestInfo &other) = default;

    };

    struct TRequestStat {
        ui64 BytesWrittenTotal;
        ui32 Size;
        TDuration Latency;
    };

    THashMap<ui64, TRequestInfo> RequestInfo;
    ui64 NextRequestIdx = 0;

    const TActorId Parent;
    ui64 Tag;
    ui32 DurationSeconds;
    ui32 IntervalMsMin = 0;
    ui32 IntervalMsMax = 0;
    TControlWrapper MaxInFlight;
    ui32 InFlight = 0;
    ui32 LogInFlight = 0;
    TInstant LastRequest;
    ui32 IntervalMs = 0;
    ui32 PDiskId;
    TVDiskID VDiskId;
    NPDisk::TOwnerRound OwnerRound;
    ui64 PDiskGuid;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    TVector<TChunkInfo> Chunks;
    TReallyFastRng32 Rng;
    TBuffer DataBuffer;
    ui64 Lsn = 1;
    TChunkInfo *ReservePending = nullptr;
    NKikimr::TEvLoadTestRequest::ELogMode LogMode;
    THashMap<TChunkIdx, ui32> ChunkUsageCount;
    TQueue<TChunkIdx> AllocationQueue;
    TMultiMap<TInstant, TRequestStat> TimeSeries;
    TVector<TChunkIdx> DeleteChunks;
    bool Sequential;
    bool Reuse;
    bool IsWardenlessTest;
    bool Harakiri = false;

    TInstant TestStartTime;
    TInstant MeasurementStartTime;

    // statistics
    ui64 ChunkWrite_RequestsSent = 0;
    ui64 ChunkWrite_OK = 0;
    ui64 ChunkWrite_NonOK = 0;
    ui64 ChunkReserve_RequestsSent = 0;
    ui64 DeletedChunksCount = 0;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
    ::NMonitoring::TDynamicCounters::TCounterPtr LogEntriesWritten;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> LogResponseTimes;

    TIntrusivePtr<TEvLoad::TLoadReport> Report;
    TIntrusivePtr<NMonitoring::TCounterForPtr> PDiskBytesWritten;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PDISK_WRITE;
    }

    TPDiskWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TPDiskWriteLoad& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , MaxInFlight(4, 0, 65536)
        , OwnerRound(1000 + index)
        , Rng(Now().GetValue())
        , Report(new TEvLoad::TLoadReport())
    {

        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());
        Report->Duration = TDuration::Seconds(DurationSeconds);

        IntervalMsMin = cmd.GetIntervalMsMin();
        IntervalMsMax = cmd.GetIntervalMsMax();

        VERIFY_PARAM(InFlightWrites);
        MaxInFlight = cmd.GetInFlightWrites();
        Report->InFlight = MaxInFlight;

        VERIFY_PARAM(PDiskId);
        PDiskId = cmd.GetPDiskId();

        VERIFY_PARAM(PDiskGuid);
        PDiskGuid = cmd.GetPDiskGuid();

        VERIFY_PARAM(VDiskId);
        VDiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

        VERIFY_PARAM(LogMode);
        LogMode = cmd.GetLogMode();

        Sequential = cmd.GetSequential();
        Reuse = cmd.GetReuse();
        IsWardenlessTest = cmd.GetIsWardenlessTest();

        for (const auto& chunk : cmd.GetChunks()) {
            if (!chunk.HasSlots() || !chunk.HasWeight() || !chunk.GetSlots() || !chunk.GetWeight()) {
                ythrow TLoadActorException() << "chunk.Slots/Weight fields are either missing or zero";
            }
            Chunks.push_back(TChunkInfo{
                    {},
                    chunk.GetSlots(),
                    0,
                    chunk.GetWeight(),
                    0,
                });
        }

        // Monitoring initialization
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                                 GetSubgroup("pdisk", Sprintf("%09" PRIu32, PDiskId));
        BytesWritten = LoadCounters->GetCounter("LoadActorBytesWritten", true);
        LogEntriesWritten = LoadCounters->GetCounter("LoadActorLogEntriesWritten", true);
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorWriteDuration", "Time in microseconds", percentiles);
        LogResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorLogWriteDuration", "Time in microseconds", percentiles);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> pDiskCounters = GetServiceCounters(counters, "pdisks")->
             GetSubgroup("pdisk", Sprintf("%09" PRIu32, PDiskId));
        PDiskBytesWritten = pDiskCounters->GetSubgroup("subsystem", "device")->GetCounter("DeviceBytesWritten", true);

        if (Chunks.empty()) {
            ythrow TLoadActorException() << "Chunks may not be empty";
        }
    }

    ~TPDiskWriterLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPDiskWriterLoadTestActor::StateFunc);
        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        AppData(ctx)->Icb->RegisterLocalControl(MaxInFlight, Sprintf("PDiskWriteLoadActor_MaxInFlight_%4" PRIu64, Tag).c_str());
        if (IsWardenlessTest) {
            SendRequest(ctx, std::make_unique<NPDisk::TEvYardInit>(OwnerRound, VDiskId, PDiskGuid));
        } else {
            Send(MakeBlobStorageNodeWardenID(ctx.SelfID.NodeId()), new TEvRegisterPDiskLoadActor());
        }
    }

    void Handle(TEvRegisterPDiskLoadActorResult::TPtr& ev, const TActorContext& ctx) {
        OwnerRound = ev->Get()->OwnerRound;
        SendRequest(ctx, std::make_unique<NPDisk::TEvYardInit>(OwnerRound, VDiskId, PDiskGuid));
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << "yard init failed, Status# " << NKikimrProto::EReplyStatus_Name(msg->Status);
            LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "%s", str.Str().c_str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }
        PDiskParams = msg->PDiskParams;
        DataBuffer.Resize(PDiskParams->ChunkSize);
        char *data = DataBuffer.data();
        for (ui32 i = 0; i < PDiskParams->ChunkSize; ++i) {
            data[i] = Rng();
        }
        for (TChunkInfo& chunk : Chunks) {
            chunk.SlotSizeBlocks = PDiskParams->ChunkSize / PDiskParams->AppendBlockSize / chunk.NumSlots;
        }
        TestStartTime = TAppData::TimeProvider->Now();
        MeasurementStartTime = TestStartTime + DelayBeforeMeasurements;
        CheckForReserve(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk reservation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ui64 NewTRequestInfo(ui32 size, TChunkIdx chunkIdx, TInstant startTime, TInstant logStartTime, bool dataWritten,
                bool logWritten) {
        ui64 requestIdx = NextRequestIdx;
        RequestInfo[requestIdx] = TRequestInfo(size, chunkIdx, startTime, logStartTime, dataWritten, logWritten);
        NextRequestIdx++;
        return requestIdx;
    }

    void CheckForReserve(const TActorContext& ctx) {
        if (!ReservePending && MaxInFlight) {
            for (TChunkInfo& chunkInfo : Chunks) {
                if (chunkInfo.WriteQueue.size() <= chunkInfo.NumSlots * 2 / 3) {
                    if (AllocationQueue) {
                        TChunkIdx chunkIdx = AllocationQueue.front();
                        AllocationQueue.pop();
                        ApplyNewChunk(chunkIdx, chunkInfo);
                    } else {
                        SendRequest(ctx, std::make_unique<NPDisk::TEvChunkReserve>(PDiskParams->Owner,
                                    PDiskParams->OwnerRound, 1U));
                        ReservePending = &chunkInfo;
                        ++ChunkReserve_RequestsSent;
                    }
                    break;
                }
            }
        }
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        Y_ABORT_UNLESS(msg->Status == NKikimrProto::OK);
        TChunkInfo& chunkInfo = *ReservePending;
        ReservePending = nullptr;
        for (TChunkIdx chunkIdx : msg->ChunkIds) {
            ApplyNewChunk(chunkIdx, chunkInfo);
        }
        CheckForReserve(ctx);
        SendWriteRequests(ctx);
    }

    void ApplyNewChunk(TChunkIdx chunkIdx, TChunkInfo& chunkInfo) {
        std::vector<ui32> slots;
        for (ui32 i = 0; i < chunkInfo.NumSlots; ++i) {
            slots.push_back(i);
        }
        if (!Sequential) {
            Shuffle(slots.begin(), slots.end());
        }
        for (ui32 slot : slots) {
            chunkInfo.WriteQueue.emplace_back(chunkIdx, slot);
        }
        ChunkUsageCount[chunkIdx] = chunkInfo.NumSlots;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Rate management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleWakeup(const TActorContext& ctx) {
        SendWriteRequests(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        Report->LoadType = TEvLoad::TLoadReport::LOAD_WRITE;
        MaxInFlight = 0;
        CheckDie(ctx);
    }

    void CheckDie(const TActorContext& ctx) {
        if (!MaxInFlight && !InFlight && !LogInFlight && !Harakiri) {
            if (PDiskParams) {
                SendRequest(ctx, std::make_unique<NPDisk::TEvHarakiri>(PDiskParams->Owner, PDiskParams->OwnerRound));
                Harakiri = true;
            } else {
                ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, Report, "OK, but can't send TEvHarakiri to PDisk"));
                Die(ctx);
            }
        }
    }

    void Handle(NPDisk::TEvHarakiriResult::TPtr& /*ev*/, const TActorContext& ctx) {
        ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, Report, "OK"));
        Die(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Monitoring
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvUpdateMonitoring::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        ResponseTimes.Update();
        LogResponseTimes.Update();

        const TInstant now = TAppData::TimeProvider->Now();
        if (now > MeasurementStartTime) {
            auto begin = TimeSeries.lower_bound(now - TDuration::MilliSeconds(MonitoringUpdateCycleMs));
            if (begin != TimeSeries.end()) {
                auto end = std::prev(TimeSeries.lower_bound(now));
                if (end != begin) {
                    ui64 speedBps = (end->second.BytesWrittenTotal - begin->second.BytesWrittenTotal) /
                        TDuration::MilliSeconds(MonitoringUpdateCycleMs).SecondsFloat();
                    Report->RwSpeedBps.push_back(speedBps);
                } else {
                    Report->RwSpeedBps.push_back(0);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk writing
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendWriteRequests(const TActorContext& ctx) {
        while (InFlight < MaxInFlight) {
            // Randomize interval (if required)
            if (!IntervalMs && IntervalMsMax && IntervalMsMin) {
                IntervalMs = IntervalMsMin;
                if (ui32 delta = (IntervalMsMax > IntervalMsMin? IntervalMsMax - IntervalMsMin: 0)) {
                    IntervalMs += Rng() % delta;
                }
            }

            if (IntervalMs) {
                // Enforce intervals between requests
                TInstant now = TAppData::TimeProvider->Now();
                TInstant nextRequest = LastRequest + TDuration::MilliSeconds(IntervalMs);
                if (now < nextRequest) {
                    // Suspend sending until interval will elapse
                    ctx.Schedule(nextRequest - now, new TEvents::TEvWakeup);
                    break;
                }
                LastRequest = now;
                IntervalMs = 0; // To enforce regeneration of new random interval
            }

            // Prepare to send request
            ui64 accumWeight = 0;
            for (TChunkInfo& chunkInfo : Chunks) {
                chunkInfo.AccumWeight = accumWeight;
                if (!chunkInfo.WriteQueue.empty()) {
                    accumWeight += chunkInfo.Weight;
                }
            }
            if (!accumWeight) {
                break;
            }

            ui64 w = (ui64(Rng()) << 32 | Rng()) % accumWeight;
            auto it = std::prev(std::upper_bound(Chunks.begin(), Chunks.end(), w, TChunkInfo::TFindByWeight()));
            TChunkInfo& chunkInfo = *it;

            Y_ABORT_UNLESS(!chunkInfo.WriteQueue.empty());

            auto& front = chunkInfo.WriteQueue.front();
            TChunkIdx chunkIdx = front.first;
            ui32 slotIndex = front.second;
            chunkInfo.WriteQueue.pop_front();

            ui32 size = chunkInfo.SlotSizeBlocks * PDiskParams->AppendBlockSize;
            Report->Size = size;
            ui32 offset = slotIndex * size;
            const TInstant now = TAppData::TimeProvider->Now();
            // like the parallel mode, but log is treated already written
            bool isLogWritten = (LogMode == NKikimr::TEvLoadTestRequest::LOG_NONE);
            ui64 requestIdx = NewTRequestInfo(size, chunkIdx, now, now, false, isLogWritten);
            SendRequest(ctx, std::make_unique<NPDisk::TEvChunkWrite>(PDiskParams->Owner, PDiskParams->OwnerRound,
                    chunkIdx, offset,
                    new TParts{DataBuffer.data() + Rng() % (DataBuffer.size() - size), size},
                    reinterpret_cast<void*>(requestIdx), true, NPriWrite::HullHugeAsyncBlob, Sequential));
            ++ChunkWrite_RequestsSent;

            if (LogMode == NKikimr::TEvLoadTestRequest::LOG_PARALLEL) {
                SendLogRequest(ctx, requestIdx, chunkIdx);
            }

            ++InFlight;
        }

        CheckForReserve(ctx);
        CheckDie(ctx);
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            ++ChunkWrite_OK;
        } else {
            ++ChunkWrite_NonOK;
        }
        ui64 requestIdx = reinterpret_cast<ui64>(msg->Cookie);
        TRequestInfo *info = &RequestInfo[requestIdx];
        info->DataWritten = true;
        *BytesWritten += info->Size;

        if (info->LogWritten) {
            // both data and log are written, this could happen only in LOG_PARALLEL mode; this request is done
            FinishRequest(ctx, requestIdx);
        } else if (LogMode == NKikimr::TEvLoadTestRequest::LOG_SEQUENTIAL) {
            // in sequential mode we send log request after completion of data write request
            SendLogRequest(ctx, requestIdx, msg->ChunkIdx);
        } else if (LogMode == NKikimr::TEvLoadTestRequest::LOG_PARALLEL) {
            // this is parallel mode and log is not written yet, so request is not complete; we release it to avoid
            // being deleted
        }

        CheckDie(ctx);
    }

    void SendLogRequest(const TActorContext& ctx, ui64 requestIdx, TChunkIdx chunkIdx) {
        RequestInfo[requestIdx].LogStartTime = TAppData::TimeProvider->Now();
        TString logRecord = "Hello, my dear log! I've just written a chunk!";
        NPDisk::TCommitRecord record;
        record.CommitChunks.push_back(chunkIdx);
        record.DeleteChunks.swap(DeleteChunks);
        DeletedChunksCount += record.DeleteChunks.size();
        TLsnSeg seg(Lsn, Lsn);
        ++Lsn;
        SendRequest(ctx, std::make_unique<NPDisk::TEvLog>(PDiskParams->Owner, PDiskParams->OwnerRound,
                TLogSignature::SignatureHugeLogoBlob, record, TRcBuf(logRecord), seg,
                reinterpret_cast<void*>(requestIdx)));
        ++LogInFlight;
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        TInstant now = TAppData::TimeProvider->Now();
        for (const auto& res : msg->Results) {
            ui64 requestIdx = reinterpret_cast<ui64>(res.Cookie);
            TRequestInfo *info = &RequestInfo[requestIdx];
            info->LogWritten = true;
            *LogEntriesWritten += 1;
            LogResponseTimes.Increment((now - info->LogStartTime).MicroSeconds());
            if (info->DataWritten) {
                // both data and log are written, complete request and send another one if possible
                FinishRequest(ctx, requestIdx);
            } else {
                // log is written, but data is not; this is parallel mode and this request will be deleted in chunk write
                // completion handler
            }
            --LogInFlight;
        }

        CheckDie(ctx);
    }

    void FinishRequest(const TActorContext& ctx, ui64 requestIdx) {
        TInstant now = TAppData::TimeProvider->Now();
        TRequestInfo *request = &RequestInfo[requestIdx];

        if (now > MeasurementStartTime) {
            Report->LatencyUs.Increment((now - request->StartTime).MicroSeconds());
        }

        TimeSeries.emplace(now, TRequestStat{
                static_cast<ui64>(*BytesWritten), // current state of bytes written counter
                request->Size,
                now - request->StartTime
            });
        ResponseTimes.Increment((now - request->StartTime).MicroSeconds());
        // cut time series to 60 seconds
        auto pos = TimeSeries.upper_bound(now - TDuration::Seconds(60));
        TimeSeries.erase(TimeSeries.begin(), pos);
        auto it = ChunkUsageCount.find(request->ChunkIdx);
        Y_ABORT_UNLESS(it != ChunkUsageCount.end());
        if (!--it->second) {
            // chunk is completely written and can be destroyed
            if (Reuse) {
                AllocationQueue.push(it->first);
            } else {
                DeleteChunks.push_back(it->first);
            }
            ChunkUsageCount.erase(it);
        }
        --InFlight;
        RequestInfo.erase(requestIdx);
        SendWriteRequests(ctx);
    }

    template<typename TRequest>
    void SendRequest(const TActorContext& ctx, std::unique_ptr<TRequest>&& request) {
        ctx.Send(MakeBlobStoragePDiskID(ctx.ExecutorThread.ActorSystem->NodeId, PDiskId), request.release());
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx) {
        TStringStream str;
#define PARAM(NAME, VALUE) \
    TABLER() { \
        TABLED() { str << NAME; } \
        TABLED() { str << VALUE; } \
    }
        TMap<ui32, TVector<TDuration>> latmap;
        for (const auto& pair : TimeSeries) {
            const TRequestStat& stat = pair.second;
            latmap[stat.Size].push_back(stat.Latency);
        }
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
                    PARAM("TEvChunkWrite msgs sent", ChunkWrite_RequestsSent);
                    PARAM("TEvChunkWriteResult msgs received, OK", ChunkWrite_OK);
                    PARAM("TEvChunkWriteResult msgs received, not OK", ChunkWrite_NonOK);
                    PARAM("TEvChunkReserve msgs sent", ChunkReserve_RequestsSent);
                    PARAM("Bytes written", static_cast<ui64>(*BytesWritten));
                    PARAM("Number of deleted chunks", DeletedChunksCount);
                    if (PDiskParams) {
                        PARAM("Owner", PDiskParams->Owner);
                        PARAM("Chunk size", PDiskParams->ChunkSize);
                        PARAM("Append block size", PDiskParams->AppendBlockSize);
                    }

                    for (ui32 dt : {5, 10, 15, 20, 60}) {
                        TInstant now = TAppData::TimeProvider->Now();
                        auto it = TimeSeries.upper_bound(now - TDuration::Seconds(dt));
                        if (it != TimeSeries.begin()) {
                            --it;
                        }
                        if (it != TimeSeries.end()) {
                            auto end = std::prev(TimeSeries.end());
                            if (end != it) {
                                double seconds = (end->first - it->first).GetValue() * 1e-6;
                                double speed = (end->second.BytesWrittenTotal - it->second.BytesWrittenTotal) / seconds;
                                speed /= 1e6;
                                PARAM("Average write speed at last " << dt << " seconds, MB/s", Sprintf("%.3f", speed));
                            }
                        }
                    }

                    for (auto& pair : latmap) {
                        str << "<br/>";
                        TVector<TDuration>& latencies = pair.second;
                        std::sort(latencies.begin(), latencies.end());
                        for (double percentile : {0.5, 0.9, 0.95, 0.99, 0.999, 1.0}) {
                            TDuration value = latencies[size_t(percentile * (latencies.size() - 1))];
                            PARAM(Sprintf("Size# %" PRIu32 " Percentile# %.3f", pair.first, percentile), value);
                        }
                    }
                    PARAM("Average speed since start, MB/s", Report->GetAverageSpeed() / 1e6);
                    PARAM("Speed standard deviation since start, MB/s", Report->GetSpeedDeviation() / 1e6);
                    for (double percentile : {0.5, 0.9, 0.95, 0.99, 0.999, 1.0}) {
                        size_t value = Report->LatencyUs.GetPercentile(percentile);
                        PARAM(Sprintf("percentile# %.3f since start, ms", percentile), value / 1000.0);
                    }
                }
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), ev->Get()->SubRequestId));
    }

    STRICT_STFUNC(StateFunc,
        CFunc(TEvents::TSystem::Wakeup, HandleWakeup)
        CFunc(TEvents::TSystem::PoisonPill, HandlePoisonPill)
        HFunc(TEvRegisterPDiskLoadActorResult, Handle)
        HFunc(NPDisk::TEvYardInitResult, Handle)
        HFunc(NPDisk::TEvHarakiriResult, Handle)
        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(NPDisk::TEvChunkWriteResult, Handle)
        HFunc(NPDisk::TEvChunkReserveResult, Handle)
        HFunc(NPDisk::TEvLogResult, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

IActor *CreatePDiskWriterLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskWriteLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TPDiskWriterLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
