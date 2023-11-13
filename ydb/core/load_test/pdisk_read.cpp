#include "service_actor.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>
#include <util/random/fast.h>
#include <util/generic/queue.h>

namespace NKikimr {

class TPDiskReaderLoadTestActor : public TActorBootstrapped<TPDiskReaderLoadTestActor> {
    struct TChunkInfo {
        TChunkIdx Idx;
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
        ui32 Size = 0;
        TChunkIdx ChunkIdx = 0;
        TInstant StartTime;

        TRequestInfo(ui32 size, TChunkIdx chunkIdx, TInstant startTime)
            : Size(size)
            , ChunkIdx(chunkIdx)
            , StartTime(startTime)
        {}

        TRequestInfo(const TRequestInfo &) = default;
        TRequestInfo() = default;
        TRequestInfo &operator=(const TRequestInfo& other) = default;
    };

    struct TRequestStat {
        ui64 BytesReadTotal;
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
    ui32 SlotIndex = 0;
    TInstant LastRequest;
    TInstant TestStartTime = TInstant::Max();
    TInstant MeasurementStartTime = TInstant::Max();
    double IntervalMs = 0;
    ui32 PDiskId;
    TVDiskID VDiskId;
    NPDisk::TOwnerRound OwnerRound;
    ui64 PDiskGuid;
    TIntrusivePtr<TPDiskParams> PDiskParams;
    TVector<TChunkInfo> Chunks;
    TReallyFastRng32 Rng;
    TString DataBuffer;
    ui64 Lsn = 1;
    TMultiMap<TInstant, TRequestStat> TimeSeries;
    TVector<TChunkIdx> DeleteChunks;
    bool Sequential;
    bool Harakiri = false;
    bool IsWardenlessTest;

    // statistics
    ui64 ChunkReserve_RequestsSent = 0;
    ui64 ChunkRead_RequestsSent = 0;
    ui64 ChunkRead_OK = 0;
    ui64 ChunkRead_NonOK = 0;
    ui64 ChunkWrite_RequestsSent = 0;
    ui64 ChunkWrite_OK = 0;
    ui64 DeletedChunksCount = 0;

    // Monitoring
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesRead;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    TIntrusivePtr<TEvLoad::TLoadReport> Report;
    TIntrusivePtr<NMonitoring::TCounterForPtr> PDiskBytesRead;
    TMap<double, TIntrusivePtr<NMonitoring::TCounterForPtr>> DevicePercentiles;

    TString ErrorReason;
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PDISK_READ;
    }

    TPDiskReaderLoadTestActor(const NKikimr::TEvLoadTestRequest::TPDiskReadLoad& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , MaxInFlight(4, 0, 65536)
        , OwnerRound(1000 + index)
        , Rng(Now().GetValue())
        , Report(new TEvLoad::TLoadReport())
    {
        ErrorReason = "Still waiting for TEvRegisterPDiskLoadActorResult";

        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());
        Report->Duration = TDuration::Seconds(DurationSeconds);

        IntervalMsMin = cmd.GetIntervalMsMin();
        IntervalMsMax = cmd.GetIntervalMsMax();

        VERIFY_PARAM(InFlightReads);
        MaxInFlight = cmd.GetInFlightReads();
        Report->InFlight = MaxInFlight;

        VERIFY_PARAM(PDiskId);
        PDiskId = cmd.GetPDiskId();

        VERIFY_PARAM(PDiskGuid);
        PDiskGuid = cmd.GetPDiskGuid();

        VERIFY_PARAM(VDiskId);
        VDiskId = VDiskIDFromVDiskID(cmd.GetVDiskId());

        Sequential = cmd.GetSequential();
        IsWardenlessTest = cmd.GetIsWardenlessTest();

        for (const auto& chunk : cmd.GetChunks()) {
            if (!chunk.HasSlots() || !chunk.HasWeight() || !chunk.GetSlots() || !chunk.GetWeight()) {
                ythrow TLoadActorException() << "chunk.Slots/Weight fields are either missing or zero";
            }
            Chunks.push_back(TChunkInfo{
                    0,
                    chunk.GetSlots(),
                    0,
                    chunk.GetWeight(),
                    0,
                });
        }

        // Monitoring initialization
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                                 GetSubgroup("pdisk", Sprintf("%09" PRIu32, PDiskId));
        BytesRead = LoadCounters->GetCounter("LoadActorBytesRead", true);
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorReadDuration", "Time in microseconds", percentiles);

        TIntrusivePtr<::NMonitoring::TDynamicCounters> pDiskCounters = GetServiceCounters(counters, "pdisks")->
            GetSubgroup("pdisk", Sprintf("%09" PRIu32, PDiskId));
        PDiskBytesRead = pDiskCounters->GetSubgroup("subsystem", "device")->GetCounter("DeviceBytesRead", true);
        TIntrusivePtr<::NMonitoring::TDynamicCounters> percentilesGroup;
        percentilesGroup = pDiskCounters->GetSubgroup("subsystem", "deviceReadDuration")->GetSubgroup("sensor", "Time in microsec");
        for (double percentile : {0.1, 0.5, 0.9, 0.99, 0.999, 1.0}) {
            DevicePercentiles.emplace(percentile, percentilesGroup->GetNamedCounter("percentile",
                Sprintf("%.1f", percentile * 100.f)));
        }

        if (Chunks.empty()) {
            ythrow TLoadActorException() << "Chunks may not be empty";
        }
    }

    ~TPDiskReaderLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPDiskReaderLoadTestActor::StateFunc);
        ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill());
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        AppData(ctx)->Icb->RegisterLocalControl(MaxInFlight, Sprintf("PDiskReadLoadActor_MaxInFlight_%4" PRIu64, Tag).c_str());
        if (IsWardenlessTest) {
            ErrorReason = "Still waiting for YardInitResult";
            SendRequest(ctx, std::make_unique<NPDisk::TEvYardInit>(OwnerRound, VDiskId, PDiskGuid));
        } else {
            Send(MakeBlobStorageNodeWardenID(ctx.SelfID.NodeId()), new TEvRegisterPDiskLoadActor());
        }
    }

    void Handle(TEvRegisterPDiskLoadActorResult::TPtr& ev, const TActorContext& ctx) {
        OwnerRound = ev->Get()->OwnerRound;
        ErrorReason = "Still waiting for YardInitResult";
        SendRequest(ctx, std::make_unique<NPDisk::TEvYardInit>(OwnerRound, VDiskId, PDiskGuid));
    }

    void Handle(NPDisk::TEvYardInitResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status != NKikimrProto::OK) {
            TStringStream str;
            str << "yard init failed, Status# " << NKikimrProto::EReplyStatus_Name(msg->Status);
            ErrorReason = str.Str();
            LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "%s", str.Str().c_str());
            SendRequest(ctx, std::make_unique<TEvents::TEvPoisonPill>());
            return;
        }
        ErrorReason = "OK";
        PDiskParams = msg->PDiskParams;
        DataBuffer = TString::Uninitialized(PDiskParams->ChunkSize);
        char *data = const_cast<char*>(DataBuffer.data());
        for (ui32 i = 0; i < PDiskParams->ChunkSize; ++i) {
            data[i] = Rng();
        }
        for (TChunkInfo& chunk : Chunks) {
            chunk.SlotSizeBlocks = PDiskParams->ChunkSize / PDiskParams->AppendBlockSize / chunk.NumSlots;
        }
        StartAllReservations(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk reservation
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void StartAllReservations(const TActorContext& ctx) {
        SendRequest(ctx, std::make_unique<NPDisk::TEvChunkReserve>(PDiskParams->Owner,
                    PDiskParams->OwnerRound, (ui32)Chunks.size()));
        ++ChunkReserve_RequestsSent;
    }

    ui64 NewTRequestInfo(ui32 size, TChunkIdx chunkIdx, TInstant startTime) {
        ui64 requestIdx = NextRequestIdx;
        RequestInfo[requestIdx] = TRequestInfo(size, chunkIdx, startTime);
        NextRequestIdx++;
        return requestIdx;
    }

    void Handle(NPDisk::TEvChunkReserveResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            for (ui32 i =0; i < Chunks.size(); ++i) {
                TChunkIdx chunkIdx = msg->ChunkIds[i];
                Chunks[i].Idx = chunkIdx;
                ui64 requestIdx = NewTRequestInfo((ui32)DataBuffer.size(), chunkIdx, TAppData::TimeProvider->Now());
                TString tmp = DataBuffer;
                SendRequest(ctx, std::make_unique<NPDisk::TEvChunkWrite>(PDiskParams->Owner, PDiskParams->OwnerRound,
                            chunkIdx, 0u, new NPDisk::TEvChunkWrite::TStrokaBackedUpParts(tmp),
                            reinterpret_cast<void*>(requestIdx), true, NPriWrite::HullHugeAsyncBlob, Sequential));
                ++ChunkWrite_RequestsSent;
            }
        } else {
            Die(ctx);
        }
    }

    void Handle(NPDisk::TEvChunkWriteResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            ui64 requestIdx = reinterpret_cast<ui64>(msg->Cookie);
            SendLogRequest(ctx, requestIdx, msg->ChunkIdx);
            ++ChunkWrite_OK;
        } else {
            TStringStream str;
            str << "Chunk writing failed, loader going to die, Status# "
                << NKikimrProto::EReplyStatus_Name(msg->Status);
            ErrorReason = str.Str();
            LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "%s", str.Str().c_str());
            SendRequest(ctx, std::make_unique<TEvents::TEvPoisonPill>());
        }
        if (ChunkWrite_OK == Chunks.size()) {
            TestStartTime = TAppData::TimeProvider->Now();
            MeasurementStartTime = TestStartTime + DelayBeforeMeasurements;

            SendReadRequests(ctx);
        }
    }



    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Rate management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleWakeup(const TActorContext& ctx) {
        SendReadRequests(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        Report->LoadType = TEvLoad::TLoadReport::LOAD_READ;
        MaxInFlight = 0;
        CheckDie(ctx);
    }

    void CheckDie(const TActorContext& ctx) {
        if (!MaxInFlight && !InFlight && !LogInFlight && !Harakiri) {
            if (PDiskParams) {
                SendRequest(ctx, std::make_unique<NPDisk::TEvHarakiri>(PDiskParams->Owner, PDiskParams->OwnerRound));
                Harakiri = true;
            } else {
                ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, ErrorReason));
                Die(ctx);
            }
        }
    }

    void Handle(NPDisk::TEvHarakiriResult::TPtr& /*ev*/, const TActorContext& ctx) {
        ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, Report, ErrorReason));
        Die(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Monitoring
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void Handle(TEvUpdateMonitoring::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        ResponseTimes.Update();

        const TInstant now = TAppData::TimeProvider->Now();
        if (now > MeasurementStartTime) {
            auto begin = TimeSeries.lower_bound(now - TDuration::MilliSeconds(MonitoringUpdateCycleMs));
            if (begin != TimeSeries.end()) {
                auto end = std::prev(TimeSeries.lower_bound(now));
                if (end != begin) {
                    //double seconds = 1;
                    ui64 speedBps = (end->second.BytesReadTotal - begin->second.BytesReadTotal) /
                        TDuration::MilliSeconds(MonitoringUpdateCycleMs).SecondsFloat();
                    Report->RwSpeedBps.push_back(speedBps);
                } else {
                    Report->RwSpeedBps.push_back(0);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Chunk reading
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendReadRequests(const TActorContext& ctx) {
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
                accumWeight += chunkInfo.Weight;
            }
            if (!accumWeight) {
                break;
            }

            ui64 w = (ui64(Rng()) << 32 | Rng()) % accumWeight;
            auto it = std::prev(std::upper_bound(Chunks.begin(), Chunks.end(), w, TChunkInfo::TFindByWeight()));
            TChunkInfo& chunkInfo = *it;

            TChunkIdx chunkIdx = chunkInfo.Idx;
            if (Sequential) {
                SlotIndex = (SlotIndex + 1) % chunkInfo.NumSlots;
            } else {
                SlotIndex = Rng() % chunkInfo.NumSlots;
            }

            ui32 size = chunkInfo.SlotSizeBlocks * PDiskParams->AppendBlockSize;
            Report->Size = size;
            ui32 offset = SlotIndex * size;
            ui64 requestIdx = NewTRequestInfo(size, chunkIdx, TAppData::TimeProvider->Now());
            SendRequest(ctx, std::make_unique<NPDisk::TEvChunkRead>(PDiskParams->Owner, PDiskParams->OwnerRound,
                    chunkIdx, offset, size, ui8(0), reinterpret_cast<void*>(requestIdx)));
            ++ChunkRead_RequestsSent;

            ++InFlight;
        }

        CheckDie(ctx);
    }

    void Handle(NPDisk::TEvChunkReadResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        if (msg->Status == NKikimrProto::OK) {
            ++ChunkRead_OK;
        } else {
            ++ChunkRead_NonOK;
        }
        ui64 requestIdx = reinterpret_cast<ui64>(msg->Cookie);
        *BytesRead += RequestInfo[requestIdx].Size;

        FinishRequest(ctx, requestIdx);

        CheckDie(ctx);
    }

    void SendLogRequest(const TActorContext& ctx, ui64 requestIdx, TChunkIdx chunkIdx) {
        TString logRecord = "Hello, my dear log! I've just written a chunk!";
        NPDisk::TCommitRecord record;
        record.CommitChunks.push_back(chunkIdx);
        TLsnSeg seg(Lsn, Lsn);
        ++Lsn;
        SendRequest(ctx, std::make_unique<NPDisk::TEvLog>(PDiskParams->Owner, PDiskParams->OwnerRound,
                TLogSignature::SignatureHugeLogoBlob, record, TRcBuf(logRecord), seg,
                reinterpret_cast<void*>(requestIdx)));
        ++LogInFlight;
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
        auto msg = ev->Get();
        for (const auto& res : msg->Results) {
            ui64 requestIdx = reinterpret_cast<ui64>(res.Cookie);
            RequestInfo.erase(requestIdx);
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
                static_cast<ui64>(*BytesRead), // current state of bytes read counter
                request->Size,
                now - request->StartTime
            });
        ResponseTimes.Increment((now - request->StartTime).MicroSeconds());

        RequestInfo.erase(requestIdx);

        // cut time series to 60 seconds
        auto pos = TimeSeries.upper_bound(now - TDuration::Seconds(60));
        TimeSeries.erase(TimeSeries.begin(), pos);
        --InFlight;
        SendReadRequests(ctx);
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
                    PARAM("Current InFlight", InFlight);
                    PARAM("TEvChunkRead msgs sent", ChunkRead_RequestsSent);
                    PARAM("TEvChunkReadResult msgs received, OK", ChunkRead_OK);
                    PARAM("TEvChunkReadResult msgs received, not OK", ChunkRead_NonOK);
                    PARAM("Bytes Read", (i64)*BytesRead);
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
                                double speed = (end->second.BytesReadTotal - it->second.BytesReadTotal) / seconds;
                                speed /= 1e6;
                                PARAM("Average read speed at last " << dt << " seconds, MB/s", Sprintf("%.3f", speed));
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
                    for(const auto& perc : DevicePercentiles) {
                        PARAM(Sprintf("Device percentile# %.3f for last 15 seconds, ms", perc.first), (TAtomicBase)*perc.second);
                    }

                    PARAM("PDiskBytesRead (Solomon counter), MB", (ui64)*PDiskBytesRead / 1e6);
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
        HFunc(NPDisk::TEvChunkReserveResult, Handle)
        HFunc(NPDisk::TEvChunkReadResult, Handle)
        HFunc(NPDisk::TEvChunkWriteResult, Handle)
        HFunc(NPDisk::TEvLogResult, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

IActor *CreatePDiskReaderLoadTest(const NKikimr::TEvLoadTestRequest::TPDiskReadLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters,
        ui64 index, ui64 tag) {
    return new TPDiskReaderLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
