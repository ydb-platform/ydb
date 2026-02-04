#include "service_actor.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/blobstorage/ddisk/ddisk.h>
#include <ydb/core/control/lib/dynamic_control_board_impl.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/time_provider/time_provider.h>

#include <util/random/fast.h>
#include <util/random/shuffle.h>
#include <util/generic/queue.h>

#include <algorithm>
#include <cstring>

namespace NKikimr {

class TDDiskWriterLoadTestActor : public TActorBootstrapped<TDDiskWriterLoadTestActor> {
    static constexpr ui32 WriteSizeBytes = 4096;

    struct TAreaInfo {
        // write positions as indices for every WriteSizeBytes
        TDeque<ui32> WriteQueue;
        ui32 AreaSizeBytes = 0;
        ui32 Weight = 1;
        ui64 AccumWeight = 0;
        bool Sequential = true;

        NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::EAreaInit InitType =
            NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_NONE;

        ui64 BaseChunkIndex = 0; // index of the first area chunk
        ui32 NumChunks = 0;
        ui32 InitNextChunk = 0;
        ui32 InitNextPosition = 0;

        struct TFindByWeight {
            bool operator ()(ui64 left, const TAreaInfo& right) const {
                return left < right.AccumWeight;
            }
        };
    };
    struct TRequestInfo {
        ui32 Size;
        TInstant StartTime;
        bool IsInit = false;
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
    ui32 DurationSeconds = 0;
    TDuration DelayBeforeMeasurements;
    ui32 IntervalMsMin = 0;
    ui32 IntervalMsMax = 0;
    TControlWrapper MaxInFlight;
    ui32 InFlight = 0;
    TInstant LastRequest;
    ui32 IntervalMs = 0;

    ui32 DDiskNodeId = 0;
    ui32 DDiskPDiskId = 0;
    ui32 DDiskSlotId = 0;
    TActorId DDiskServiceId;
    NDDisk::TQueryCredentials Credentials;
    bool Connected = false;
    bool DisconnectSent = false;
    bool TestStarted = false;

    TVector<TAreaInfo> Areas;
    ui64 TotalWeight = 0;

    ui32 CurrentInitArea = 0;
    TString ZeroData;
    bool Initializing = false;
    ui32 InitInFlightMax = 0;

    TReallyFastRng32 Rng;
    TString RandomData;

    TString WriteSizeInfo = ToString(WriteSizeBytes);
    TString SequentialInfo = "unknown";

    ui64 ExpectedChunkSizeBytes = 0;

    TInstant TestStartTime;
    TInstant MeasurementStartTime;

    ui64 Write_RequestsSent = 0;
    ui64 Write_OK = 0;
    ui64 Write_Error = 0;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;

    TIntrusivePtr<TEvLoad::TLoadReport> Report;
    TMultiMap<TInstant, TRequestStat> TimeSeries;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_DDISK_WRITE;
    }

    TDDiskWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TDDiskWriteLoad& cmd, const TActorId& parent,
            const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 /*index*/, ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , MaxInFlight(4, 0, 65536)
        , Rng(Now().GetValue())
        , Report(new TEvLoad::TLoadReport())
    {
        VERIFY_PARAM(DurationSeconds);
        DurationSeconds = cmd.GetDurationSeconds();
        DelayBeforeMeasurements = TDuration::Seconds(cmd.GetDelayBeforeMeasurementsSeconds());
        Y_ASSERT(DurationSeconds > DelayBeforeMeasurements.Seconds());
        Report->Duration = TDuration::Seconds(DurationSeconds);
        Report->LoadType = TEvLoad::TLoadReport::LOAD_WRITE;

        VERIFY_PARAM(InFlightWrites);
        MaxInFlight = cmd.GetInFlightWrites();
        Report->InFlight = MaxInFlight;

        VERIFY_PARAM(DDiskId);
        const auto& ddiskId = cmd.GetDDiskId();
        DDiskNodeId = ddiskId.GetNodeId();
        DDiskPDiskId = ddiskId.GetPDiskId();
        DDiskSlotId = ddiskId.GetDDiskSlotId();
        DDiskServiceId = MakeBlobStorageDDiskId(DDiskNodeId, DDiskPDiskId, DDiskSlotId);

        InitInFlightMax = cmd.GetInitInFlightWrites()
            ? cmd.GetInitInFlightWrites()
            : static_cast<ui32>(MaxInFlight);
        Y_ABORT_UNLESS(InitInFlightMax, "InitInFlightWrites must be non-zero");

        IntervalMsMin = cmd.GetIntervalMsMin();
        IntervalMsMax = cmd.GetIntervalMsMax();

        Credentials.TabletId = Tag ? Tag : 1;
        Credentials.Generation = 1;

        VERIFY_PARAM(ExpectedChunkSize);
        ExpectedChunkSizeBytes = cmd.GetExpectedChunkSize();
        Y_ABORT_UNLESS(ExpectedChunkSizeBytes, "ExpectedChunkSize must be non-zero");
        Y_ABORT_UNLESS(ExpectedChunkSizeBytes <= Max<ui32>(), "ExpectedChunkSize must fit into 32-bit offset");
        Y_ABORT_UNLESS(ExpectedChunkSizeBytes % WriteSizeBytes == 0,
            "ExpectedChunkSize must be divisible by WriteSizeBytes");

        ui64 nextBaseChunk = 0;
        ui64 accumWeight = 0;
        for (const auto& area : cmd.GetAreas()) {
            const ui32 areaSize = area.GetAreaSize();
            if (!areaSize) {
                ythrow TLoadActorException() << "area.AreaSize field is missing or zero";
            }
            if (areaSize % WriteSizeBytes != 0) {
                ythrow TLoadActorException() << "area.AreaSize must be divisible by WriteSizeBytes";
            }
            Y_ABORT_UNLESS(area.GetWeight(), "area.Weight must be non-zero");
            const ui32 numChunks = (areaSize + ExpectedChunkSizeBytes - 1) / ExpectedChunkSizeBytes;
            Areas.push_back(TAreaInfo{
                {},
                areaSize,
                area.GetWeight(),
                accumWeight,
                area.GetSequential(),
                area.GetInitType(),
                nextBaseChunk,
                numChunks,
                0,
                0
            });
            accumWeight += area.GetWeight();
            nextBaseChunk += numChunks;
        }
        TotalWeight = accumWeight;
        if (Areas.empty()) {
            ythrow TLoadActorException() << "Areas may not be empty";
        }

        const bool sequentialSample = Areas.front().Sequential;
        bool uniformSequential = true;
        for (const auto& area : Areas) {
            if (area.Sequential != sequentialSample) {
                uniformSequential = false;
                break;
            }
        }
        if (uniformSequential) {
            SequentialInfo = sequentialSample ? "true" : "false";
        } else {
            SequentialInfo = "varies";
        }

        // Monitoring initialization
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                GetSubgroup("ddisk", Sprintf("%" PRIu32 ":%" PRIu32 ":%" PRIu32,
                        DDiskNodeId, DDiskPDiskId, DDiskSlotId));
        BytesWritten = LoadCounters->GetCounter("LoadActorBytesWritten", true);
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorWriteDuration", "Time in microseconds", percentiles);
    }

    ~TDDiskWriterLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TDDiskWriterLoadTestActor::StateFunc);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        AppData(ctx)->Dcb->RegisterLocalControl(MaxInFlight, Sprintf("DDiskWriteLoadActor_MaxInFlight_%4" PRIu64, Tag).c_str());
        SendRequest(ctx, std::make_unique<NDDisk::TEvConnect>(Credentials));
    }

    void Handle(NDDisk::TEvConnectResult::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get()->Record;
        if (msg.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            TStringStream str;
            str << "ddisk connect failed, Status# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(msg.GetStatus());
            LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "%s", str.Str().c_str());
            ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, nullptr, str.Str()));
            Die(ctx);
            return;
        }

        Connected = true;
        Credentials.DDiskInstanceGuid = msg.GetDDiskInstanceGuid();

        RandomData = TString::Uninitialized(WriteSizeBytes);
        for (ui32 i = 0; i < WriteSizeBytes; ++i) {
            RandomData[i] = Rng();
        }

        ZeroData = TString::Uninitialized(WriteSizeBytes);
        memset(ZeroData.Detach(), 0, ZeroData.size());

        for (auto& area : Areas) {
            const ui32 positions = area.AreaSizeBytes / WriteSizeBytes;
            Y_ABORT_UNLESS(positions, "WriteSizeBytes must be smaller than AreaSizeBytes");
            if (area.InitType != NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_NONE) {
                Initializing = true;
            }
            FillWritePositions(area.WriteQueue, positions, area.Sequential);
        }
        SendWriteRequests(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Rate management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleWakeup(const TActorContext& ctx) {
        SendWriteRequests(ctx);
    }

    void FillWritePositions(TDeque<ui32>& queue, ui32 positionsCount, bool sequential) {
        TVector<ui32> positions;
        positions.reserve(positionsCount);
        for (ui32 i = 0; i < positionsCount; ++i) {
            positions.push_back(i);
        }
        if (!sequential) {
            Shuffle(positions.begin(), positions.end());
        }
        for (ui32 pos : positions) {
            queue.push_back(pos);
        }
    }

    bool HasPendingInit() const {
        for (const auto& area : Areas) {
            if (area.InitType == NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_ZEROES_FIRST_BLOCK) {
                if (area.InitNextChunk < area.NumChunks) {
                    return true;
                }
            } else if (area.InitType ==
                    NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_ZEROES_FULL) {
                const ui32 positions = area.AreaSizeBytes / WriteSizeBytes;
                if (area.InitNextPosition < positions) {
                    return true;
                }
            }
        }
        return false;
    }

    TAreaInfo& PickAreaByWeight() {
        Y_DEBUG_ABORT_UNLESS(TotalWeight, "TotalWeight must be non-zero");
        const ui64 w = (ui64(Rng()) << 32 | Rng()) % TotalWeight;
        auto it = std::prev(std::upper_bound(Areas.begin(), Areas.end(), w, TAreaInfo::TFindByWeight()));
        return *it;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        MaxInFlight = 0;
        CheckDie(ctx);
    }

    void CheckDie(const TActorContext& ctx) {
        if (!MaxInFlight && !InFlight) {
            if (Connected && !DisconnectSent) {
                DisconnectSent = true;
                auto ev = std::make_unique<NDDisk::TEvDisconnect>();
                Credentials.Serialize(ev->Record.MutableCredentials());
                SendRequest(ctx, std::move(ev));
            } else {
                ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, Report, "OK"));
                Die(ctx);
            }
        }
    }

    void Handle(NDDisk::TEvDisconnectResult::TPtr& /*ev*/, const TActorContext& ctx) {
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
    // DDisk writing
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void SendWriteRequests(const TActorContext& ctx) {
        if (!Connected) {
            return;
        }

        if (Initializing) {
            SendInitRequests(ctx);
            CheckDie(ctx);
            return;
        }

        if (!TestStarted) {
            TestStarted = true;
            TestStartTime = TAppData::TimeProvider->Now();
            MeasurementStartTime = TestStartTime + DelayBeforeMeasurements;
            ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        }

        while (InFlight < MaxInFlight) {
            // Randomize interval (if required)
            if (!IntervalMs && IntervalMsMax && IntervalMsMin) {
                IntervalMs = IntervalMsMin;
                if (ui32 delta = (IntervalMsMax > IntervalMsMin ? IntervalMsMax - IntervalMsMin : 0)) {
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

            TAreaInfo& area = PickAreaByWeight();
            if (area.WriteQueue.empty()) {
                const ui32 positions = area.AreaSizeBytes / WriteSizeBytes;
                Y_ABORT_UNLESS(positions, "WriteSizeBytes must be smaller than AreaSizeBytes");
                FillWritePositions(area.WriteQueue, positions, area.Sequential);
            }
            const ui32 writeIndex = area.WriteQueue.front();
            area.WriteQueue.pop_front();
            area.WriteQueue.push_back(writeIndex);

            const ui32 offset = writeIndex * WriteSizeBytes;
            const ui32 size = WriteSizeBytes;
            Report->Size = size;

            const TInstant now = TAppData::TimeProvider->Now();
            const ui64 requestIdx = NewTRequestInfo(size, now, false);
            const ui64 vChunkIndex = area.BaseChunkIndex + offset / ExpectedChunkSizeBytes;
            const ui32 offsetInChunk = offset % ExpectedChunkSizeBytes;
            auto ev = std::make_unique<NDDisk::TEvWrite>(Credentials, NDDisk::TBlockSelector(vChunkIndex, offsetInChunk, size),
                NDDisk::TWriteInstruction(0));
            ev->AddPayload(TRope(RandomData));
            SendRequest(ctx, std::move(ev), requestIdx);
            ++Write_RequestsSent;
            ++InFlight;
        }

        CheckDie(ctx);
    }

    void SendInitRequests(const TActorContext& ctx) {
        while (InFlight < InitInFlightMax) {
            bool sent = false;
            for (ui32 i = 0; i < Areas.size(); ++i) {
                const ui32 idx = (CurrentInitArea + i) % Areas.size();
                TAreaInfo& area = Areas[idx];
                if (area.InitType == NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_NONE) {
                    continue;
                }

                ui32 offset = 0;
                ui64 vChunkIndex = 0;
                ui32 offsetInChunk = 0;
                if (area.InitType ==
                        NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_ZEROES_FIRST_BLOCK) {
                    if (area.InitNextChunk >= area.NumChunks) {
                        continue;
                    }
                    vChunkIndex = area.BaseChunkIndex + area.InitNextChunk;
                    offsetInChunk = 0;
                } else {
                    const ui32 positions = area.AreaSizeBytes / WriteSizeBytes;
                    if (area.InitNextPosition >= positions) {
                        continue;
                    }
                    offset = area.InitNextPosition * WriteSizeBytes;
                    vChunkIndex = area.BaseChunkIndex + offset / ExpectedChunkSizeBytes;
                    offsetInChunk = offset % ExpectedChunkSizeBytes;
                }

                const ui32 size = WriteSizeBytes;

                const TInstant now = TAppData::TimeProvider->Now();
                const ui64 requestIdx = NewTRequestInfo(size, now, true);
                auto ev = std::make_unique<NDDisk::TEvWrite>(Credentials,
                    NDDisk::TBlockSelector(vChunkIndex, offsetInChunk, size), NDDisk::TWriteInstruction(0));
                ev->AddPayload(TRope(ZeroData));
                SendRequest(ctx, std::move(ev), requestIdx);
                ++InFlight;

                if (area.InitType ==
                        NKikimr::TEvLoadTestRequest::TDDiskWriteLoad::TWriteArea::INIT_ZEROES_FIRST_BLOCK) {
                    ++area.InitNextChunk;
                } else {
                    ++area.InitNextPosition;
                }

                CurrentInitArea = (idx + 1) % Areas.size();
                sent = true;
                break;
            }
            if (!sent) {
                break;
            }
        }

        if (!HasPendingInit() && InFlight == 0) {
            Initializing = false;
            SendWriteRequests(ctx);
        }
    }

    void Handle(NDDisk::TEvWriteResult::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get()->Record;
        const bool ok = msg.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        const ui64 requestIdx = ev->Cookie;
        FinishRequest(ctx, requestIdx, ok);
        CheckDie(ctx);
    }

    ui64 NewTRequestInfo(ui32 size, TInstant startTime, bool isInit) {
        const ui64 requestIdx = NextRequestIdx++;
        RequestInfo.emplace(requestIdx, TRequestInfo{size, startTime, isInit});
        return requestIdx;
    }

    void FinishRequest(const TActorContext& ctx, ui64 requestIdx, bool ok) {
        const TInstant now = TAppData::TimeProvider->Now();
        auto it = RequestInfo.find(requestIdx);
        if (it == RequestInfo.end()) {
            return;
        }
        const TRequestInfo& request = it->second;

        if (!request.IsInit) {
            if (ok) {
                ++Write_OK;
            } else {
                ++Write_Error;
            }

            if (now > MeasurementStartTime) {
                Report->LatencyUs.Increment((now - request.StartTime).MicroSeconds());
            }

            *BytesWritten += request.Size;
            TimeSeries.emplace(now, TRequestStat{
                    static_cast<ui64>(*BytesWritten),
                    request.Size,
                    now - request.StartTime
                });
            ResponseTimes.Increment((now - request.StartTime).MicroSeconds());
            // cut time series to 60 seconds
            auto pos = TimeSeries.upper_bound(now - TDuration::Seconds(60));
            TimeSeries.erase(TimeSeries.begin(), pos);
        }
        --InFlight;
        RequestInfo.erase(it);
        SendWriteRequests(ctx);
    }

    template<typename TRequest>
    void SendRequest(const TActorContext& ctx, std::unique_ptr<TRequest>&& request, ui64 cookie = 0) {
        ctx.Send(DDiskServiceId, request.release(), 0, cookie);
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
                    PARAM("TEvWrite msgs sent", Write_RequestsSent);
                    PARAM("TEvWriteResult msgs received, OK", Write_OK);
                    PARAM("TEvWriteResult msgs received, not OK", Write_Error);
                    PARAM("Bytes written", static_cast<ui64>(*BytesWritten));
                    PARAM("DDiskId", Sprintf("%" PRIu32 ":%" PRIu32 ":%" PRIu32, DDiskNodeId, DDiskPDiskId, DDiskSlotId));
                    PARAM("Write size", WriteSizeInfo);
                    PARAM("Sequential", SequentialInfo);

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
        HFunc(NDDisk::TEvConnectResult, Handle)
        HFunc(NDDisk::TEvDisconnectResult, Handle)
        HFunc(NDDisk::TEvWriteResult, Handle)
        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

IActor *CreateDDiskWriterLoadTest(const NKikimr::TEvLoadTestRequest::TDDiskWriteLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TDDiskWriterLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
