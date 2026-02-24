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
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <memory>

namespace NKikimr {

namespace {

class TPersistentBufferWriterLoadTestActor : public TActorBootstrapped<TPersistentBufferWriterLoadTestActor> {
    static constexpr ui32 SectorSize = 4096;

    struct TWriteInfo {
        ui32 Size;
        ui32 Weight = 1;
        ui32 AccumWeight = 0;
        TRope Data;

        struct TFindByWeight {
            bool operator ()(ui32 left, const TWriteInfo& right) const {
                return left < right.AccumWeight;
            }
        };
    };

    struct TRequestInfo {
        ui32 Size;
        TInstant StartTime;
        bool IsErase = false;
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
    TControlWrapper MaxInFlight;
    ui32 InFlight = 0;
    TInstant LastRequest;
    ui32 FillRatio = 0;

    ui32 DDiskNodeId = 0;
    ui32 DDiskPDiskId = 0;
    ui32 DDiskSlotId = 0;
    TActorId DDiskServiceId;
    NDDisk::TQueryCredentials Credentials;
    bool Finished = false;
    bool Connected = false;
    bool DisconnectSent = false;
    bool TestStarted = false;

    std::vector<TWriteInfo> WriteInfos;
    ui32 TotalWeight = 0;

    std::map<ui64, ui64> Lsns;
    ui64 WriteSizeBytes = 0;
    double FreeSpace = 0;


    TReallyFastRng32 Rng;

    TString WriteSizeInfo = ToString(WriteSizeBytes);
    TString SequentialInfo = "unknown";


    TInstant TestStartTime;
    TInstant MeasurementStartTime;

    ui64 Write_RequestsSent = 0;
    ui64 Erase_RequestsSent = 0;
    ui64 Write_OK = 0;
    ui64 Write_Error = 0;
    ui64 Erase_OK = 0;
    ui64 Erase_Error = 0;

    // Monitoring
    TIntrusivePtr<::NMonitoring::TDynamicCounters> LoadCounters;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
    NMonitoring::TPercentileTrackerLg<6, 5, 15> ResponseTimes;

    TIntrusivePtr<TEvLoad::TLoadReport> Report;
    TMultiMap<TInstant, TRequestStat> TimeSeries;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_PERSISTENT_BUFFER_WRITE;
    }

    TPersistentBufferWriterLoadTestActor(const NKikimr::TEvLoadTestRequest::TPersistentBufferWriteLoad& cmd, const TActorId& parent,
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

        Credentials.TabletId = Tag ? Tag : 1;
        Credentials.Generation = 1;

        VERIFY_PARAM(FillRatio);
        FillRatio = cmd.GetFillRatio();
        Y_ABORT_UNLESS(FillRatio <= 100, "FillRatio percentage should be less than or equal to 100");

        ui32 accumWeight = 0;
        for (auto wi : cmd.GetWriteInfos()) {
            ui32 size = wi.GetSize();
            ui32 weight = wi.GetWeight();
            Y_ABORT_UNLESS(weight, "WriteInfo.Weight must be non-zero");
            if (!size) {
                ythrow TLoadActorException() << "WriteInfo.Size field is missing or zero";
            }
            if (size % SectorSize != 0) {
                ythrow TLoadActorException() << "WriteInfo.Size must be divisible by SectorSize";
            }
            accumWeight += weight;

            WriteInfos.push_back(TWriteInfo{size, weight, accumWeight, BuildPayload(size)});
        }
        TotalWeight = accumWeight;
        if (WriteInfos.empty()) {
            ythrow TLoadActorException() << "WriteInfos may not be empty";
        }

        // Monitoring initialization
        LoadCounters = counters->GetSubgroup("tag", Sprintf("%" PRIu64, tag))->
                GetSubgroup("ddisk", Sprintf("%" PRIu32 ":%" PRIu32 ":%" PRIu32,
                        DDiskNodeId, DDiskPDiskId, DDiskSlotId));
        BytesWritten = LoadCounters->GetCounter("LoadActorBytesWritten", true);
        TVector<float> percentiles {0.1f, 0.5f, 0.9f, 0.99f, 0.999f, 1.0f};
        ResponseTimes.Initialize(LoadCounters, "subsystem", "LoadActorWriteDuration", "Time in microseconds", percentiles);
    }

    ~TPersistentBufferWriterLoadTestActor() {
        LoadCounters->ResetCounters();
    }

    void Bootstrap(const TActorContext& ctx) {
        Become(&TPersistentBufferWriterLoadTestActor::StateFunc);
        ctx.Schedule(TDuration::MilliSeconds(MonitoringUpdateCycleMs), new TEvUpdateMonitoring);
        AppData(ctx)->Dcb->RegisterLocalControl(MaxInFlight, Sprintf("PersistentBufferWriteLoadActor_MaxInFlight_%4" PRIu64, Tag).c_str());
        SendRequest(ctx, std::make_unique<NDDisk::TEvConnect>(Credentials));
    }

    void Handle(NDDisk::TEvConnectResult::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get()->Record;
        if (msg.GetStatus() != NKikimrBlobStorage::NDDisk::TReplyStatus::OK) {
            TStringStream str;
            str << "persistent buffer connect failed, Status# " << NKikimrBlobStorage::NDDisk::TReplyStatus::E_Name(msg.GetStatus());
            LOG_INFO(ctx, NKikimrServices::BS_LOAD_TEST, "%s", str.Str().c_str());
            FinishAndDie(ctx, str.Str());
            return;
        }

        Connected = true;
        Credentials.DDiskInstanceGuid = msg.GetDDiskInstanceGuid();

        PrepareDataAndStart(ctx);
    }

    void PrepareDataAndStart(const TActorContext& ctx) {
        SendWriteRequests(ctx);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Rate management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandleWakeup(const TActorContext& ctx) {
        SendWriteRequests(ctx);
    }

    TRope BuildPayload(ui32 size) {
        TRcBuf res = TRcBuf::Uninitialized(size);
        char* p = res.GetDataMut();
        for (size_t i = 0; i < size; ++i) {
            p[i] = Rng();
        }
        return TRope(res);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Death management
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    void HandlePoisonPill(const TActorContext& ctx) {
        MaxInFlight = 0;
        CheckDie(ctx);
    }

    void FinishAndDie(const TActorContext& ctx, const TString& status = "OK") {
        if (Finished) {
            return;
        }
        Finished = true;
        Report->Size /= Write_RequestsSent;
        ctx.Send(Parent, new TEvLoad::TEvLoadTestFinished(Tag, Report, status));
        Die(ctx);
    }

    void CheckDie(const TActorContext& ctx) {
        if (!MaxInFlight && !InFlight) {
            if (Connected && !DisconnectSent) {
                DisconnectSent = true;
                auto ev = std::make_unique<NDDisk::TEvDisconnect>();
                Credentials.Serialize(ev->Record.MutableCredentials());
                SendRequest(ctx, std::move(ev));
            } else {
                FinishAndDie(ctx);
            }
        }
    }

    void Handle(NDDisk::TEvDisconnectResult::TPtr& /*ev*/, const TActorContext& ctx) {
        FinishAndDie(ctx);
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
    // PB writing
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    TWriteInfo& PickWriteByWeight() {
        Y_DEBUG_ABORT_UNLESS(TotalWeight, "TotalWeight must be non-zero");
        const ui32 w = Rng() % TotalWeight;
        auto it = std::upper_bound(WriteInfos.begin(), WriteInfos.end(), w, TWriteInfo::TFindByWeight());
        if (it == WriteInfos.end()) {
            it = std::prev(it);
        }
        return *it;
    }

    void SendWriteRequests(const TActorContext& ctx) {
        if (!Connected) {
            return;
        }

        if (!TestStarted) {
            TestStarted = true;
            TestStartTime = TAppData::TimeProvider->Now();
            MeasurementStartTime = TestStartTime + DelayBeforeMeasurements;
            ctx.Schedule(TDuration::Seconds(DurationSeconds), new TEvents::TEvPoisonPill);
        }

        while (InFlight < MaxInFlight) {
            bool doWrite = Rng() % 2;
            if (Lsns.empty() || doWrite || FillRatio < FreeSpace * 100) {
                TWriteInfo& write = PickWriteByWeight();
                Report->Size += write.Size;
                const TInstant now = TAppData::TimeProvider->Now();
                const ui64 requestIdx = NewTRequestInfo(write.Size, now, false);

                auto ev = std::make_unique<NDDisk::TEvWritePersistentBuffer>(Credentials,
                    NDDisk::TBlockSelector(1, 0, write.Size),
                    requestIdx, NDDisk::TWriteInstruction(0));
                ev->AddPayload(BuildPayload(write.Size));
                SendRequest(ctx, std::move(ev), requestIdx);
                ++Write_RequestsSent;
                ++InFlight;
            } else {
                auto it = Lsns.begin();
                std::advance(it, Rng() % Lsns.size());
                const TInstant now = TAppData::TimeProvider->Now();
                const ui64 requestIdx = NewTRequestInfo(it->second, now, true);

                auto ev = std::make_unique<NDDisk::TEvErasePersistentBuffer>(Credentials,
                    NDDisk::TBlockSelector(1, 0, it->second),
                    it->first);
                SendRequest(ctx, std::move(ev), requestIdx);
                Lsns.erase(it);

                ++Erase_RequestsSent;
                ++InFlight;
            }
        }

        CheckDie(ctx);
    }

    void Handle(NDDisk::TEvWritePersistentBufferResult::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get()->Record;
        const bool ok = msg.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        const ui64 requestIdx = ev->Cookie;
        FreeSpace = msg.GetFreeSpace();
        FinishRequest(ctx, requestIdx, ok);
        CheckDie(ctx);
    }

    void Handle(NDDisk::TEvErasePersistentBufferResult::TPtr& ev, const TActorContext& ctx) {
        const auto& msg = ev->Get()->Record;
        const bool ok = msg.GetStatus() == NKikimrBlobStorage::NDDisk::TReplyStatus::OK;
        const ui64 requestIdx = ev->Cookie;
        FreeSpace = msg.GetFreeSpace();
        FinishRequest(ctx, requestIdx, ok);
        CheckDie(ctx);
    }

    ui64 NewTRequestInfo(ui32 size, TInstant startTime, bool isErase) {
        const ui64 requestIdx = NextRequestIdx++;
        RequestInfo.emplace(requestIdx, TRequestInfo{size, startTime, isErase});
        return requestIdx;
    }

    void FinishRequest(const TActorContext& ctx, ui64 requestIdx, bool ok) {
        const TInstant now = TAppData::TimeProvider->Now();
        auto it = RequestInfo.find(requestIdx);
        if (it == RequestInfo.end()) {
            return;
        }
        const TRequestInfo& request = it->second;

        if (request.IsErase) {
            WriteSizeBytes -= request.Size;
            if (ok) {
                ++Erase_OK;
            } else {
                ++Erase_Error;
            }
        } else {
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
            Lsns.insert({requestIdx, request.Size});
            WriteSizeBytes += request.Size;
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
                    PARAM("TEvErasePersistentBuffer msgs sent", Erase_RequestsSent);
                    PARAM("TEvErasePersistentBufferResult msgs received, OK", Erase_OK);
                    PARAM("TEvErasePersistentBufferResult msgs received, not OK", Erase_Error);
                    PARAM("TEvWritePersistentBuffer msgs sent", Write_RequestsSent);
                    PARAM("TEvWritePersistentBufferResult msgs received, OK", Write_OK);
                    PARAM("TEvWritePersistentBufferResult msgs received, not OK", Write_Error);
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
        HFunc(NDDisk::TEvWritePersistentBufferResult, Handle)
        HFunc(NDDisk::TEvErasePersistentBufferResult, Handle)
        HFunc(TEvUpdateMonitoring, Handle)
        HFunc(NMon::TEvHttpInfo, Handle)
    )
};

} // namespace

IActor *CreatePersistentBufferWriterLoadTest(const NKikimr::TEvLoadTestRequest::TPersistentBufferWriteLoad& cmd,
        const TActorId& parent, const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 index, ui64 tag) {
    return new TPersistentBufferWriterLoadTestActor(cmd, parent, counters, index, tag);
}

} // NKikimr
