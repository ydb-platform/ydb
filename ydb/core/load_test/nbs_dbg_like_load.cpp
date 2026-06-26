#include "events.h"
#include "nbs_dbg_like_load.h"
#include "nbs_dbg_like_load_defs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/services/blobstorage_service_id.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/util/circular_sparse_queue.h>

#include <ydb/public/lib/base/msgbus.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>
#include <util/generic/hash_set.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/system/hp_timer.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsDbgLikeLoad] " << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsDbgLikeLoad] " << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsDbgLikeLoad] " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsDbgLikeLoad] " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::BS_LOAD_TEST, "[NbsDbgLikeLoad] " << stream)

namespace NKikimr::NNbsDbgLike {

namespace {

constexpr TDuration kDrainTimeout = TDuration::Seconds(30);
constexpr ui64 kWakeupDrainTimeoutTag  = 1;
constexpr ui64 kWakeupInitTimeoutTag   = 2;
constexpr ui64 kWakeupErrorBackoffTag  = 3;
constexpr TDuration kInitTimeout        = TDuration::Seconds(15);
constexpr TDuration kErrorBackoffDuration = TDuration::MilliSeconds(10);
constexpr ui32 kPipeRetryLimit = 3;
constexpr ui32 kMaxInflightPerActor = 512;

// Latency histogram bounds (spec §15.1). Up to ~134s, microsecond precision.
constexpr i64 kLatencyHistMaxUs = 134'000'000;
constexpr i32 kLatencyHistPrecision = 4;

ui64 LatencyUsFromHPTimer(NHPTimer::STime elapsed) {
    if (elapsed <= 0) {
        return 0;
    }
    const double us = NHPTimer::GetSeconds(elapsed) * 1'000'000.0;
    if (us <= 0.0) {
        return 0;
    }
    return static_cast<ui64>(us + 0.5);
}

TRope BuildWritePayload(ui32 size, TFastRng64& rng) {
    TString data;
    data.reserve(size);
    for (ui32 i = 0; i < size; i += 8) {
        const ui64 v = rng.GenRand();
        const ui32 take = Min<ui32>(8, size - i);
        data.AppendNoAlias(reinterpret_cast<const char*>(&v), take);
    }
    return TRope(std::move(data));
}

// Distributes `total` io-units across `count` workers as evenly as possible.
// Workers with index < (total % count) get one extra unit, so shares sum to `total`.
static ui64 SplitShare(ui64 total, ui32 count, ui32 index) {
    if (count == 0) {
        return total;
    }
    return total / count + (index < (total % count) ? 1 : 0);
}

// Returns the start offset for worker `index` — i.e. the sum of all shares
// before it: index * (total/count) + min(index, total%count).
static ui64 SplitStart(ui64 total, ui32 count, ui32 index) {
    if (count == 0) {
        return 0;
    }
    const ui64 base  = total / count;
    const ui64 extra = total % count;
    return base * index + Min<ui64>(index, extra);
}

struct TInflightEntry {
    ui64 Address = 0;
    ui32 SizeBytes = 0;
    NHPTimer::STime SentAt = 0;
    bool IsRead = false;
};

// Summary info resolved by the proxy (from GetSummary + validation).
// Passed to each worker so they skip GetSummary and ConfigureTablet.
struct TResolvedTabletInfo {
    ui32 EffectiveDbgCount = 0;
    ui64 VChunkSizeBytes = 0;
    ui32 TargetNumVChunks = 0;
    ui32 IoSizeBytes = 0;
};

// Builds the combined TEvLoadTestFinished from an already-aggregated
// TNbsDbgLikeFinishStats. Used both by a single load worker and by the
// proxy that merges several workers' stats. `stats` is consumed (its
// histograms are moved into the event's WorkerStats).
TEvLoad::TEvLoadTestFinished* BuildNbsDbgLikeFinishEvent(
    ui64 tag,
    ui64 tabletId,
    const TString& errorReason,
    TNbsDbgLikeFinishStats stats)
{
    const ui64 durationMs = stats.RunningMs;
    const ui64 measuredMs = stats.MeasuredMs;
    const double measuredSec = measuredMs > 0 ? measuredMs / 1000.0 : 1.0;

    // Compute percentiles before moving histograms into WorkerStats.
    const ui64 writeP50Us = stats.WriteE2eUs.GetValueAtPercentile(50.0);
    const ui64 writeP95Us = stats.WriteE2eUs.GetValueAtPercentile(95.0);
    const ui64 writeP99Us = stats.WriteE2eUs.GetValueAtPercentile(99.0);
    const ui64 readP50Us  = stats.ReadPbUs.GetValueAtPercentile(50.0);
    const ui64 readP95Us  = stats.ReadPbUs.GetValueAtPercentile(95.0);
    const ui64 readP99Us  = stats.ReadPbUs.GetValueAtPercentile(99.0);

    LOG_I("Run finished Tag# " << tag
        << " Status# " << (errorReason.empty() ? "OK" : errorReason)
        << " DurationMs# " << durationMs
        << " MeasuredMs# " << measuredMs
        << " WritesIssued# " << stats.WritesIssued
        << " WritesOk# " << stats.WritesOkTotal
        << " MeasuredWritesOk# " << stats.WritesOk
        << " WriteBytes# " << stats.WriteBytesTotal
        << " MeasuredWriteBytes# " << stats.WriteBytes
        << " WriteLatencyP50Us# " << writeP50Us
        << " WriteLatencyP99Us# " << writeP99Us
        << " ReadsIssued# " << stats.ReadsIssuedTotal
        << " ReadsOk# " << stats.ReadsOkTotal
        << " MeasuredReadsOk# " << stats.ReadsPbOk
        << " ReadBytes# " << stats.ReadBytesTotal
        << " MeasuredReadBytes# " << stats.ReadsPbBytes
        << " ReadLatencyP50Us# " << readP50Us
        << " ReadLatencyP99Us# " << readP99Us);

    auto report = MakeIntrusive<TEvLoad::TLoadReport>();
    report->Duration = TDuration::MilliSeconds(durationMs);
    report->Size = stats.WriteBytes + stats.ReadsPbBytes;
    report->InFlight = stats.MaxInFlight;
    report->LoadType = TEvLoad::TLoadReport::LOAD_WRITE;

    auto* finishEv = new TEvLoad::TEvLoadTestFinished(
        tag,
        report,
        errorReason.empty() ? TString{} : errorReason);

    // Build a minimal JsonResult compatible with service_actor's aggregation.
    // The service actor will further enrich this from WorkerStats below.
    const ui64 measuredTxs = stats.WritesOk + stats.ReadsPbOk;
    const ui64 measuredErrors = stats.WritesErr + stats.ReadsErr;
    finishEv->JsonResult["txs"] = measuredTxs;
    finishEv->JsonResult["rps"] = measuredTxs / measuredSec;
    finishEv->JsonResult["errors"] = static_cast<double>(measuredErrors) / measuredSec;
    finishEv->JsonResult["percentile"]["50"] = static_cast<double>(writeP50Us);
    finishEv->JsonResult["percentile"]["95"] = static_cast<double>(writeP95Us);
    finishEv->JsonResult["percentile"]["99"] = static_cast<double>(writeP99Us);

    // Render a brief HTML summary as the "last page".
    {
        TStringStream html;
        html << "<b>NbsDbgLike run summary</b><br/>"
            << "Tag: " << tag << "<br/>"
            << "TabletId: " << tabletId << "<br/>"
            << "MaxInFlight: " << stats.MaxInFlight << "<br/>"
            << "Duration: " << durationMs << " ms (measured: " << measuredMs << " ms)<br/>"
            << "WritesIssued: " << stats.WritesIssued << " (total ok: " << stats.WritesOkTotal
            << ") MeasuredWritesOk: " << stats.WritesOk
            << " MeasuredWriteBytes: " << stats.WriteBytes << "<br/>"
            << "WriteLatency p50=" << writeP50Us << "us p95=" << writeP95Us
            << "us p99=" << writeP99Us << "us<br/>"
            << "ReadsIssued: " << stats.ReadsIssuedTotal << " (total ok: " << stats.ReadsOkTotal
            << ") MeasuredReadsOk: " << stats.ReadsPbOk
            << " MeasuredReadBytes: " << stats.ReadsPbBytes << "<br/>"
            << "ReadLatency p50=" << readP50Us << "us p95=" << readP95Us
            << "us p99=" << readP99Us << "us<br/>";
        if (!errorReason.empty()) {
            html << "<b>Error:</b> " << errorReason << "<br/>";
        }
        finishEv->LastHtmlPage = html.Str();
    }

    // Move typed WorkerStats so the service actor can enrich JsonResult
    // with split write/read keys consumed by the sweep table.
    SetNbsDbgLikeFinishStats(*finishEv, std::move(stats));

    return finishEv;
}

class TNbsDbgLikeLoadActor : public TActorBootstrapped<TNbsDbgLikeLoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE;
    }

    // Workers are constructed by the proxy after it has resolved tablet info.
    // All fields from TResolvedTabletInfo are already validated by the proxy.
    TNbsDbgLikeLoadActor(
        const TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
        const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        ui64 tag,
        ui32 workerIndex,
        ui32 numWorkers,
        TResolvedTabletInfo resolved)
        : Parent(parent)
        , Tag(tag)
        , TabletId(cmd.GetNbsDbgLikeTabletId())
        , Config(cmd.GetWorkloadConfig())
        , WorkerIndex(workerIndex)
        , NumWorkers(numWorkers)
        , EffectiveDbgCount(resolved.EffectiveDbgCount)
        , VChunkSizeBytes(resolved.VChunkSizeBytes)
        , TargetNumVChunks(resolved.TargetNumVChunks)
        , IoSizeBytes(resolved.IoSizeBytes)
        , Rng(TInstant::Now().GetValue() ^ tag)
        , Counters(std::move(counters))
        , Inflight(Max<size_t>(1, Config.GetMaxInFlight()))
        , MeasuredWriteLatencyUs(kLatencyHistMaxUs, kLatencyHistPrecision)
        , MeasuredReadLatencyUs(kLatencyHistMaxUs, kLatencyHistPrecision)
    {
        // Calibrate TSC frequency once (~50ms); must not run on first measured op.
        (void)NHPTimer::GetCyclesPerSecond();
    }

    void Bootstrap() {
        LOG_I("Bootstrap Tag# " << Tag
            << " TabletId# " << TabletId
            << " DurationSeconds# " << Config.GetDurationSeconds()
            << " MaxInFlight# " << Config.GetMaxInFlight()
            << " ReadRatio# " << Config.GetReadRatio()
            << " Sequential# " << Config.GetSequential()
            << " Worker# " << WorkerIndex << "/" << NumWorkers);

        // Compute this worker's slice of the flat io-unit address space.
        // The summary fields (EffectiveDbgCount, VChunkSizeBytes, etc.) were
        // already validated by the proxy.
        ComputeWorkerSlice();
        if (!ErrorReason.empty()) {
            LOG_N("ComputeWorkerSlice error Tag# " << Tag << " " << ErrorReason);
            FinishRun();
            return;
        }

        // Open a pipe for I/O only (no GetSummary, no ConfigureTablet — that
        // was already done by the proxy before spawning us).
        NTabletPipe::TClientConfig pipeConfig{
            .RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}
        };
        PipeClient = Register(NTabletPipe::CreateClient(SelfId(), TabletId, pipeConfig));

        Schedule(kInitTimeout, new TEvents::TEvWakeup(kWakeupInitTimeoutTag));
        Become(&TNbsDbgLikeLoadActor::StateInit);
    }

private:
    // ---- StateInit: wait for pipe connection ----

    void HandlePipeConnectedInit(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }
        if (ev->Get()->Status != NKikimrProto::OK) {
            ErrorReason = TStringBuilder()
                << "pipe connect failed: status=" << static_cast<int>(ev->Get()->Status);
            LOG_N("Pipe connect error Tag# " << Tag << " " << ErrorReason);
            FinishRun();
            return;
        }

        LOG_N("Worker ready Tag# " << Tag
            << " EffectiveDbgCount# " << EffectiveDbgCount
            << " VChunkSizeBytes# " << VChunkSizeBytes
            << " TargetNumVChunks# " << TargetNumVChunks
            << " IoSizeBytes# " << IoSizeBytes
            << " Worker# " << WorkerIndex << "/" << NumWorkers
            << " UnitRange# [" << UnitRangeStart << "," << UnitRangeStart + UnitRangeCount << ")"
            << " — starting load");

        InitCounters();
        WritePayload = BuildWritePayload(IoSizeBytes, Rng);
        TestStartTime = MonotonicNow();
        MeasurementStartTime = TestStartTime
            + TDuration::Seconds(Config.GetDelayBeforeMeasurementsSeconds());

        if (Config.GetDurationSeconds() > 0) {
            Schedule(TDuration::Seconds(Config.GetDurationSeconds()),
                new TEvents::TEvPoisonPill);
        }

        Become(&TNbsDbgLikeLoadActor::StateRunning);
        SendNext();
    }

    void HandleInitPipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }
        PipeClient = TActorId();
        ErrorReason = "pipe lost before connecting";
        FinishRun();
    }

    void HandleInitPoison(TEvents::TEvPoisonPill::TPtr&) {
        ErrorReason = "stopped before start";
        FinishRun();
    }

    void HandleInitWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == kWakeupInitTimeoutTag) {
            LOG_N("Pipe connect timeout after " << kInitTimeout
                << " Tag# " << Tag
                << " TabletId# " << TabletId);
            ErrorReason = TStringBuilder()
                << "pipe connect timed out after " << kInitTimeout
                << " (TabletId " << TabletId << ")";
            FinishRun();
        }
    }

    // ---- Helpers shared by both states ----

    // Compute the derived address-space fields and this worker's io-unit slice.
    // Called during Bootstrap; all summary fields are already set from TResolvedTabletInfo.
    void ComputeWorkerSlice() {
        BytesPerDbg = static_cast<ui64>(TargetNumVChunks) * VChunkSizeBytes;
        AddressSpaceBytes = static_cast<ui64>(EffectiveDbgCount) * BytesPerDbg;
        IoUnitsPerVChunk = VChunkSizeBytes / IoSizeBytes;
        AddressSpaceIoUnits = static_cast<ui64>(EffectiveDbgCount)
            * static_cast<ui64>(TargetNumVChunks) * IoUnitsPerVChunk;

        const ui64 unitsPerDbg = static_cast<ui64>(TargetNumVChunks) * IoUnitsPerVChunk;
        if (EffectiveDbgCount >= NumWorkers) {
            // Preferred: align to whole DBG boundaries.
            UnitRangeStart = SplitStart(EffectiveDbgCount, NumWorkers, WorkerIndex) * unitsPerDbg;
            UnitRangeCount = SplitShare(EffectiveDbgCount, NumWorkers, WorkerIndex) * unitsPerDbg;
        } else {
            // Fewer DBGs than workers: fall back to io-unit granularity.
            UnitRangeStart = SplitStart(AddressSpaceIoUnits, NumWorkers, WorkerIndex);
            UnitRangeCount = SplitShare(AddressSpaceIoUnits, NumWorkers, WorkerIndex);
        }
        if (UnitRangeCount == 0) {
            ErrorReason = TStringBuilder()
                << "Degenerate address space slice for worker #" << WorkerIndex
                << " (AddressSpaceIoUnits=" << AddressSpaceIoUnits
                << " NumWorkers=" << NumWorkers << ")";
        }
    }

    void InitCounters() {
        if (!Counters) {
            Counters = MakeIntrusive<::NMonitoring::TDynamicCounters>();
        }
        Root = Counters->GetSubgroup("load", "actor");
        Writes.Init(Root->GetSubgroup("op", "Writes"));
        Reads.Init(Root->GetSubgroup("op", "Reads"));
    }

    static NActors::TMonotonic MonotonicNow() {
        return NActors::TActivationContext::Monotonic();
    }

    bool InMeasurementWindow() const {
        return !Draining && MonotonicNow() >= MeasurementStartTime;
    }

    void BeginDraining() {
        if (Draining) {
            return;
        }
        Draining = true;
        if (MeasurementEndTime == NActors::TMonotonic::Zero()) {
            MeasurementEndTime = MonotonicNow();
        }
    }

    ui64 PickAddress(ui32 sizeBytes) {
        Y_ABORT_UNLESS(UnitRangeCount != 0);
        ui64 unit;
        if (Config.GetSequential()) {
            if (NextSequentialIoUnit < UnitRangeStart ||
                NextSequentialIoUnit >= UnitRangeStart + UnitRangeCount) {
                // First call or out-of-range: start from the beginning of our slice.
                NextSequentialIoUnit = UnitRangeStart;
            }
            unit = NextSequentialIoUnit;
            if (++NextSequentialIoUnit >= UnitRangeStart + UnitRangeCount) {
                NextSequentialIoUnit = UnitRangeStart;
            }
        } else {
            unit = UnitRangeStart + Rng.GenRand() % UnitRangeCount;
        }
        // Sampling in I/O units guarantees the resulting flat address is
        // aligned to sizeBytes and fits entirely inside its vChunk.
        const ui32 vChunkIdx = static_cast<ui32>((unit / IoUnitsPerVChunk) % TargetNumVChunks);
        const ui32 dbgIdx    = static_cast<ui32>(unit / (static_cast<ui64>(TargetNumVChunks) * IoUnitsPerVChunk));
        const ui64 offset    = (unit % IoUnitsPerVChunk) * static_cast<ui64>(sizeBytes);
        return static_cast<ui64>(dbgIdx) * BytesPerDbg
            + static_cast<ui64>(vChunkIdx) * VChunkSizeBytes
            + offset;
    }

    // ---- StateRunning ----

    void SendNext() {
        if (Draining) {
            return;
        }

        // Interleave reads with writes to honour ReadRatio. The ratio is
        // observed against WritesIssued so the math stays simple even if
        // some replies fail.
        const ui32 readRatio = Config.GetReadRatio();
        const ui32 stopCount = Config.GetStopOnWritesDoneCount();

        while (WriteInFlight + ReadInFlight < Config.GetMaxInFlight()) {
            const bool wantRead = readRatio > 0
                && WritesIssued > 0
                && (static_cast<double>(ReadsIssued)
                        / static_cast<double>(WritesIssued))
                    < static_cast<double>(readRatio) / 100.0;
            if (wantRead) {
                if (!IssueRead()) {
                    break;
                }
            } else if (stopCount == 0 || WritesIssued < stopCount) {
                if (!IssueWrite()) {
                    break;
                }
            } else {
                break; // write cap reached; no more ops to issue
            }
        }
    }

    // Returns false if the inflight queue was full (slot occupied by stuck entry).
    bool IssueWrite() {
        const ui32 size = IoSizeBytes;
        const ui64 addr = PickAddress(size);
        NHPTimer::STime sentAt = 0;
        NHPTimer::GetTime(&sentAt);
        auto res = Inflight.Emplace(addr, size, sentAt, /*IsRead=*/false);
        if (!res) {
            LOG_D("IssueWrite queue full Tag# " << Tag << " " << res.error());
            return false;
        }
        const ui64 cookie = res.value();
        auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(addr, size);
        ev->Payload = TRope(WritePayload);
        NTabletPipe::SendData(SelfId(), PipeClient, ev.release(), cookie);
        ++WriteInFlight;
        LOG_T("IssueWrite Cookie# " << cookie << " Addr# " << addr << " Size# " << size << " WriteInFlight# " << WriteInFlight);
        ++WritesIssued;
        if (Writes.Requests) {
            Writes.Requests->Inc();
        }
        if (Writes.BytesInFlight) {
            *Writes.BytesInFlight += size;
        }
        return true;
    }

    // Returns false if the inflight queue was full (slot occupied by stuck entry).
    bool IssueRead() {
        const ui32 size = IoSizeBytes;
        const ui64 addr = PickAddress(size);
        NHPTimer::STime sentAt = 0;
        NHPTimer::GetTime(&sentAt);
        auto res = Inflight.Emplace(addr, size, sentAt, /*IsRead=*/true);
        if (!res) {
            LOG_D("IssueRead queue full Tag# " << Tag << " " << res.error());
            return false;
        }
        const ui64 cookie = res.value();
        auto ev = std::make_unique<TEvLoad::TEvNbsRead>(addr, size);
        NTabletPipe::SendData(SelfId(), PipeClient, ev.release(), cookie);
        ++ReadInFlight;
        LOG_T("IssueRead Cookie# " << cookie << " Addr# " << addr << " Size# " << size << " ReadInFlight# " << ReadInFlight);
        ++ReadsIssued;
        if (Reads.Requests) {
            Reads.Requests->Inc();
        }
        if (Reads.BytesInFlight) {
            *Reads.BytesInFlight += size;
        }
        return true;
    }

    void HandleWriteResult(TEvLoad::TEvNbsWriteResult::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        auto* entry = Inflight.Find(cookie);
        if (!entry) {
            return;
        }
        // Guard against cross-type replies. Should be unreachable with
        // monotonic cookies; if it ever fires, erase the entry and release
        // its inflight budget so the queue front can't be pinned forever.
        if (entry->IsRead) {
            LOG_E("HandleWriteResult cookie type mismatch Tag# " << Tag
                << " Cookie# " << cookie << " expected write but IsRead=true");
            Y_DEBUG_ABORT_UNLESS(false, "cookie type mismatch in HandleWriteResult");
            const ui32 sizeBytes = entry->SizeBytes;
            Inflight.Erase(cookie);
            if (ReadInFlight > 0) {
                --ReadInFlight;
            }
            if (Reads.BytesInFlight) {
                *Reads.BytesInFlight -= sizeBytes;
            }
            CheckDrainComplete();
            return;
        }
        const TInflightEntry e = *entry;
        Inflight.Erase(cookie);

        NHPTimer::STime now = 0;
        NHPTimer::GetTime(&now);
        const ui64 latencyUs = LatencyUsFromHPTimer(now - e.SentAt);
        const bool ok = ev->Get()->Record.GetStatus() == NBSIO_OK;
        const bool measure = InMeasurementWindow();
        LOG_T("HandleWriteResult Cookie# " << cookie << " Status# "
            << ENbsIoResultStatus_Name(ev->Get()->Record.GetStatus()) << " LatencyUs# "
            << latencyUs << " WriteInFlight# " << WriteInFlight);

        if (WriteInFlight > 0) {
            --WriteInFlight;
        }
        if (Writes.BytesInFlight) {
            *Writes.BytesInFlight -= e.SizeBytes;
        }
        if (ok) {
            ++WritesOk;
            WriteBytes += e.SizeBytes;
            if (Writes.ReplyOk) {
                Writes.ReplyOk->Inc();
            }
            if (Writes.Bytes) {
                *Writes.Bytes += e.SizeBytes;
            }
            if (measure) {
                ++MeasuredWritesOk;
                MeasuredWriteBytes += e.SizeBytes;
                MeasuredWriteLatencyUs.RecordValue(static_cast<i64>(latencyUs));
            }
            const ui32 stopCount = Config.GetStopOnWritesDoneCount();
            if (stopCount > 0 && WritesOk >= stopCount && !Draining) {
                LOG_N("StopOnWritesDoneCount reached Tag# " << Tag << " WritesOk# " << WritesOk);
                BeginDraining();
                Schedule(kDrainTimeout, new TEvents::TEvWakeup(kWakeupDrainTimeoutTag));
            }
        } else {
            const auto& record = ev->Get()->Record;
            LOG_D("HandleWriteResult error Tag# " << Tag
                << " Cookie# " << cookie
                << " LatencyUs# " << latencyUs
                << " Status# " << ENbsIoResultStatus_Name(record.GetStatus())
                << " Reason# " << record.GetReason());
            if (Writes.ReplyErr) {
                Writes.ReplyErr->Inc();
            }
            if (measure) {
                ++MeasuredWriteErrors;
            }
        }
        if (Writes.ResponseTimeUs) {
            Writes.ResponseTimeUs->Collect(static_cast<double>(latencyUs));
        }

        CheckDrainComplete();
        if (!ok && !Draining && !ErrorBackoffScheduled) {
            ErrorBackoffScheduled = true;
            Schedule(kErrorBackoffDuration, new TEvents::TEvWakeup(kWakeupErrorBackoffTag));
        } else {
            SendNext();
        }
    }

    void HandleReadResult(TEvLoad::TEvNbsReadResult::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        auto* entry = Inflight.Find(cookie);
        if (!entry) {
            return;
        }
        // Guard against cross-type replies. Should be unreachable with
        // monotonic cookies; if it ever fires, erase the entry and release
        // its inflight budget so the queue front can't be pinned forever.
        if (!entry->IsRead) {
            LOG_E("HandleReadResult cookie type mismatch Tag# " << Tag
                << " Cookie# " << cookie << " expected read but IsRead=false");
            Y_DEBUG_ABORT_UNLESS(false, "cookie type mismatch in HandleReadResult");
            const ui32 sizeBytes = entry->SizeBytes;
            Inflight.Erase(cookie);
            if (WriteInFlight > 0) {
                --WriteInFlight;
            }
            if (Writes.BytesInFlight) {
                *Writes.BytesInFlight -= sizeBytes;
            }
            CheckDrainComplete();
            return;
        }
        const TInflightEntry e = *entry;
        Inflight.Erase(cookie);

        NHPTimer::STime now = 0;
        NHPTimer::GetTime(&now);
        const ui64 latencyUs = LatencyUsFromHPTimer(now - e.SentAt);
        const bool ok = ev->Get()->Record.GetStatus() == NBSIO_OK;
        const bool measure = InMeasurementWindow();
        LOG_T("HandleReadResult Cookie# " << cookie << " Status# "
            << ENbsIoResultStatus_Name(ev->Get()->Record.GetStatus()) << " LatencyUs# " << latencyUs
            << " ReadInFlight# " << ReadInFlight);

        if (ReadInFlight > 0) {
            --ReadInFlight;
        }
        if (Reads.BytesInFlight) {
            *Reads.BytesInFlight -= e.SizeBytes;
        }
        if (ok) {
            ++ReadsOk;
            ReadBytes += e.SizeBytes;
            if (Reads.ReplyOk) {
                Reads.ReplyOk->Inc();
            }
            if (Reads.Bytes) {
                *Reads.Bytes += e.SizeBytes;
            }
            if (measure) {
                ++MeasuredReadsOk;
                MeasuredReadBytes += e.SizeBytes;
                MeasuredReadLatencyUs.RecordValue(static_cast<i64>(latencyUs));
            }
        } else {
            const auto& record = ev->Get()->Record;
            LOG_D("HandleReadResult error Tag# " << Tag
                << " Cookie# " << cookie
                << " LatencyUs# " << latencyUs
                << " PayloadCount# " << ev->Get()->GetPayloadCount()
                << " Status# " << ENbsIoResultStatus_Name(record.GetStatus())
                << " Reason# " << record.GetReason());
            if (Reads.ReplyErr) {
                Reads.ReplyErr->Inc();
            }
            if (measure) {
                ++MeasuredReadErrors;
            }
        }
        if (Reads.ResponseTimeUs) {
            Reads.ResponseTimeUs->Collect(static_cast<double>(latencyUs));
        }

        CheckDrainComplete();
        if (!ok && !Draining && !ErrorBackoffScheduled) {
            ErrorBackoffScheduled = true;
            Schedule(kErrorBackoffDuration, new TEvents::TEvWakeup(kWakeupErrorBackoffTag));
        } else {
            SendNext();
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr&) {
        LOG_N("Drain start Tag# " << Tag
            << " WriteInFlight# " << WriteInFlight
            << " ReadInFlight# " << ReadInFlight);
        BeginDraining();
        Schedule(kDrainTimeout, new TEvents::TEvWakeup(kWakeupDrainTimeoutTag));
        CheckDrainComplete();
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == kWakeupErrorBackoffTag) {
            ErrorBackoffScheduled = false;
            SendNext();
            return;
        }
        if (ev->Get()->Tag == kWakeupDrainTimeoutTag) {
            LOG_E("Drain timeout Tag# " << Tag
                << " WriteInFlight# " << WriteInFlight
                << " ReadInFlight# " << ReadInFlight
                << " — forcing finish");
            if (ErrorReason.empty()) {
                ErrorReason = "drain timeout";
            }
            FinishRun();
        }
    }

    void HandleRunPipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId != PipeClient) {
            return;
        }
        PipeClient = TActorId();
        LOG_E("Pipe to tablet " << TabletId << " destroyed during run Tag# " << Tag
            << " WriteInFlight# " << WriteInFlight
            << " ReadInFlight# " << ReadInFlight);
        if (ErrorReason.empty()) {
            ErrorReason = "tablet pipe lost during run";
        }
        // Enter drain: in-flight requests are orphaned, drain timeout will fire.
        if (!Draining) {
            BeginDraining();
            Schedule(kDrainTimeout, new TEvents::TEvWakeup(kWakeupDrainTimeoutTag));
            CheckDrainComplete();
        }
    }

    void CheckDrainComplete() {
        if (!Draining) {
            return;
        }
        if (WriteInFlight + ReadInFlight > 0) {
            return;
        }
        FinishRun();
    }

    void FinishRun() {
        if (Finished) {
            return;
        }
        Finished = true;

        if (PipeClient) {
            NTabletPipe::CloseClient(SelfId(), PipeClient);
            PipeClient = TActorId();
        }

        const NActors::TMonotonic now = MonotonicNow();
        const ui64 durationMs = (TestStartTime != NActors::TMonotonic::Zero() && now > TestStartTime)
            ? (now - TestStartTime).MilliSeconds() : 0;
        const ui64 measuredMs = (MeasurementEndTime != NActors::TMonotonic::Zero()
                                  && MeasurementEndTime > MeasurementStartTime)
            ? (MeasurementEndTime - MeasurementStartTime).MilliSeconds() : 0;

        // Populate typed WorkerStats: measured fields feed the service-actor
        // aggregation, *Total fields feed the run summary.
        TNbsDbgLikeFinishStats stats;
        stats.WritesIssued     = WritesIssued;
        stats.WritesOk         = MeasuredWritesOk;
        stats.WritesErr        = MeasuredWriteErrors;
        stats.WriteBytes       = MeasuredWriteBytes;
        stats.ReadsPbOk        = MeasuredReadsOk;   // load actor measures all reads together
        stats.ReadsPbBytes     = MeasuredReadBytes;
        stats.ReadsErr         = MeasuredReadErrors;
        stats.ReadsDDiskOk     = 0;
        stats.WritesOkTotal    = WritesOk;
        stats.WriteBytesTotal  = WriteBytes;
        stats.ReadsIssuedTotal = ReadsIssued;
        stats.ReadsOkTotal     = ReadsOk;
        stats.ReadBytesTotal   = ReadBytes;
        stats.RunningMs        = durationMs;
        stats.MeasuredMs       = measuredMs;
        stats.MaxInFlight      = Config.GetMaxInFlight();
        stats.WriteE2eUs       = std::move(MeasuredWriteLatencyUs);
        stats.ReadPbUs         = std::move(MeasuredReadLatencyUs);

        auto* finishEv = BuildNbsDbgLikeFinishEvent(Tag, TabletId, ErrorReason, std::move(stats));

        Send(Parent, finishEv);

        if (Root) {
            Root->ResetCounters();
        }
        PassAway();
    }

    void HandlePipeConnectedRunning(TEvTabletPipe::TEvClientConnected::TPtr&) {
        // Reconnect during run — no action needed; the pipe client retried for us.
    }

    STRICT_STFUNC(StateInit,
        hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnectedInit)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleInitPipeDestroyed)
        hFunc(TEvents::TEvPoisonPill, HandleInitPoison)
        hFunc(TEvents::TEvWakeup, HandleInitWakeup)
    )

    STRICT_STFUNC(StateRunning,
        hFunc(TEvLoad::TEvNbsWriteResult, HandleWriteResult)
        hFunc(TEvLoad::TEvNbsReadResult, HandleReadResult)
        hFunc(TEvents::TEvPoisonPill, HandlePoison)
        hFunc(TEvents::TEvWakeup, HandleWakeup)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleRunPipeDestroyed)
        hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnectedRunning)
    )

private:
    struct TOpCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr Requests;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReplyOk;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReplyErr;
        ::NMonitoring::TDynamicCounters::TCounterPtr Bytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr BytesInFlight;
        ::NMonitoring::THistogramPtr ResponseTimeUs;

        void Init(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& g) {
            Requests       = g->GetCounter("Requests", true);
            ReplyOk        = g->GetCounter("ReplyOk", true);
            ReplyErr       = g->GetCounter("ReplyErr", true);
            Bytes          = g->GetCounter("Bytes", true);
            BytesInFlight  = g->GetCounter("BytesInFlight", false);
            ResponseTimeUs = g->GetHistogram(
                "ResponseTimeUs",
                NMonitoring::ExplicitHistogram(LoadActorResponseTimeUsBounds()));
        }
    };

    const TActorId Parent;
    const ui64 Tag;
    const ui64 TabletId;
    TEvLoadTestRequest::TNbsDbgLikeLoad::TWorkloadConfig Config;

    const ui32 WorkerIndex;
    const ui32 NumWorkers;

    // Set from TResolvedTabletInfo; validated by the proxy before worker creation.
    ui32 EffectiveDbgCount = 0;
    ui64 VChunkSizeBytes = 0;
    ui32 TargetNumVChunks = 0;
    ui32 IoSizeBytes = 0;

    // Assigned slice of the flat io-unit space; computed in ComputeWorkerSlice().
    ui64 UnitRangeStart = 0;
    ui64 UnitRangeCount = 0;

    ui64 BytesPerDbg = 0;
    ui64 AddressSpaceBytes = 0;
    ui64 IoUnitsPerVChunk = 0;
    ui64 AddressSpaceIoUnits = 0;
    ui64 NextSequentialIoUnit = 0;

    TFastRng64 Rng;
    TRope WritePayload;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Root;
    TOpCounters Writes;
    TOpCounters Reads;

    TActorId PipeClient;

    TCircularSparseQueue<TInflightEntry> Inflight;

    ui32 WriteInFlight = 0;
    ui32 ReadInFlight  = 0;

    ui64 WritesIssued = 0;
    ui64 WritesOk     = 0;
    ui64 WriteBytes   = 0;
    ui64 ReadsIssued  = 0;
    ui64 ReadsOk      = 0;
    ui64 ReadBytes    = 0;

    ui64 MeasuredWritesOk = 0;
    ui64 MeasuredWriteBytes = 0;
    ui64 MeasuredReadsOk = 0;
    ui64 MeasuredReadBytes = 0;
    ui64 MeasuredWriteErrors = 0;
    ui64 MeasuredReadErrors = 0;

    NHdr::THistogram MeasuredWriteLatencyUs;
    NHdr::THistogram MeasuredReadLatencyUs;

    NActors::TMonotonic TestStartTime;
    NActors::TMonotonic MeasurementStartTime;
    NActors::TMonotonic MeasurementEndTime;

    bool Draining = false;
    bool Finished = false;
    bool ErrorBackoffScheduled = false;
    TString ErrorReason;
};

// TNbsDbgLikeLoadActorProxy fans a single NbsDbgLike run out to several
// TNbsDbgLikeLoadActor workers (each with a share of the in-flight and
// StopOnWritesDoneCount budget), then merges their per-worker results
// into a single TEvLoadTestFinished — exactly the event the service actor
// (which registers one actor per Tag) expects.
//
// The proxy is responsible for the init sequence:
//   1. Open pipe → send GetSummary → validate → send single ConfigureTablet
//   2. Spawn workers (passing resolved info); switch to StateWork
//   3. Merge worker results; close pipe; emit combined TEvLoadTestFinished
class TNbsDbgLikeLoadActorProxy : public TActorBootstrapped<TNbsDbgLikeLoadActorProxy> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE;
    }

    TNbsDbgLikeLoadActorProxy(
        const TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
        const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        ui64 tag)
        : Cmd(cmd)
        , Parent(parent)
        , Tag(tag)
        , TabletId(cmd.GetNbsDbgLikeTabletId())
        , Counters(std::move(counters))
    {}

    void Bootstrap() {
        if (TabletId == 0) {
            EmitError("NbsDbgLikeTabletId is not set");
            return;
        }

        const auto& config = Cmd.GetWorkloadConfig();
        TotalMaxInFlight = config.GetMaxInFlight();

        LOG_I("Proxy bootstrap Tag# " << Tag
            << " TabletId# " << TabletId
            << " TotalMaxInFlight# " << TotalMaxInFlight);

        NTabletPipe::TClientConfig pipeConfig{
            .RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}
        };
        ProxyPipeClient = Register(NTabletPipe::CreateClient(SelfId(), TabletId, pipeConfig));

        auto req = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummary>();
        NTabletPipe::SendData(SelfId(), ProxyPipeClient, req.release());

        Schedule(kInitTimeout, new TEvents::TEvWakeup(kWakeupInitTimeoutTag));
        Become(&TNbsDbgLikeLoadActorProxy::StateInit);
    }

private:
    static ui32 CalcNumWorkers(ui32 totalMaxInFlight, bool sequential) {
        if (sequential) {
            return 1;
        }
        // At least one worker even when MaxInFlight is 0.
        return Max<ui32>(
            1,
            (totalMaxInFlight + kMaxInflightPerActor - 1) / kMaxInflightPerActor);
    }

    // ---- StateInit ----

    void HandleProxySummaryResult(TEvLoad::TEvNbsLoadTabletGetSummaryResult::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NBSLT_OK) {
            EmitError(TStringBuilder()
                << "GetSummary failed: status=" << rec.GetStatus()
                << " reason=" << rec.GetErrorReason());
            return;
        }

        const ui32 totalDbgs = rec.GetNumDirectBlockGroups();
        const ui32 readyDbgs = rec.GetNumReadyDirectBlockGroups();
        const ui64 vChunkSizeBytes = rec.GetVChunkSizeBytes();
        const ui32 targetNumVChunks = rec.GetTargetNumVChunks();

        // Use the ready count (longest contiguous prefix of DBGs with peers
        // connected) as the base. Fall back to total count for backward
        // compatibility with tablets that don't set the field yet.
        ui32 effectiveDbgCount = (readyDbgs > 0) ? readyDbgs : totalDbgs;

        const auto& config = Cmd.GetWorkloadConfig();
        const ui32 mDbg = config.GetNumDirectBlockGroupsToUse();
        if (mDbg > 0 && mDbg < effectiveDbgCount) {
            effectiveDbgCount = mDbg;
        }

        // Validate everything the workers would have validated.
        if (effectiveDbgCount == 0) {
            EmitError("EffectiveDbgCount=0 (NumDirectBlockGroups from tablet)");
            return;
        }
        if (targetNumVChunks == 0) {
            EmitError("TargetNumVChunks=0 (from tablet GetSummary)");
            return;
        }
        if (vChunkSizeBytes == 0 || vChunkSizeBytes % kSectorSize != 0) {
            EmitError(TStringBuilder()
                << "VChunkSizeBytes (" << vChunkSizeBytes
                << ") must be a positive multiple of " << kSectorSize);
            return;
        }
        const ui32 kib = config.GetReadWriteSizeKiB();
        if (kib < 4 || (kib % 4) != 0) {
            EmitError(TStringBuilder()
                << "ReadWriteSizeKiB (" << kib << " KiB) must be >= 4 and a multiple of 4");
            return;
        }
        const ui64 ioBytes = static_cast<ui64>(kib) * 1024;
        if (ioBytes > vChunkSizeBytes) {
            EmitError(TStringBuilder()
                << "ReadWriteSizeKiB * 1024 (" << ioBytes
                << ") exceeds VChunkSizeBytes (" << vChunkSizeBytes << ")");
            return;
        }
        // The tablet's ComputeRoutingParams marks IoValid=false unless
        // IoSizeBytes evenly divides VChunkSizeBytes; fail fast here instead
        // of letting every worker get NBSIO_NOT_CONFIGURED.
        if (vChunkSizeBytes % ioBytes != 0) {
            EmitError(TStringBuilder()
                << "IoSizeBytes (" << ioBytes
                << ") must evenly divide VChunkSizeBytes (" << vChunkSizeBytes << ")");
            return;
        }
        if (ioBytes > Max<ui32>()) {
            EmitError("ReadWriteSizeKiB * 1024 overflows ui32");
            return;
        }
        if (config.GetDurationSeconds() == 0 && config.GetStopOnWritesDoneCount() == 0) {
            EmitError("at least one of DurationSeconds or StopOnWritesDoneCount must be non-zero");
            return;
        }
        if (config.GetDurationSeconds() > 0
            && config.GetDelayBeforeMeasurementsSeconds() >= config.GetDurationSeconds())
        {
            EmitError(TStringBuilder()
                << "DelayBeforeMeasurementsSeconds (" << config.GetDelayBeforeMeasurementsSeconds()
                << ") must be < DurationSeconds (" << config.GetDurationSeconds() << ")");
            return;
        }
        if (config.GetTabletConfig().GetDisableReplication() && config.GetReadRatio() > 0) {
            EmitError("DisableReplication is incompatible with ReadRatio > 0 (reads require flush)");
            return;
        }

        const ui32 ioSizeBytes = static_cast<ui32>(ioBytes);
        const ui32 totalStopCount = config.GetStopOnWritesDoneCount();

        // Determine worker count.
        ui32 numWorkers = CalcNumWorkers(TotalMaxInFlight, config.GetSequential());
        if (totalStopCount > 0) {
            numWorkers = Min(numWorkers, totalStopCount);
        }
        // Each worker needs a non-empty slice of the flat io-unit space;
        // otherwise it fails the run with "Degenerate address space slice".
        // Cap by the total number of io-units (>= 1: all factors validated
        // non-zero above, and ioBytes divides vChunkSizeBytes).
        const ui64 addressSpaceIoUnits = static_cast<ui64>(effectiveDbgCount)
            * static_cast<ui64>(targetNumVChunks) * (vChunkSizeBytes / ioBytes);
        numWorkers = static_cast<ui32>(Min<ui64>(numWorkers, addressSpaceIoUnits));

        LOG_N("Proxy GetSummary OK Tag# " << Tag
            << " TotalDbgCount# " << totalDbgs
            << " ReadyDbgCount# " << readyDbgs
            << " EffectiveDbgCount# " << effectiveDbgCount
            << " VChunkSizeBytes# " << vChunkSizeBytes
            << " TargetNumVChunks# " << targetNumVChunks
            << " IoSizeBytes# " << ioSizeBytes
            << " NumWorkers# " << numWorkers
            << " TotalMaxInFlight# " << TotalMaxInFlight
            << " TotalStopOnWritesDoneCount# " << totalStopCount);

        // Send a single ConfigureTablet. The proxy's pipe stays open until
        // PassAway() to ensure delivery.
        {
            auto cfg = std::make_unique<TEvLoad::TEvConfigureTablet>();
            cfg->Record = config.GetTabletConfig();
            cfg->Record.SetNumDirectBlockGroupsToUse(effectiveDbgCount);
            cfg->Record.SetIoSizeBytes(ioSizeBytes);
            NTabletPipe::SendData(SelfId(), ProxyPipeClient, cfg.release());
        }

        // Spawn workers and switch to StateWork.
        const TResolvedTabletInfo resolved{effectiveDbgCount, vChunkSizeBytes,
                                           targetNumVChunks, ioSizeBytes};
        for (ui32 i = 0; i < numWorkers; ++i) {
            auto workerCmd = Cmd;
            auto& wc = *workerCmd.MutableWorkloadConfig();
            wc.SetMaxInFlight(static_cast<ui32>(SplitShare(TotalMaxInFlight, numWorkers, i)));
            if (totalStopCount > 0) {
                wc.SetStopOnWritesDoneCount(
                    static_cast<ui32>(SplitShare(totalStopCount, numWorkers, i)));
            }

            TIntrusivePtr<::NMonitoring::TDynamicCounters> workerCounters = Counters
                ? Counters->GetSubgroup("worker", ToString(i))
                : nullptr;

            // Give each worker a distinct tag so their RNG seeds diverge
            // (the worker seeds with TInstant::Now() ^ tag, and a tight
            // registration loop can observe the same Now()) and their log
            // lines are distinguishable. The proxy tracks workers by actor
            // id and rebuilds the finish event with the original Tag, so the
            // service actor only ever sees the real tag.
            const ui64 workerTag = Tag * 1000 + i;
            const TActorId workerId = Register(
                new TNbsDbgLikeLoadActor(
                    workerCmd, SelfId(), workerCounters,
                    workerTag, i, numWorkers, resolved));
            Workers.insert(workerId);
        }

        Become(&TNbsDbgLikeLoadActorProxy::StateWork);
    }

    void HandleInitPipeConnected(TEvTabletPipe::TEvClientConnected::TPtr&) {
        // No-op: we wait for the GetSummary reply, not for the connection event.
    }

    void HandleInitPipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId != ProxyPipeClient) {
            return;
        }
        ProxyPipeClient = TActorId();
        EmitError("proxy pipe lost before GetSummary reply");
    }

    void HandleInitPoison(TEvents::TEvPoisonPill::TPtr&) {
        EmitError("stopped before start");
    }

    void HandleInitWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == kWakeupInitTimeoutTag) {
            LOG_N("Proxy GetSummary timeout after " << kInitTimeout
                << " Tag# " << Tag
                << " TabletId# " << TabletId);
            EmitError(TStringBuilder()
                << "GetSummary timed out after " << kInitTimeout
                << " (TabletId " << TabletId << ")");
        }
    }

    // ---- StateWork ----

    void HandleWorkerFinished(TEvLoad::TEvLoadTestFinished::TPtr& ev) {
        const TActorId worker = ev->Sender;
        if (!Workers.erase(worker)) {
            return;
        }

        if (!ev->Get()->ErrorReason.empty()) {
            if (!CombinedError.empty()) {
                CombinedError += "; ";
            }
            CombinedError += ev->Get()->ErrorReason;
        }

        if (auto& ws = ev->Get()->WorkerStats) {
            if (auto* s = std::get_if<TNbsDbgLikeFinishStats>(&*ws)) {
                MergeStats(*s);
            }
        }

        LOG_I("Proxy worker finished Tag# " << Tag
            << " RemainingWorkers# " << Workers.size());

        if (Workers.empty()) {
            EmitCombined();
        }
    }

    void HandleWorkPipeDestroyed(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) {
        if (ev->Get()->ClientId != ProxyPipeClient) {
            return;
        }
        ProxyPipeClient = TActorId();
        // The proxy pipe dying during the run is benign: ConfigureTablet was
        // already delivered. Workers have their own pipes and self-manage.
        LOG_D("Proxy pipe to tablet " << TabletId << " destroyed during run Tag# " << Tag);
    }

    void HandleWorkPipeConnected(TEvTabletPipe::TEvClientConnected::TPtr&) {
        // No-op.
    }

    void HandleWorkWakeup(TEvents::TEvWakeup::TPtr& ev) {
        // The init-timeout wakeup scheduled in Bootstrap() is not cancelled
        // on the success path and fires here; ignore it.
        if (ev->Get()->Tag != kWakeupInitTimeoutTag) {
            LOG_D("Proxy unexpected wakeup Tag# " << Tag
                << " WakeupTag# " << ev->Get()->Tag);
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr&) {
        LOG_I("Proxy poison Tag# " << Tag
            << " forwarding to " << Workers.size() << " worker(s)");
        for (const TActorId& worker : Workers) {
            Send(worker, new TEvents::TEvPoisonPill);
        }
        // Keep the proxy alive; workers will send TEvLoadTestFinished back,
        // and HandleWorkerFinished will call EmitCombined when all are done.
    }

    void MergeStats(TNbsDbgLikeFinishStats& s) {
        if (!HasMerged) {
            HasMerged = true;
            Merged = std::move(s);
            return;
        }
        // The fields summed below are the only ones the load actor populates.
        // BytesAccounted, LsnsCompleted, LsnsFailed, ReadsPbIssued,
        // ReadsDDiskIssued and ReadsDDiskBytes are set only by the tablet-side
        // actor and are always 0 here, so they keep the first worker's value
        // (0) rather than being summed. Add them here if a load worker ever
        // starts producing them.
        Merged.WritesIssued     += s.WritesIssued;
        Merged.WritesOk         += s.WritesOk;
        Merged.WritesErr        += s.WritesErr;
        Merged.WriteBytes       += s.WriteBytes;
        Merged.ReadsPbOk        += s.ReadsPbOk;
        Merged.ReadsPbBytes     += s.ReadsPbBytes;
        Merged.ReadsErr         += s.ReadsErr;
        Merged.ReadsDDiskOk     += s.ReadsDDiskOk;
        Merged.WritesOkTotal    += s.WritesOkTotal;
        Merged.WriteBytesTotal  += s.WriteBytesTotal;
        Merged.ReadsIssuedTotal += s.ReadsIssuedTotal;
        Merged.ReadsOkTotal     += s.ReadsOkTotal;
        Merged.ReadBytesTotal   += s.ReadBytesTotal;
        Merged.RunningMs         = Max(Merged.RunningMs, s.RunningMs);
        Merged.MeasuredMs        = Max(Merged.MeasuredMs, s.MeasuredMs);
        Merged.WriteE2eUs.Add(s.WriteE2eUs);
        Merged.ReadPbUs.Add(s.ReadPbUs);
    }

    void EmitCombined() {
        if (ProxyPipeClient) {
            NTabletPipe::CloseClient(SelfId(), ProxyPipeClient);
            ProxyPipeClient = TActorId();
        }
        Merged.MaxInFlight = TotalMaxInFlight;
        auto* finishEv = BuildNbsDbgLikeFinishEvent(
            Tag, TabletId, CombinedError, std::move(Merged));
        Send(Parent, finishEv);
        PassAway();
    }

    void EmitError(TString reason) {
        LOG_N("Proxy error Tag# " << Tag << " " << reason);
        if (ProxyPipeClient) {
            NTabletPipe::CloseClient(SelfId(), ProxyPipeClient);
            ProxyPipeClient = TActorId();
        }
        TNbsDbgLikeFinishStats stats;
        stats.MaxInFlight = TotalMaxInFlight;
        auto* finishEv = BuildNbsDbgLikeFinishEvent(Tag, TabletId, reason, std::move(stats));
        Send(Parent, finishEv);
        PassAway();
    }

    STRICT_STFUNC(StateInit,
        hFunc(TEvLoad::TEvNbsLoadTabletGetSummaryResult, HandleProxySummaryResult)
        hFunc(TEvTabletPipe::TEvClientConnected, HandleInitPipeConnected)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleInitPipeDestroyed)
        hFunc(TEvents::TEvPoisonPill, HandleInitPoison)
        hFunc(TEvents::TEvWakeup, HandleInitWakeup)
    )

    STRICT_STFUNC(StateWork,
        hFunc(TEvLoad::TEvLoadTestFinished, HandleWorkerFinished)
        hFunc(TEvTabletPipe::TEvClientConnected, HandleWorkPipeConnected)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleWorkPipeDestroyed)
        hFunc(TEvents::TEvPoisonPill, HandlePoison)
        hFunc(TEvents::TEvWakeup, HandleWorkWakeup)
    )

private:
    const TEvLoadTestRequest::TNbsDbgLikeLoad Cmd;
    const TActorId Parent;
    const ui64 Tag;
    const ui64 TabletId;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TActorId ProxyPipeClient;
    ui32 TotalMaxInFlight = 0;
    THashSet<TActorId> Workers;

    bool HasMerged = false;
    TNbsDbgLikeFinishStats Merged;
    TString CombinedError;
};

// Reconstruct a TNbsDbgLikeFinishStats from the wire stats carried by a remote
// child's TEvNodeFinishResponse (counters + serialized latency histograms).
TNbsDbgLikeFinishStats StatsFromNodeProto(
    const NKikimr::TEvNodeFinishResponse::TNbsDbgLikeNodeStats& ns)
{
    TNbsDbgLikeFinishStats s;
    s.WritesOk     = ns.GetWritesOk();
    s.WriteBytes   = ns.GetWriteBytes();
    s.WritesErr    = ns.GetWritesErr();
    s.ReadsPbOk    = ns.GetReadsOk();
    s.ReadsPbBytes = ns.GetReadBytes();
    s.ReadsErr     = ns.GetReadsErr();
    s.MeasuredMs   = ns.GetMeasuredMs();
    s.MaxInFlight  = ns.GetMaxInFlight();
    if (ns.HasWriteLatencyUs()) {
        s.WriteE2eUs = DeserializeNbsDbgLikeHistogram(ns.GetWriteLatencyUs());
    }
    if (ns.HasReadLatencyUs()) {
        s.ReadPbUs = DeserializeNbsDbgLikeHistogram(ns.GetReadLatencyUs());
    }
    return s;
}

// Render the per-tablet metrics object used by the front-end (same field names
// the service actor enrichment produces for a single run).
NJson::TJsonValue PerTabletJson(
    ui64 tabletId, ui32 nodeId,
    const TNbsDbgLikeFinishStats& s, const TString& errorReason)
{
    const double sec = s.MeasuredMs > 0 ? s.MeasuredMs / 1000.0 : 1.0;
    NJson::TJsonValue jr;
    jr["tablet_id"]     = tabletId;
    jr["node_id"]       = nodeId;
    jr["write_rps"]     = s.WritesOk / sec;
    jr["write_p50"]     = static_cast<double>(s.WriteE2eUs.GetValueAtPercentile(50.0));
    jr["write_p95"]     = static_cast<double>(s.WriteE2eUs.GetValueAtPercentile(95.0));
    jr["write_p99"]     = static_cast<double>(s.WriteE2eUs.GetValueAtPercentile(99.0));
    jr["read_rps"]      = (s.ReadsPbOk + s.ReadsDDiskOk) / sec;
    jr["read_p50"]      = static_cast<double>(s.ReadPbUs.GetValueAtPercentile(50.0));
    jr["read_p95"]      = static_cast<double>(s.ReadPbUs.GetValueAtPercentile(95.0));
    jr["read_p99"]      = static_cast<double>(s.ReadPbUs.GetValueAtPercentile(99.0));
    jr["max_in_flight"] = static_cast<ui64>(s.MaxInFlight);
    if (!errorReason.empty()) {
        jr["error"] = errorReason;
    }
    return jr;
}

// TNbsDbgLikeMultiLoadActor fans a single multi-tablet run out to one
// single-tablet child run per target tablet, placing each child's load actor on
// the node that hosts the tablet (Targets[i].NodeId). Children on the local node
// are registered directly (full in-process stats); remote children are started
// by sending TEvLoadTestRequest to that node's load service, which replies with
// TEvNodeFinishResponse carrying serialized latency histograms. Results are
// merged into an exact combined TNbsDbgLikeFinishStats (so the service actor's
// existing enrichment fills the combined top-level metrics) plus a per-tablet
// breakdown emitted as JsonResult["tablets"].
class TNbsDbgLikeMultiLoadActor : public TActorBootstrapped<TNbsDbgLikeMultiLoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE;
    }

    TNbsDbgLikeMultiLoadActor(
        const TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
        const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        ui64 tag)
        : Cmd(cmd)
        , Parent(parent)
        , Tag(tag)
        , Counters(std::move(counters))
    {}

    void Bootstrap() {
        if (Cmd.TargetsSize() == 0) {
            EmitError("multi-tablet run has no targets");
            return;
        }

        LOG_I("Multi bootstrap Tag# " << Tag << " Targets# " << Cmd.TargetsSize());

        ui32 maxDurationSeconds = 0;
        const int targetsCount = static_cast<int>(Cmd.TargetsSize());
        for (int i = 0; i < targetsCount; ++i) {
            const auto& target = Cmd.GetTargets(i);
            TChild child;
            child.TabletId = target.GetTabletId();
            child.NodeId = target.GetNodeId();

            auto childCmd = Cmd;
            childCmd.ClearTargets();
            childCmd.SetNbsDbgLikeTabletId(child.TabletId);
            const ui64 childTag = Tag * 1000 + i;
            childCmd.SetTag(childTag);
            childCmd.MutableWorkloadConfig()->SetTag(childTag);

            maxDurationSeconds = Max(maxDurationSeconds,
                childCmd.GetWorkloadConfig().GetDurationSeconds());

            const ui32 selfNode = SelfId().NodeId();
            if (child.NodeId == 0 || child.NodeId == selfNode) {
                child.NodeId = selfNode;
                TIntrusivePtr<::NMonitoring::TDynamicCounters> childCounters = Counters
                    ? Counters->GetSubgroup("tablet", ToString(i))
                    : nullptr;
                child.ActorId = Register(
                    CreateNbsDbgLikeLoadActor(childCmd, SelfId(), childCounters, childTag));
                LOG_D("Multi local child Tag# " << Tag
                    << " TabletId# " << child.TabletId
                    << " ChildTag# " << childTag
                    << " Actor# " << child.ActorId);
            } else {
                child.Uuid = CreateGuidAsString();
                auto req = std::make_unique<TEvLoad::TEvLoadTestRequest>();
                auto& rec = req->Record;
                *rec.MutableNbsDbgLikeLoad() = childCmd;
                rec.SetTag(childTag);
                rec.SetUuid(child.Uuid);
                rec.SetTimestamp(TInstant::Now().Seconds());
                rec.SetCookie(child.NodeId);
                Send(MakeLoadServiceID(child.NodeId), req.release());
                LOG_D("Multi remote child Tag# " << Tag
                    << " TabletId# " << child.TabletId
                    << " NodeId# " << child.NodeId
                    << " ChildTag# " << childTag
                    << " Uuid# " << child.Uuid);
            }

            Children.push_back(std::move(child));
        }

        Pending = Children.size();

        // Safety timeout: children stop on DurationSeconds; allow generous slack
        // (warmup + drain) before forcing a finish with whatever has arrived.
        const TDuration timeout = TDuration::Seconds(maxDurationSeconds)
            + kInitTimeout + kDrainTimeout + TDuration::Seconds(60);
        Schedule(timeout, new TEvents::TEvWakeup(kWakeupDrainTimeoutTag));

        Become(&TNbsDbgLikeMultiLoadActor::StateWork);
    }

private:
    struct TChild {
        ui64 TabletId = 0;
        ui32 NodeId = 0;
        TActorId ActorId;      // set for local children
        TString Uuid;          // set for remote children
        bool Finished = false;
        TString ErrorReason;
        bool HasStats = false;
        TNbsDbgLikeFinishStats Stats;
    };

    void HandleLocalFinished(TEvLoad::TEvLoadTestFinished::TPtr& ev) {
        TChild* child = nullptr;
        for (auto& c : Children) {
            if (!c.Finished && c.ActorId && c.ActorId == ev->Sender) {
                child = &c;
                break;
            }
        }
        if (!child) {
            LOG_E("Multi unexpected local finish Tag# " << Tag
                << " Sender# " << ev->Sender);
            return;
        }
        child->ErrorReason = ev->Get()->ErrorReason;
        if (const auto* s = GetNbsDbgLikeFinishStats(*ev->Get())) {
            child->Stats = std::move(const_cast<TNbsDbgLikeFinishStats&>(*s));
            child->HasStats = true;
        }
        FinishChild(*child);
    }

    void HandleRemoteFinished(TEvLoad::TEvNodeFinishResponse::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        TChild* child = nullptr;
        for (auto& c : Children) {
            if (!c.Finished && !c.Uuid.empty() && c.Uuid == rec.GetUuid()) {
                child = &c;
                break;
            }
        }
        if (!child) {
            LOG_E("Multi unexpected remote finish Tag# " << Tag
                << " Uuid# " << rec.GetUuid());
            return;
        }
        child->ErrorReason = rec.GetErrorReason();
        if (rec.HasNbsDbgLikeStats()) {
            child->Stats = StatsFromNodeProto(rec.GetNbsDbgLikeStats());
            child->HasStats = true;
        }
        FinishChild(*child);
    }

    void HandleLoadTestResponse(TEvLoad::TEvLoadTestResponse::TPtr& ev) {
        // Ack from a remote load service that it accepted (or rejected) the run.
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            LOG_E("Multi remote rejected child Tag# " << Tag
                << " Cookie# " << rec.GetCookie()
                << " Reason# " << rec.GetErrorReason());
            // Mark the matching unstarted remote child (by node id == cookie) as
            // failed so the run can still complete.
            const ui32 nodeId = static_cast<ui32>(rec.GetCookie());
            for (auto& c : Children) {
                if (!c.Finished && !c.Uuid.empty() && c.NodeId == nodeId && !c.HasStats) {
                    c.ErrorReason = rec.HasErrorReason()
                        ? rec.GetErrorReason()
                        : TString("remote node rejected run");
                    FinishChild(c);
                    break;
                }
            }
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr&) {
        LOG_I("Multi poison Tag# " << Tag << " forwarding to children");
        for (size_t i = 0; i < Children.size(); ++i) {
            const TChild& c = Children[i];
            if (c.Finished) {
                continue;
            }
            if (c.ActorId) {
                Send(c.ActorId, new TEvents::TEvPoisonPill);
            } else if (!c.Uuid.empty()) {
                // Ask the remote node's load service to stop this child run by
                // its child tag. Remote children also self-terminate on
                // DurationSeconds, and the safety timeout backstops either way.
                auto req = std::make_unique<TEvLoad::TEvLoadTestRequest>();
                auto& rec = req->Record;
                // Stop.Tag selects the child load actor to kill on the remote
                // node; the request's own Tag must be distinct from the child
                // tag (still in the remote's RequestSender map) to pass its
                // duplicate-tag guard.
                rec.MutableStop()->SetTag(Tag * 1000 + i);
                rec.SetTag(Tag * 1000000 + i + 1);
                rec.SetUuid(CreateGuidAsString());
                rec.SetTimestamp(TInstant::Now().Seconds());
                Send(MakeLoadServiceID(c.NodeId), req.release());
            }
        }
        // Children will report their results; EmitCombined fires when all done.
    }

    void HandleWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag != kWakeupDrainTimeoutTag) {
            return;
        }
        LOG_E("Multi timeout Tag# " << Tag << " Pending# " << Pending
            << " — forcing finish");
        for (auto& c : Children) {
            if (!c.Finished) {
                if (c.ErrorReason.empty()) {
                    c.ErrorReason = "child run timed out";
                }
                c.Finished = true;
                if (Pending > 0) {
                    --Pending;
                }
            }
        }
        EmitCombined();
    }

    void FinishChild(TChild& child) {
        if (child.Finished) {
            return;
        }
        child.Finished = true;
        if (Pending > 0) {
            --Pending;
        }
        LOG_I("Multi child finished Tag# " << Tag
            << " TabletId# " << child.TabletId
            << " Remaining# " << Pending
            << " Status# " << (child.ErrorReason.empty() ? "OK" : child.ErrorReason));
        if (Pending == 0) {
            EmitCombined();
        }
    }

    void MergeStats(const TNbsDbgLikeFinishStats& s) {
        if (!HasMerged) {
            HasMerged = true;
            Merged.WritesIssued     = s.WritesIssued;
            Merged.WritesOk         = s.WritesOk;
            Merged.WritesErr        = s.WritesErr;
            Merged.WriteBytes       = s.WriteBytes;
            Merged.ReadsPbOk        = s.ReadsPbOk;
            Merged.ReadsPbBytes     = s.ReadsPbBytes;
            Merged.ReadsErr         = s.ReadsErr;
            Merged.ReadsDDiskOk     = s.ReadsDDiskOk;
            Merged.WritesOkTotal    = s.WritesOkTotal;
            Merged.WriteBytesTotal  = s.WriteBytesTotal;
            Merged.ReadsIssuedTotal = s.ReadsIssuedTotal;
            Merged.ReadsOkTotal     = s.ReadsOkTotal;
            Merged.ReadBytesTotal   = s.ReadBytesTotal;
            Merged.RunningMs        = s.RunningMs;
            Merged.MeasuredMs       = s.MeasuredMs;
            Merged.MaxInFlight      = s.MaxInFlight;
            Merged.WriteE2eUs.Add(s.WriteE2eUs);
            Merged.ReadPbUs.Add(s.ReadPbUs);
            return;
        }
        Merged.WritesIssued     += s.WritesIssued;
        Merged.WritesOk         += s.WritesOk;
        Merged.WritesErr        += s.WritesErr;
        Merged.WriteBytes       += s.WriteBytes;
        Merged.ReadsPbOk        += s.ReadsPbOk;
        Merged.ReadsPbBytes     += s.ReadsPbBytes;
        Merged.ReadsErr         += s.ReadsErr;
        Merged.ReadsDDiskOk     += s.ReadsDDiskOk;
        Merged.WritesOkTotal    += s.WritesOkTotal;
        Merged.WriteBytesTotal  += s.WriteBytesTotal;
        Merged.ReadsIssuedTotal += s.ReadsIssuedTotal;
        Merged.ReadsOkTotal     += s.ReadsOkTotal;
        Merged.ReadBytesTotal   += s.ReadBytesTotal;
        Merged.RunningMs         = Max(Merged.RunningMs, s.RunningMs);
        Merged.MeasuredMs        = Max(Merged.MeasuredMs, s.MeasuredMs);
        Merged.MaxInFlight      += s.MaxInFlight;
        Merged.WriteE2eUs.Add(s.WriteE2eUs);
        Merged.ReadPbUs.Add(s.ReadPbUs);
    }

    void EmitCombined() {
        if (Emitted) {
            return;
        }
        Emitted = true;

        NJson::TJsonValue tablets(NJson::JSON_ARRAY);
        TString combinedError;
        const TNbsDbgLikeFinishStats emptyStats;
        for (auto& c : Children) {
            if (!c.ErrorReason.empty()) {
                if (!combinedError.empty()) {
                    combinedError += "; ";
                }
                combinedError += TStringBuilder()
                    << "tablet " << c.TabletId << ": " << c.ErrorReason;
            }
            if (c.HasStats) {
                MergeStats(c.Stats);
            }
            const TNbsDbgLikeFinishStats& statsRef = c.HasStats ? c.Stats : emptyStats;
            tablets.AppendValue(PerTabletJson(
                c.TabletId, c.NodeId, statsRef, c.ErrorReason));
        }

        auto* finishEv = BuildNbsDbgLikeFinishEvent(
            Tag, /*tabletId=*/0, combinedError, std::move(Merged));
        finishEv->JsonResult["tablets"] = std::move(tablets);
        Send(Parent, finishEv);
        PassAway();
    }

    void EmitError(TString reason) {
        if (Emitted) {
            return;
        }
        Emitted = true;
        LOG_N("Multi error Tag# " << Tag << " " << reason);
        TNbsDbgLikeFinishStats stats;
        auto* finishEv = BuildNbsDbgLikeFinishEvent(Tag, /*tabletId=*/0, reason, std::move(stats));
        finishEv->JsonResult["tablets"] = NJson::TJsonValue(NJson::JSON_ARRAY);
        Send(Parent, finishEv);
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvLoad::TEvLoadTestFinished, HandleLocalFinished)
        hFunc(TEvLoad::TEvNodeFinishResponse, HandleRemoteFinished)
        hFunc(TEvLoad::TEvLoadTestResponse, HandleLoadTestResponse)
        hFunc(TEvents::TEvPoisonPill, HandlePoison)
        hFunc(TEvents::TEvWakeup, HandleWakeup)
    )

private:
    const TEvLoadTestRequest::TNbsDbgLikeLoad Cmd;
    const TActorId Parent;
    const ui64 Tag;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TVector<TChild> Children;
    size_t Pending = 0;
    bool HasMerged = false;
    bool Emitted = false;
    TNbsDbgLikeFinishStats Merged;
};

} // anonymous namespace

NActors::IActor* CreateNbsDbgLikeLoadActor(
    const NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
    const NActors::TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag)
{
    if (cmd.TargetsSize() > 0) {
        return new TNbsDbgLikeMultiLoadActor(cmd, parent, std::move(counters), tag);
    }
    return new TNbsDbgLikeLoadActorProxy(cmd, parent, std::move(counters), tag);
}

} // namespace NKikimr::NNbsDbgLike
