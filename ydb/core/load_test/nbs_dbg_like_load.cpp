#include "events.h"
#include "nbs_dbg_like_load.h"
#include "nbs_dbg_like_load_defs.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipe.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/histogram/hdr/histogram.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>
#include <util/random/fast.h>
#include <util/string/builder.h>
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

struct TInflightEntry {
    ui64 Address = 0;
    ui32 SizeBytes = 0;
    NHPTimer::STime SentAt = 0;
    bool IsRead = false;
};

class TNbsDbgLikeLoadActor : public TActorBootstrapped<TNbsDbgLikeLoadActor> {
public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BS_LOAD_NBS_DBG_LIKE;
    }

    TNbsDbgLikeLoadActor(
        const TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
        const TActorId& parent,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        ui64 tag)
        : Parent(parent)
        , Tag(tag)
        , TabletId(cmd.GetNbsDbgLikeTabletId())
        , Config(cmd.GetWorkloadConfig())
        , Rng(TInstant::Now().GetValue() ^ tag)
        , Counters(std::move(counters))
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
            << " Sequential# " << Config.GetSequential());

        if (TabletId == 0) {
            ErrorReason = "NbsDbgLikeTabletId is not set";
            FinishRun();
            return;
        }

        NTabletPipe::TClientConfig pipeConfig{
            .RetryPolicy = {.RetryLimitCount = kPipeRetryLimit}
        };
        PipeClient = Register(NTabletPipe::CreateClient(SelfId(), TabletId, pipeConfig));

        auto req = std::make_unique<TEvLoad::TEvNbsLoadTabletGetSummary>();
        NTabletPipe::SendData(SelfId(), PipeClient, req.release());

        Schedule(kInitTimeout, new TEvents::TEvWakeup(kWakeupInitTimeoutTag));
        Become(&TNbsDbgLikeLoadActor::StateInit);
    }

private:
    // ---- StateInit: wait for GetSummary reply before starting the load ----

    void HandleSummaryResult(TEvLoad::TEvNbsLoadTabletGetSummaryResult::TPtr& ev) {
        const auto& rec = ev->Get()->Record;
        if (rec.GetStatus() != NBSLT_OK) {
            ErrorReason = TStringBuilder()
                << "GetSummary failed: status=" << rec.GetStatus()
                << " reason=" << rec.GetErrorReason();
            LOG_N("GetSummary error Tag# " << Tag << " " << ErrorReason);
            FinishRun();
            return;
        }

        const ui32 totalDbgs = rec.GetNumDirectBlockGroups();
        const ui32 readyDbgs = rec.GetNumReadyDirectBlockGroups();
        VChunkSizeBytes = rec.GetVChunkSizeBytes();
        TargetNumVChunks = rec.GetTargetNumVChunks();

        // Use the ready count (longest contiguous prefix of DBGs with peers
        // connected) as the base. Only target DBGs that can actually accept
        // writes; targeting unconnected DBGs would silently drop writes and
        // consume the entire inflight budget. Fall back to the total count for
        // backward compatibility with tablets that don't set the field yet.
        EffectiveDbgCount = (readyDbgs > 0) ? readyDbgs : totalDbgs;

        // Apply NumDirectBlockGroupsToUse slice.
        const ui32 mDbg = Config.GetNumDirectBlockGroupsToUse();
        if (mDbg > 0 && mDbg < EffectiveDbgCount) {
            EffectiveDbgCount = mDbg;
        }

        Validate();
        if (!ErrorReason.empty()) {
            LOG_N("Validate error Tag# " << Tag << " " << ErrorReason);
            FinishRun();
            return;
        }

        LOG_N("GetSummary OK Tag# " << Tag
            << " TotalDbgCount# " << totalDbgs
            << " ReadyDbgCount# " << readyDbgs
            << " EffectiveDbgCount# " << EffectiveDbgCount
            << " VChunkSizeBytes# " << VChunkSizeBytes
            << " TargetNumVChunks# " << TargetNumVChunks
            << " IoSizeBytes# " << IoSizeBytes
            << " — starting load");

        // Tell the tablet the per-run I/O size and DBG slice.
        {
            auto cfg = std::make_unique<TEvLoad::TEvConfigureTablet>();
            cfg->Record = Config.GetTabletConfig();
            cfg->Record.SetNumDirectBlockGroupsToUse(EffectiveDbgCount);
            cfg->Record.SetIoSizeBytes(IoSizeBytes);
            NTabletPipe::SendData(SelfId(), PipeClient, cfg.release());
        }

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
        ErrorReason = "pipe lost before GetSummary reply";
        FinishRun();
    }

    void HandleInitPoison(TEvents::TEvPoisonPill::TPtr&) {
        ErrorReason = "stopped before start";
        FinishRun();
    }

    void HandleInitWakeup(TEvents::TEvWakeup::TPtr& ev) {
        if (ev->Get()->Tag == kWakeupInitTimeoutTag) {
            LOG_N("GetSummary timeout after " << kInitTimeout
                << " Tag# " << Tag
                << " TabletId# " << TabletId);
            ErrorReason = TStringBuilder()
                << "GetSummary timed out after " << kInitTimeout
                << " (TabletId " << TabletId << ")";
            FinishRun();
        }
    }

    // ---- Helpers shared by both states ----

    void Validate() {
        if (EffectiveDbgCount == 0) {
            ErrorReason = "EffectiveDbgCount=0 (NumDirectBlockGroups from tablet)";
            return;
        }
        if (TargetNumVChunks == 0) {
            ErrorReason = "TargetNumVChunks=0 (from tablet GetSummary)";
            return;
        }
        if (VChunkSizeBytes == 0 || VChunkSizeBytes % kSectorSize != 0) {
            ErrorReason = TStringBuilder()
                << "VChunkSizeBytes (" << VChunkSizeBytes
                << ") must be a positive multiple of " << kSectorSize;
            return;
        }
        const ui32 kib = Config.GetReadWriteSizeKiB();
        if (kib < 4 || (kib % 4) != 0) {
            ErrorReason = TStringBuilder()
                << "ReadWriteSizeKiB (" << kib << " KiB) must be >= 4 and a multiple of 4";
            return;
        }
        const ui64 ioBytes = static_cast<ui64>(kib) * 1024;
        if (ioBytes > VChunkSizeBytes) {
            ErrorReason = TStringBuilder()
                << "ReadWriteSizeKiB * 1024 (" << ioBytes
                << ") exceeds VChunkSizeBytes (" << VChunkSizeBytes << ")";
            return;
        }
        if (ioBytes > Max<ui32>()) {
            ErrorReason = "ReadWriteSizeKiB * 1024 overflows ui32";
            return;
        }
        if (Config.GetDurationSeconds() == 0 && Config.GetStopOnWritesDoneCount() == 0) {
            ErrorReason = "at least one of DurationSeconds or StopOnWritesDoneCount must be non-zero";
            return;
        }
        if (Config.GetDurationSeconds() > 0
            && Config.GetDelayBeforeMeasurementsSeconds() >= Config.GetDurationSeconds())
        {
            ErrorReason = TStringBuilder()
                << "DelayBeforeMeasurementsSeconds (" << Config.GetDelayBeforeMeasurementsSeconds()
                << ") must be < DurationSeconds (" << Config.GetDurationSeconds() << ")";
            return;
        }
        if (Config.GetTabletConfig().GetDisableReplication() && Config.GetReadRatio() > 0) {
            ErrorReason = "DisableReplication is incompatible with ReadRatio > 0 (reads require flush)";
            return;
        }

        IoSizeBytes = static_cast<ui32>(ioBytes);
        BytesPerDbg = static_cast<ui64>(TargetNumVChunks) * VChunkSizeBytes;
        AddressSpaceBytes = static_cast<ui64>(EffectiveDbgCount) * BytesPerDbg;
        IoUnitsPerVChunk = VChunkSizeBytes / IoSizeBytes;
        AddressSpaceIoUnits = static_cast<ui64>(EffectiveDbgCount)
            * static_cast<ui64>(TargetNumVChunks) * IoUnitsPerVChunk;
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
        Y_ABORT_UNLESS(AddressSpaceIoUnits != 0);
        ui64 unit;
        if (Config.GetSequential()) {
            unit = NextSequentialIoUnit;
            if (++NextSequentialIoUnit >= AddressSpaceIoUnits) {
                NextSequentialIoUnit = 0;
            }
        } else {
            unit = Rng.GenRand() % AddressSpaceIoUnits;
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
                IssueRead();
            } else if (stopCount == 0 || WritesIssued < stopCount) {
                IssueWrite();
            } else {
                break; // write cap reached; no more ops to issue
            }
        }
    }

    void IssueWrite() {
        const ui32 size = IoSizeBytes;
        const ui64 addr = PickAddress(size);
        const ui64 cookie = ++NextCookie;
        NHPTimer::STime sentAt = 0;
        NHPTimer::GetTime(&sentAt);
        Inflight[cookie] = TInflightEntry{addr, size, sentAt, /*IsRead=*/false};
        auto ev = std::make_unique<TEvLoad::TEvNbsWrite>(addr, size);
        const ui32 payloadId = ev->AddPayload(TRope(WritePayload));
        ev->Record.SetPayloadId(payloadId);
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
    }

    void IssueRead() {
        const ui32 size = IoSizeBytes;
        const ui64 addr = PickAddress(size);
        const ui64 cookie = ++NextCookie;
        NHPTimer::STime sentAt = 0;
        NHPTimer::GetTime(&sentAt);
        Inflight[cookie] = TInflightEntry{addr, size, sentAt, /*IsRead=*/true};
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
    }

    void HandleWriteResult(TEvLoad::TEvNbsWriteResult::TPtr& ev) {
        const ui64 cookie = ev->Cookie;
        auto it = Inflight.find(cookie);
        if (it == Inflight.end()) {
            return;
        }
        const TInflightEntry e = it->second;
        Inflight.erase(it);

        NHPTimer::STime now = 0;
        NHPTimer::GetTime(&now);
        const ui64 latencyUs = LatencyUsFromHPTimer(now - e.SentAt);
        const bool ok = ev->Get()->Record.GetStatus() == 0;
        const bool measure = InMeasurementWindow();
        LOG_T("HandleWriteResult Cookie# " << cookie << " Status# " << ev->Get()->Record.GetStatus() << " LatencyUs# " << latencyUs << " WriteInFlight# " << WriteInFlight);

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
        auto it = Inflight.find(cookie);
        if (it == Inflight.end()) {
            return;
        }
        const TInflightEntry e = it->second;
        Inflight.erase(it);

        NHPTimer::STime now = 0;
        NHPTimer::GetTime(&now);
        const ui64 latencyUs = LatencyUsFromHPTimer(now - e.SentAt);
        const bool ok = ev->Get()->Record.GetStatus() == 0;
        const bool measure = InMeasurementWindow();
        LOG_T("HandleReadResult Cookie# " << cookie << " Status# " << ev->Get()->Record.GetStatus() << " LatencyUs# " << latencyUs << " ReadInFlight# " << ReadInFlight);
        // Payload is on the actor event as a TRope; we don't validate it
        // beyond observing the status — the worker already checked sizes.
        (void)ev->Get()->Record.HasPayloadId();

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
        const double measuredSec = measuredMs > 0 ? measuredMs / 1000.0 : 1.0;

        // Compute percentiles before moving histograms into WorkerStats.
        const ui64 writeP50Us = MeasuredWriteLatencyUs.GetValueAtPercentile(50.0);
        const ui64 writeP95Us = MeasuredWriteLatencyUs.GetValueAtPercentile(95.0);
        const ui64 writeP99Us = MeasuredWriteLatencyUs.GetValueAtPercentile(99.0);
        const ui64 readP50Us  = MeasuredReadLatencyUs.GetValueAtPercentile(50.0);
        const ui64 readP95Us  = MeasuredReadLatencyUs.GetValueAtPercentile(95.0);
        const ui64 readP99Us  = MeasuredReadLatencyUs.GetValueAtPercentile(99.0);

        LOG_I("Run finished Tag# " << Tag
            << " Status# " << (ErrorReason.empty() ? "OK" : ErrorReason)
            << " DurationMs# " << durationMs
            << " MeasuredMs# " << measuredMs
            << " WritesIssued# " << WritesIssued
            << " WritesOk# " << WritesOk
            << " MeasuredWritesOk# " << MeasuredWritesOk
            << " WriteBytes# " << WriteBytes
            << " MeasuredWriteBytes# " << MeasuredWriteBytes
            << " WriteLatencyP50Us# " << writeP50Us
            << " WriteLatencyP99Us# " << writeP99Us
            << " ReadsIssued# " << ReadsIssued
            << " ReadsOk# " << ReadsOk
            << " MeasuredReadsOk# " << MeasuredReadsOk
            << " ReadBytes# " << ReadBytes
            << " MeasuredReadBytes# " << MeasuredReadBytes
            << " ReadLatencyP50Us# " << readP50Us
            << " ReadLatencyP99Us# " << readP99Us);

        auto report = MakeIntrusive<TEvLoad::TLoadReport>();
        report->Duration = TDuration::MilliSeconds(durationMs);
        report->Size = MeasuredWriteBytes + MeasuredReadBytes;
        report->InFlight = Config.GetMaxInFlight();
        report->LoadType = TEvLoad::TLoadReport::LOAD_WRITE;

        auto* finishEv = new TEvLoad::TEvLoadTestFinished(
            Tag,
            report,
            ErrorReason.empty() ? TString{} : ErrorReason);

        // Build a minimal JsonResult compatible with service_actor's aggregation.
        // The service actor will further enrich this from WorkerStats below.
        const ui64 measuredTxs = MeasuredWritesOk + MeasuredReadsOk;
        const ui64 measuredErrors = MeasuredWriteErrors + MeasuredReadErrors;
        finishEv->JsonResult["txs"] = measuredTxs;
        finishEv->JsonResult["rps"] = measuredTxs / measuredSec;
        finishEv->JsonResult["errors"] = static_cast<double>(measuredErrors) / measuredSec;
        finishEv->JsonResult["percentile"]["50"] = static_cast<double>(writeP50Us);
        finishEv->JsonResult["percentile"]["95"] = static_cast<double>(writeP95Us);
        finishEv->JsonResult["percentile"]["99"] = static_cast<double>(writeP99Us);

        // Populate typed WorkerStats so the service actor can enrich JsonResult
        // with split write/read keys consumed by the sweep table.
        {
            TNbsDbgLikeFinishStats stats;
            stats.WritesIssued  = WritesIssued;
            stats.WritesOk      = MeasuredWritesOk;
            stats.WritesErr     = MeasuredWriteErrors;
            stats.WriteBytes    = MeasuredWriteBytes;
            stats.ReadsPbOk     = MeasuredReadsOk;   // load actor measures all reads together
            stats.ReadsDDiskOk  = 0;
            stats.RunningMs     = durationMs;
            stats.MeasuredMs    = measuredMs;
            stats.MaxInFlight   = Config.GetMaxInFlight();
            stats.WriteE2eUs    = std::move(MeasuredWriteLatencyUs);
            stats.ReadPbUs      = std::move(MeasuredReadLatencyUs);
            SetNbsDbgLikeFinishStats(*finishEv, std::move(stats));
        }

        // Render a brief HTML summary as the "last page".
        {
            TStringStream html;
            html << "<b>NbsDbgLike run summary</b><br/>"
                << "Tag: " << Tag << "<br/>"
                << "TabletId: " << TabletId << "<br/>"
                << "MaxInFlight: " << Config.GetMaxInFlight() << "<br/>"
                << "Duration: " << durationMs << " ms (measured: " << measuredMs << " ms)<br/>"
                << "WritesIssued: " << WritesIssued << " (total ok: " << WritesOk
                << ") MeasuredWritesOk: " << MeasuredWritesOk
                << " MeasuredWriteBytes: " << MeasuredWriteBytes << "<br/>"
                << "WriteLatency p50=" << writeP50Us << "us p95=" << writeP95Us
                << "us p99=" << writeP99Us << "us<br/>"
                << "ReadsIssued: " << ReadsIssued << " (total ok: " << ReadsOk
                << ") MeasuredReadsOk: " << MeasuredReadsOk
                << " MeasuredReadBytes: " << MeasuredReadBytes << "<br/>"
                << "ReadLatency p50=" << readP50Us << "us p95=" << readP95Us
                << "us p99=" << readP99Us << "us<br/>";
            if (!ErrorReason.empty()) {
                html << "<b>Error:</b> " << ErrorReason << "<br/>";
            }
            finishEv->LastHtmlPage = html.Str();
        }

        Send(Parent, finishEv);

        if (Root) {
            Root->ResetCounters();
        }
        PassAway();
    }

    void HandlePipeConnected(TEvTabletPipe::TEvClientConnected::TPtr&) {
        // Nothing to do — we already sent GetSummary; wait for the reply.
    }

    STRICT_STFUNC(StateInit,
        hFunc(TEvLoad::TEvNbsLoadTabletGetSummaryResult, HandleSummaryResult)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleInitPipeDestroyed)
        hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
        hFunc(TEvents::TEvPoisonPill, HandleInitPoison)
        hFunc(TEvents::TEvWakeup, HandleInitWakeup)
    )

    STRICT_STFUNC(StateRunning,
        hFunc(TEvLoad::TEvNbsWriteResult, HandleWriteResult)
        hFunc(TEvLoad::TEvNbsReadResult, HandleReadResult)
        hFunc(TEvents::TEvPoisonPill, HandlePoison)
        hFunc(TEvents::TEvWakeup, HandleWakeup)
        hFunc(TEvTabletPipe::TEvClientDestroyed, HandleRunPipeDestroyed)
        hFunc(TEvTabletPipe::TEvClientConnected, HandlePipeConnected)
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

    // Filled in from GetSummary reply.
    ui32 EffectiveDbgCount = 0;
    ui64 VChunkSizeBytes = 0;
    ui32 TargetNumVChunks = 0;

    ui32 IoSizeBytes = 0;
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

    THashMap<ui64, TInflightEntry> Inflight;
    ui64 NextCookie = 0;

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

} // anonymous namespace

NActors::IActor* CreateNbsDbgLikeLoadActor(
    const NKikimr::TEvLoadTestRequest::TNbsDbgLikeLoad& cmd,
    const NActors::TActorId& parent,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
    ui64 tag)
{
    return new TNbsDbgLikeLoadActor(cmd, parent, std::move(counters), tag);
}

} // namespace NKikimr::NNbsDbgLike
