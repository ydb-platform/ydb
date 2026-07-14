#include "service_actor.h"

#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/record.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/system/mutex.h>

namespace NKikimr {

namespace {

// Interconnect load actor (NInterconnect::TLoadActor, see
// ydb/library/actors/interconnect/load.cpp) does not expose its aggregated
// throughput/RTT statistics through the finish callback directly -- it only
// hands over a rendered HTML page. However it also periodically emits a
// structured plain-text summary via LOG_NOTICE(..., INTERCONNECT_SPEED_TEST, ...)
// (see NInterconnect::TLoadActor::PublishResults()). We intercept those log
// lines with a wrapping log backend, parse out the numbers, and accumulate
// full-run statistics (average throughput, RTT percentiles from the last
// window, dropped count) keyed by load name, so that the stress tool can
// render a proper summary table (see
// ydb/tools/stress_tool/device_test_tool_interconnect_test.h).

struct TAccumulatedStats {
    ui64 TotalBytes = 0;
    double TotalWindowSeconds = 0.0;
    ui64 TotalSamples = 0;      // sum of ThroughputSamples across all published windows
    ui64 NumDropped = 0;        // last seen value (cumulative counter)
    TInterconnectLoadFinishStats Last; // stats parsed from the most recent ("final") line
};

class TInterconnectStatsRegistry {
public:
    static TInterconnectStatsRegistry& Instance() {
        static TInterconnectStatsRegistry instance;
        return instance;
    }

    void Update(const TString& name, const TInterconnectLoadFinishStats& parsed,
            ui64 windowBytes, double windowSeconds, ui64 windowSamples) {
        with_lock(Lock) {
            TAccumulatedStats& acc = Stats[name];
            acc.TotalBytes += windowBytes;
            acc.TotalWindowSeconds += windowSeconds;
            acc.TotalSamples += windowSamples;
            acc.NumDropped = parsed.NumDropped;
            acc.Last = parsed;
        }
    }

    TMaybe<TInterconnectLoadFinishStats> Extract(const TString& name) {
        with_lock(Lock) {
            auto it = Stats.find(name);
            if (it == Stats.end()) {
                return Nothing();
            }
            TInterconnectLoadFinishStats result = it->second.Last;
            result.Valid = true;
            result.NumDropped = it->second.NumDropped;
            if (it->second.TotalWindowSeconds > 0) {
                result.BytesPerSecond = static_cast<ui64>(it->second.TotalBytes / it->second.TotalWindowSeconds);
            }
            Stats.erase(it);
            return result;
        }
    }

private:
    TMutex Lock;
    THashMap<TString, TAccumulatedStats> Stats;
};

// Parses a single occurrence of "<label><value><stop chars>" starting the search at `from`.
// Returns the substring right after label up to (not including) any of the stopChars, or
// updates `from` past the parsed value. Returns false if label is not found.
bool ExtractAfter(TStringBuf line, TStringBuf label, size_t& pos, TStringBuf& out, TStringBuf stopChars = " }") {
    size_t idx = line.find(label, pos);
    if (idx == TStringBuf::npos) {
        return false;
    }
    idx += label.size();
    size_t end = line.find_first_of(stopChars, idx);
    if (end == TStringBuf::npos) {
        end = line.size();
    }
    out = line.SubStr(idx, end - idx);
    pos = end;
    return true;
}

// Parses one PublishResults() LOG_NOTICE line (see load.cpp) of the form:
//   Load# 'name' Throughput# {window# 1.234s bytes# 123 samples# 4 b/s# 567 common# 890} RTT# {window# ... samples# ... 0.5000# 123us ...} NumDropped# 12[ final]
// Returns true and fills `outName`/`outStats`/`windowBytes`/`windowSeconds`/`windowSamples` on success.
bool ParsePublishResultsLine(TStringBuf line, TString& outName, TInterconnectLoadFinishStats& outStats,
        ui64& windowBytes, double& windowSeconds, ui64& windowSamples) {
    size_t pos = 0;
    TStringBuf nameBuf;
    {
        size_t idx = line.find("Load# '");
        if (idx == TStringBuf::npos) {
            return false;
        }
        idx += 7;
        size_t end = line.find('\'', idx);
        if (end == TStringBuf::npos) {
            return false;
        }
        nameBuf = line.SubStr(idx, end - idx);
        pos = end + 1;
    }
    outName = TString(nameBuf);

    TStringBuf windowStr, bytesStr, samplesStr, bpsStr;
    if (!ExtractAfter(line, "Throughput# {window# ", pos, windowStr)) {
        return false;
    }
    TDuration windowDur;
    windowSeconds = TDuration::TryParse(windowStr, windowDur) ? windowDur.SecondsFloat() : 0.0;

    if (!ExtractAfter(line, "bytes# ", pos, bytesStr)) {
        return false;
    }
    windowBytes = FromStringWithDefault<ui64>(bytesStr, 0);

    if (!ExtractAfter(line, "samples# ", pos, samplesStr)) {
        return false;
    }
    windowSamples = FromStringWithDefault<ui64>(samplesStr, 0);

    if (!ExtractAfter(line, "b/s# ", pos, bpsStr)) {
        return false;
    }
    outStats.ThroughputWindow = windowDur;
    outStats.ThroughputBytes = windowBytes;
    outStats.ThroughputSamples = windowSamples;
    outStats.BytesPerSecond = FromStringWithDefault<ui64>(bpsStr, 0);

    // RTT block: may be "<empty>" if no samples were collected in the aggregation window.
    size_t rttIdx = line.find("RTT# ", pos);
    if (rttIdx != TStringBuf::npos) {
        pos = rttIdx + 5;
        if (line.SubStr(pos, 7) != TStringBuf("<empty>")) {
            TStringBuf rttWindowStr, rttSamplesStr;
            if (ExtractAfter(line, "{window# ", pos, rttWindowStr) &&
                    ExtractAfter(line, "samples# ", pos, rttSamplesStr)) {
                TDuration rttWindowDur;
                outStats.RttWindow = TDuration::TryParse(rttWindowStr, rttWindowDur) ? rttWindowDur : TDuration::Zero();
                outStats.RttSamples = FromStringWithDefault<ui64>(rttSamplesStr, 0);

                outStats.LatencyPercentilesUs.clear();
                for (double q : {0.5, 0.9, 0.99, 0.999, 0.9999, 1.0}) {
                    TString label = Sprintf(" %.4f# ", q);
                    TStringBuf valueStr;
                    if (ExtractAfter(line, label, pos, valueStr, " }")) {
                        TDuration value;
                        if (TDuration::TryParse(valueStr, value)) {
                            outStats.LatencyPercentilesUs.emplace_back(q, value.MicroSeconds());
                        }
                    }
                }
            }
        }
    }

    TStringBuf droppedStr;
    if (ExtractAfter(line, "NumDropped# ", pos, droppedStr, " ")) {
        outStats.NumDropped = FromStringWithDefault<ui64>(droppedStr, 0);
    }

    return true;
}

class TInterconnectStatsCapturingBackend : public TLogBackend {
public:
    explicit TInterconnectStatsCapturingBackend(TAutoPtr<TLogBackend> underlying)
        : Underlying(underlying)
    {}

    void WriteData(const TLogRecord& rec) override {
        TStringBuf line(rec.Data, rec.Len);
        if (line.Contains("INTERCONNECT_SPEED_TEST") && line.Contains("Load# '")) {
            TString name;
            TInterconnectLoadFinishStats stats;
            ui64 windowBytes = 0;
            ui64 windowSamples = 0;
            double windowSeconds = 0.0;
            if (ParsePublishResultsLine(line, name, stats, windowBytes, windowSeconds, windowSamples)) {
                TInterconnectStatsRegistry::Instance().Update(name, stats, windowBytes, windowSeconds, windowSamples);
            }
        }
        Underlying->WriteData(rec);
    }

    void ReopenLog() override {
        Underlying->ReopenLog();
    }

    void ReopenLogNoFlush() override {
        Underlying->ReopenLogNoFlush();
    }

    ELogPriority FiltrationLevel() const override {
        return Underlying->FiltrationLevel();
    }

    size_t QueueSize() const override {
        return Underlying->QueueSize();
    }

private:
    TAutoPtr<TLogBackend> Underlying;
};

} // anonymous namespace

TAutoPtr<TLogBackend> WrapWithInterconnectStatsCapture(TAutoPtr<TLogBackend> underlying) {
    return TAutoPtr<TLogBackend>(new TInterconnectStatsCapturingBackend(underlying));
}

IActor *CreateInterconnectLoadTest(const NKikimr::TEvLoadTestRequest::TInterconnectLoad& cmd, const NActors::TActorId& parent,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>&, ui64 tag) {
    const TString name = cmd.HasName() ? cmd.GetName() : TString("Interconnect load #") += ToString(tag);

    NInterconnect::TLoadParams params {
        .Name = name,
        .Channel = 0U,
        .SizeMin = cmd.HasSizeMin() ? cmd.GetSizeMin() : 0U,
        .SizeMax = cmd.HasSizeMax() ? cmd.GetSizeMax() : 0U,
        .InFlyMax = cmd.GetInFlyMax(),
        .IntervalMin = cmd.HasIntervalMinUs() ? TDuration::MicroSeconds(cmd.GetIntervalMinUs()) : TDuration::Zero(),
        .IntervalMax = cmd.HasIntervalMaxUs() ? TDuration::MicroSeconds(cmd.GetIntervalMaxUs()) : TDuration::Zero(),
        .SoftLoad = cmd.HasSoftLoad() && cmd.GetSoftLoad(),
        .Duration = TDuration::Seconds(cmd.GetDurationSeconds()),
        .UseProtobufWithPayload = cmd.HasUseProtobufWithPayload() && cmd.GetUseProtobufWithPayload()
    };

    for (const auto& node : cmd.GetNodeHops())
        params.NodeHops.emplace_back(node);

    const auto callback = [tag, parent, name] (const TActorContext& ctx, TString&& html) {
        TIntrusivePtr<TEvLoad::TLoadReport> report(new TEvLoad::TLoadReport());
        auto finishEv = new TEvLoad::TEvLoadTestFinished(tag, report, "Load test finished.");
        finishEv->LastHtmlPage = std::move(html);

        if (TMaybe<TInterconnectLoadFinishStats> stats = TInterconnectStatsRegistry::Instance().Extract(name)) {
            SetInterconnectLoadFinishStats(*finishEv, std::move(*stats));
        }

        ctx.Send(parent, finishEv);
    };

    return CreateLoadActor(params, callback);
}

} // NKikimr
