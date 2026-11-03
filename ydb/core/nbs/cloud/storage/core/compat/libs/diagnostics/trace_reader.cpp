#include "trace_reader.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/public.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/lwtrace/log.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <utility>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

const NLWTrace::TParam* FindParam(
    const NLWTrace::TLogItem& logItem,
    const TStringBuf name)
{
    if (const auto* probe = logItem.Probe) {
        for (ui32 i = 0; i < probe->Event.Signature.ParamCount; ++i) {
            if (probe->Event.Signature.ParamNames[i] == name) {
                return &logItem.Params.Param[i];
            }
        }
    }

    return nullptr;
}

void SerializeTraceToJson(
    const NLWTrace::TTrackLog& tl,
    ui64 minSeenTimestamp,
    const TString& tag,
    NJsonWriter::TBuf& writer)
{
    TString paramValues[LWTRACE_MAX_PARAMS];

    writer.BeginList();
    for (const auto& item: tl.Items) {
        if (!item.Probe) {
            // NBS-492#5d45b57d701665001d0e946e
            continue;
        }

        const auto spanLength =
            CyclesToDurationSafe(item.TimestampCycles - minSeenTimestamp);

        item.Probe->Event.Signature.SerializeParams(item.Params, paramValues);

        writer.BeginList();
        {
            writer.WriteString(item.Probe->Event.Name);
            writer.WriteULongLong(spanLength.MicroSeconds());
            for (size_t i = 0; i < item.SavedParamsCount; ++i) {
                writer.WriteString(paramValues[i]);
            }
        }
        writer.EndList();
    }
    writer.BeginList();
    writer.WriteString(tag);
    writer.EndList();
    writer.EndList();
}

TString SerializeTraceToString(
    const NLWTrace::TTrackLog& tl,
    ui64 minSeenTimestamp,
    const TString& tag)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_UNSAFE, &ss);

    SerializeTraceToJson(tl, minSeenTimestamp, tag, writer);

    return ss.Str();
}

////////////////////////////////////////////////////////////////////////////////

class TTraceLogger final: public ITraceReader
{
private:
    const ITraceReaderPtr Consumer;
    const TString Tag;
    const ELogPriority Priority;

    TLog Log;

public:
    TTraceLogger(
        TString id,
        ITraceReaderPtr consumer,
        TString tag,
        ILoggingServicePtr logging,
        const TString& componentName,
        ELogPriority priority)
        : ITraceReader(std::move(id))
        , Consumer(std::move(consumer))
        , Tag(std::move(tag))
        , Priority(priority)
    {
        Log = logging->CreateLog(componentName);
    }

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        if (tl.Items.empty()) {
            return;
        }

        ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

        Log.Write(Priority, SerializeTraceToString(tl, minSeenTimestamp, Tag));
        Consumer->Push(tid, tl);
    }

    void Reset() override
    {
        Consumer->Reset();
    }

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        Consumer->ForEach(fn);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSlowRequestsFilter final: public ITraceReader
{
private:
    TLog Log;

    ITraceReaderPtr Consumer;

    TRequestThresholds RequestThresholds;

    THashMap<TString, ui64> SeenStartProbes;

public:
    TSlowRequestsFilter(
        TString id,
        ITraceReaderPtr consumer,
        ILoggingServicePtr logging,
        const TString& componentName,
        TRequestThresholds requestThresholds)
        : ITraceReader(std::move(id))
        , Consumer(std::move(consumer))
        , RequestThresholds(std::move(requestThresholds))
    {
        Log = logging->CreateLog(componentName);
    }

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        if (tl.Items.empty()) {
            return;
        }

        // first pass to measure track length
        ui64 maxSeenTimestamp = 0;
        ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

        for (const auto& item: tl.Items) {
            // item.Timestamp is usually 0
            const auto ts = item.TimestampCycles;
            maxSeenTimestamp = Max(maxSeenTimestamp, ts);
        }

        using namespace NYdb::NBS::NProbeParam;

        const auto* mediaKindParam = FindParam(tl.Items.front(), MediaKind);
        const auto* requestTypeParam = FindParam(tl.Items.front(), RequestType);

        if (tl.Items.front().Probe) {
            ++SeenStartProbes[tl.Items.front().Probe->Event.Name];
        }

        if (!mediaKindParam) {
            return;
        }

        auto mediaKind = mediaKindParam->Get<NProto::EStorageMediaKind>();

        const auto* requestSizeParam = FindParam(tl.Items.front(), RequestSize);

        auto srt = GetThresholdByRequestType(
            mediaKind,
            RequestThresholds,
            requestTypeParam,
            requestSizeParam);

        const auto* executionTimeParam =
            FindParam(tl.Items.back(), RequestExecutionTime);

        TDuration trackLength;
        if (executionTimeParam) {
            trackLength =
                TDuration::MicroSeconds(executionTimeParam->Get<ui64>());
        } else {
            trackLength =
                CyclesToDurationSafe(maxSeenTimestamp - minSeenTimestamp);
        }

        if (trackLength >= srt) {
            Consumer->Push(tid, tl);
        }
    }

    void Reset() override
    {
        TStringBuilder out;
        out << "Filter: " << Id << " stats";
        for (auto& p: SeenStartProbes) {
            out << '[' << p.first << ',' << p.second << ']';
            p.second = 0;
        }
        Log.Write(ELogPriority::TLOG_DEBUG, out);
        Consumer->Reset();
    }

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        Consumer->ForEach(std::move(fn));
    }
};

class TTraceLimiter final: public ITraceReader
{
private:
    const ui64 TraceLimit;
    ui64 TracesCount = 0;
    ITraceReaderPtr Consumer;

public:
    TTraceLimiter(TString id, ITraceReaderPtr consumer, ui64 traceLimit)
        : ITraceReader(std::move(id))
        , TraceLimit(traceLimit)
        , Consumer(std::move(consumer))
    {}

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override
    {
        if (tl.Items.empty() || TracesCount >= TraceLimit) {
            return;
        }

        ++TracesCount;

        Consumer->Push(tid, tl);
    }

    void Reset() override
    {
        TracesCount = 0;
        Consumer->Reset();
    }

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        Consumer->ForEach(std::move(fn));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TTraceReaderWithRingBuffer::TTraceReaderWithRingBuffer(TString id, TString tag)
    : ITraceReader(std::move(id))
    , RingBuffer(DefaultRingBufferSize)
    , Tag(std::move(tag))
{}

void TTraceReaderWithRingBuffer::Push(
    TThread::TId tid,
    const NLWTrace::TTrackLog& tl)
{
    Y_UNUSED(tid);

    if (tl.Items.empty()) {
        return;
    }

    ui64 minSeenTimestamp = tl.Items[0].TimestampCycles;

    RingBuffer.PushBack(
        {.Ts = TInstant::Now(),
         .Date = minSeenTimestamp,
         .TrackLog = tl,
         .Tag = Tag});
}

void TTraceReaderWithRingBuffer::Reset()
{}

void TTraceReaderWithRingBuffer::ForEach(std::function<void(const TEntry&)> fn)
{
    for (ui64 i = RingBuffer.FirstIndex(); i < RingBuffer.TotalSize(); ++i) {
        fn(RingBuffer[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceLogger(
    TString id,
    ITraceReaderPtr consumer,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ELogPriority priority)
{
    return std::make_shared<TTraceLogger>(
        std::move(id),
        std::move(consumer),
        std::move(tag),
        std::move(logging),
        std::move(componentName),
        priority);
}

ITraceReaderPtr CreateSlowRequestsFilter(
    TString id,
    ITraceReaderPtr consumer,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds)
{
    return std::make_shared<TSlowRequestsFilter>(
        std::move(id),
        std::move(consumer),
        std::move(logging),
        std::move(componentName),
        std::move(requestThresholds));
}

ITraceReaderPtr
CreateTraceLimiter(TString id, ITraceReaderPtr consumer, ui64 limit)
{
    return std::make_shared<TTraceLimiter>(
        std::move(id),
        std::move(consumer),
        limit);
}

ITraceReaderPtr SetupTraceReaderWithLog(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ELogPriority priority)
{
    ITraceReaderPtr reader =
        std::make_shared<TTraceReaderWithRingBuffer>(id, tag);
    reader = CreateTraceLogger(
        id,
        std::move(reader),
        std::move(logging),
        std::move(componentName),
        std::move(tag),
        priority);
    return CreateTraceLimiter(
        std::move(id),
        std::move(reader),
        NYdb::NBS::DumpTracksLimit);
}

ITraceReaderPtr SetupTraceReaderForSlowRequests(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds,
    TString tag)
{
    auto reader = SetupTraceReaderWithLog(
        id,
        logging,
        componentName,
        std::move(tag),
        TLOG_WARNING);
    return CreateSlowRequestsFilter(
        std::move(id),
        std::move(reader),
        std::move(logging),
        std::move(componentName),
        std::move(requestThresholds));
}

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate)
{
    return ProbabilisticQuery(probes, samplingRate, 0);
}

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate,
    ui32 shuttleCount)
{
    NLWTrace::TQuery q;

    Y_DEBUG_ABORT_UNLESS(samplingRate > 0);
    if (samplingRate == 0) {
        return q;
    }

    for (const auto& x: probes) {
        auto* block = q.AddBlocks();
        block->MutableProbeDesc()->SetName(std::get<0>(x));
        block->MutableProbeDesc()->SetProvider(std::get<1>(x));
        auto& action = *block->AddAction()->MutableRunLogShuttleAction();
        action.SetMaxTrackLength(1000);
        if (shuttleCount) {
            action.SetShuttlesCount(shuttleCount);
        }

        auto* predicate = block->MutablePredicate();
        predicate->SetSampleRate(1.0 / samplingRate);
    }

    return q;
}

////////////////////////////////////////////////////////////////////////////////

TRequestThresholds ConvertRequestThresholds(
    const TProtoRequestThresholds& value)
{
    TRequestThresholds requestThresholds;
    for (const auto& threshold: value) {
        TLWTraceThreshold requestThreshold;
        requestThreshold.Default =
            TDuration::MilliSeconds(threshold.GetDefault());
        requestThreshold.PerSizeUnit =
            TDuration::MilliSeconds(threshold.GetPerSizeUnit());
        for (const auto& [rType, typeThresh]: threshold.GetByRequestType()) {
            requestThreshold.ByRequestType[rType] =
                TDuration::MilliSeconds(typeThresh);
        }
        const auto mediaKind = threshold.GetMediaKind();
        Y_DEBUG_ABORT_UNLESS(!requestThresholds.count(mediaKind));
        requestThresholds[mediaKind] = std::move(requestThreshold);
    }
    return requestThresholds;
}

void OutRequestThresholds(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    for (const auto& [mediaKind, tr]: value) {
        NCloud::NProto::TLWTraceThreshold protoTr;
        protoTr.SetMediaKind(
            static_cast<NCloud::NProto::EStorageMediaKind>(mediaKind));
        protoTr.SetDefault(tr.Default.MilliSeconds());
        protoTr.SetPerSizeUnit(tr.PerSizeUnit.MilliSeconds());
        auto& protoByRequestType = *protoTr.MutableByRequestType();
        for (const auto& [requestType, duration]: tr.ByRequestType) {
            protoByRequestType[requestType] = duration.MilliSeconds();
        }
        SerializeToTextFormat(protoTr, out);
    }
}

TDuration GetThresholdByRequestType(
    const NProto::EStorageMediaKind mediaKind,
    const TRequestThresholds& requestThresholds,
    const NLWTrace::TParam* requestTypeParam,
    const NLWTrace::TParam* requestSizeParam)
{
    TDuration srt;
    auto mediaKindThresholdsIt = requestThresholds.find(mediaKind);
    if (mediaKindThresholdsIt == requestThresholds.end()) {
        mediaKindThresholdsIt = requestThresholds.find(
            NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT);
    }

    if (mediaKindThresholdsIt != requestThresholds.end()) {
        const auto& mediaKindThresholds = mediaKindThresholdsIt->second;
        srt = mediaKindThresholds.Default;
        if (requestTypeParam) {
            const auto& requestType = requestTypeParam->Get<TString>();
            const auto threshold =
                mediaKindThresholds.ByRequestType.find(requestType);
            if (threshold != mediaKindThresholds.ByRequestType.end()) {
                srt = Max(srt, threshold->second);
            }
        }
        if (requestSizeParam) {
            auto requestSize = requestSizeParam->Get<ui64>();
            srt += mediaKindThresholds.PerSizeUnit * requestSize / 1_KB;
        }
    }
    return srt;
}

}   // namespace NCloud
