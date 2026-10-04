#pragma once

#include <ydb/core/nbs/cloud/storage/core/compat/protos/media.pb.h>
#include <ydb/core/nbs/cloud/storage/core/compat/protos/trace.pb.h>
#include <ydb/core/nbs/cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/containers/ring_buffer/ring_buffer.h>
#include <library/cpp/logger/priority.h>
#include <library/cpp/lwtrace/log.h>

#include <util/generic/hash.h>

namespace NLWTrace {
class TManager;
}   // namespace NLWTrace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

using NYdb::NBS::ILoggingServicePtr;

struct ITraceReader;
using ITraceReaderPtr = std::shared_ptr<ITraceReader>;

////////////////////////////////////////////////////////////////////////////////

struct TLWTraceThreshold
{
    TDuration Default;
    TDuration PerSizeUnit;
    THashMap<TString, TDuration> ByRequestType;
};

using TRequestThresholds =
    THashMap<NProto::EStorageMediaKind, TLWTraceThreshold>;

using TProtoRequestThresholds =
    google::protobuf::RepeatedPtrField<NCloud::NProto::TLWTraceThreshold>;

TRequestThresholds ConvertRequestThresholds(
    const TProtoRequestThresholds& value);

void OutRequestThresholds(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value);

struct TEntry
{
    TInstant Ts;
    ui64 Date = 0;
    NLWTrace::TTrackLog TrackLog;
    TString Tag;
};

struct ITraceReader
{
    const TString Id;

    explicit ITraceReader(TString id)
        : Id(std::move(id))
    {}

    virtual ~ITraceReader() = default;

    virtual void Push(TThread::TId tid, const NLWTrace::TTrackLog&) = 0;
    virtual void ForEach(std::function<void(const TEntry&)> fn) = 0;
    virtual void Reset() = 0;
};

class TTraceReaderWithRingBuffer final: public ITraceReader
{
    static constexpr size_t DefaultRingBufferSize = 1000;

private:
    TSimpleRingBuffer<TEntry> RingBuffer;
    const TString Tag;

public:
    TTraceReaderWithRingBuffer(TString id, TString tag);

    void Push(TThread::TId tid, const NLWTrace::TTrackLog& tl) override;

    void Reset() override;

    void ForEach(std::function<void(const TEntry&)> fn) override;
};

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceLogger(
    TString id,
    ITraceReaderPtr consumer,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ELogPriority priority = ELogPriority::TLOG_INFO);

ITraceReaderPtr CreateSlowRequestsFilter(
    TString id,
    ITraceReaderPtr consumer,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds);

ITraceReaderPtr
CreateTraceLimiter(TString id, ITraceReaderPtr consumer, ui64 limit);

ITraceReaderPtr SetupTraceReaderWithLog(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ELogPriority priority = ELogPriority::TLOG_INFO);

ITraceReaderPtr SetupTraceReaderForSlowRequests(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds,
    TString tag);

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate);

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate,
    ui32 shuttleCount);

TDuration GetThresholdByRequestType(
    const NProto::EStorageMediaKind mediaKind,
    const TRequestThresholds& requestThresholds,
    const NLWTrace::TParam* requestTypeParam,
    const NLWTrace::TParam* requestSizeParam);

}   // namespace NCloud
