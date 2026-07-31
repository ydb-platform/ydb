
#include "volume_counters.h"

#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TVector<double> RequestTimeBoundsMs = {
    0.01,   // 10th us
    0.02,
    0.03,
    0.04,
    0.05,
    0.1,   // 100th us
    0.25,
    0.5,
    0.75,
    1,   // ms
    2,
    4,
    8,
    32,
    128,
    1'024,   // s
    65'536   // minutes
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeRequestCounters::TVolumeRequestCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : Requests(parent ? parent->GetCounter("Requests", true) : nullptr)
    , ReplyOk(parent ? parent->GetCounter("ReplyOk", true) : nullptr)
    , ReplyErr(parent ? parent->GetCounter("ReplyErr", true) : nullptr)
    , Bytes(parent ? parent->GetCounter("Bytes", true) : nullptr)
    , RequestTime(
          parent ? parent->GetHistogram(
                       "RequestTimeMs",
                       NMonitoring::ExplicitHistogram(RequestTimeBoundsMs))
                 : nullptr)
{}

void TVolumeRequestCounters::RequestStarted(ui32 bytes)
{
    if (Requests) {
        ++*Requests;
    }
    if (bytes && Bytes) {
        *Bytes += bytes;
    }
}

void TVolumeRequestCounters::RequestFinished(bool ok, TDuration duration)
{
    if (ok && ReplyOk) {
        ++*ReplyOk;
    } else if (!ok && ReplyErr) {
        ++*ReplyErr;
    }
    if (RequestTime && duration != TDuration::Zero()) {
        RequestTime->Collect(duration.MillisecondsFloat());
    }
}

////////////////////////////////////////////////////////////////////////////////

TVolumeCounters::TVolumeCounters(NMonitoring::TDynamicCounterPtr parent)
    : ReadBlocks(
          parent ? parent->GetSubgroup("operation", "ReadBlocks") : nullptr)
    , WriteBlocks(
          parent ? parent->GetSubgroup("operation", "WriteBlocks") : nullptr)
    , ZeroBlocks(
          parent ? parent->GetSubgroup("operation", "ZeroBlocks") : nullptr)
{}

void TVolumeCounters::RequestStarted(EBlockStoreRequest requestType, ui32 bytes)
{
    Get(requestType).RequestStarted(bytes);
}

void TVolumeCounters::RequestFinished(
    EBlockStoreRequest requestType,
    bool ok,
    TDuration duration)
{
    Get(requestType).RequestFinished(ok, duration);
}

TVolumeRequestCounters& TVolumeCounters::Get(EBlockStoreRequest requestType)
{
    switch (requestType) {
        case EBlockStoreRequest::ReadBlocks:
            return ReadBlocks;
        case EBlockStoreRequest::WriteBlocks:
            return WriteBlocks;
        case EBlockStoreRequest::ZeroBlocks:
            return ZeroBlocks;

        case EBlockStoreRequest::MAX:
            Y_ASSERT(false);
    }
    return ReadBlocks;
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
