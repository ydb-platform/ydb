
#include "volume_counters.h"

#include <library/cpp/monlib/metrics/histogram_collector.h>

namespace NYdb::NBS::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const TVector<double> RequestTimeBoundsMs = {
    0.25,   // 250th us
    0.5,
    0.75,
    1,   // ms
    2,
    4,
    8,
    16,
    32,
    64,
    128,
    256,
    512,
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
    , Inflight(parent ? parent->GetCounter("Inflight", false) : nullptr)
    , RequestTime(
          parent ? parent->GetHistogram(
                       "RequestTimeMs",
                       NMonitoring::ExplicitHistogram(RequestTimeBoundsMs))
                 : nullptr)
{}

void TVolumeRequestCounters::RequestStarted(ui32 bytes)
{
    if (Requests) {
        Requests->Inc();
    }
    if (bytes && Bytes) {
        Bytes->Add(bytes);
    }
    if (Inflight) {
        Inflight->Inc();
    }
}

void TVolumeRequestCounters::RequestFinished(bool ok, TDuration duration)
{
    if (ok && ReplyOk) {
        ReplyOk->Inc();
    } else if (!ok && ReplyErr) {
        ReplyErr->Inc();
    }
    if (RequestTime && duration != TDuration::Zero()) {
        RequestTime->Collect(duration.MillisecondsFloat());
    }
    if (Inflight) {
        Inflight->Dec();
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
