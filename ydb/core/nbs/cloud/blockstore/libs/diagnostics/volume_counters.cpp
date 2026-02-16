
#include "volume_counters.h"

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TVolumeRequestCounters::TVolumeRequestCounters(
    NMonitoring::TDynamicCounterPtr parent)
    : Requests(parent ? parent->GetCounter("Requests", true) : nullptr)
    , ReplyOk(parent ? parent->GetCounter("ReplyOk", true) : nullptr)
    , ReplyErr(parent ? parent->GetCounter("ReplyErr", true) : nullptr)
    , Bytes(parent ? parent->GetCounter("Bytes", true) : nullptr)
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

void TVolumeRequestCounters::RequestFinished(bool ok)
{
    if (ok && ReplyOk) {
        ++*ReplyOk;
    } else if (!ok && ReplyErr) {
        ++*ReplyErr;
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

void TVolumeCounters::RequestFinished(EBlockStoreRequest requestType, bool ok)
{
    Get(requestType).RequestFinished(ok);
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
