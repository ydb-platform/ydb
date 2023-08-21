#pragma once

#include "data_statistics.h"
#include "ready_event_reader_base.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct IReaderBase
    : public virtual IReadyEventReaderBase
{
    virtual NProto::TDataStatistics GetDataStatistics() const = 0;
    virtual TCodecStatistics GetDecompressionStatistics() const = 0;

    virtual bool IsFetchingCompleted() const = 0;
    virtual std::vector<TChunkId> GetFailedChunkIds() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IReaderBase)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
