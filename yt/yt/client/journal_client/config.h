#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/client/chunk_client/config.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderConfig
    : public virtual NChunkClient::TReplicationReaderConfig
{
public:
    //! Reader will skip replicas with less than this amount of relevant data
    //! data available.
    i64 ReplicaDataSizeReadThreshold;

    //! When fetching replica metas, journal reader will wait for this period of time
    //! before starting slow path in hope to run fast path.
    TDuration SlowPathDelay;

    REGISTER_YSON_STRUCT(TChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
