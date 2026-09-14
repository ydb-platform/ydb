#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderConfig
    : public virtual NChunkClient::TReplicationReaderConfig
{
    REGISTER_YSON_STRUCT(TChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
