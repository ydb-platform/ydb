#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/config.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

struct TFileChunkWriterConfig
    : public virtual NChunkClient::TEncodingWriterConfig
{
    i64 BlockSize;

    REGISTER_YSON_STRUCT(TFileChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
