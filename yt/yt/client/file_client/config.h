#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkWriterConfig
    : public virtual NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    REGISTER_YSON_STRUCT(TFileChunkWriterConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("block_size", &TThis::BlockSize)
            .Default(16_MB)
            .GreaterThan(0);
    }
};

DEFINE_REFCOUNTED_TYPE(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
