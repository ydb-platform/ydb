#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

using NTableClient::TLoadContext;
using NTableClient::TSaveContext;
using NTableClient::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChunkClientLogger, "ChunkClient");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

