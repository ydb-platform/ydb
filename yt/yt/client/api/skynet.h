#pragma once

#include "public.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TSkynetSharePartsLocations
    : public TRefCounted
{
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
};

DEFINE_REFCOUNTED_TYPE(TSkynetSharePartsLocations)

void Serialize(
    const TSkynetSharePartsLocations& skynetPartsLocations,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
