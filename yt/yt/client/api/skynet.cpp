#include "skynet.h"

#include <yt/yt/client/chunk_client/read_limit.h>
#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYson;
using namespace NYTree;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSkynetSharePartsLocations& skynetPartsLocations, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("nodes").Value(skynetPartsLocations.NodeDirectory)
            .Item("chunk_specs").DoListFor(
                skynetPartsLocations.ChunkSpecs,
                [&] (TFluentList fluent, const NChunkClient::NProto::TChunkSpec& spec) {
                    fluent
                        .Item()
                        .BeginMap()
                            .Item("chunk_id").Value(FromProto<TChunkId>(spec.chunk_id()))
                            .Item("row_index").Value(spec.table_row_index())
                            .Item("row_count").Value(spec.row_count_override())
                            .Item("range_index").Value(spec.range_index())
                            .DoIf(spec.has_lower_limit(), [&] (TFluentMap fluent) {
                                fluent.Item("lower_limit").Value(FromProto<TLegacyReadLimit>(spec.lower_limit()));
                            })
                            .DoIf(spec.has_upper_limit(), [&] (TFluentMap fluent) {
                                fluent.Item("upper_limit").Value(FromProto<TLegacyReadLimit>(spec.upper_limit()));
                            })
                            .Item("replicas").DoListFor(
                                GetReplicasFromChunkSpec(spec),
                                [] (TFluentList fluent, TChunkReplica replica) {
                                    fluent.Item().Value(replica.GetNodeId());
                                })
                        .EndMap();
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
