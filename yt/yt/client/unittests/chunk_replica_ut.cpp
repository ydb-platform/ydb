#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <util/random/random.h>

#include <stdlib.h>

namespace NYT::NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkReplicaWithMediumTest, Simple)
{
    int totalNodes = 10;
    srand(0);
    for (int i = 0; i < totalNodes; ++i) {
        auto nodeId = NNodeTrackerClient::TNodeId(rand() % (1 << 23));
        int replicaIndex = rand() % (1 << 4);
        int mediumIndex = rand() % (1 << 7);
        TChunkReplicaWithMedium replica(nodeId, replicaIndex, mediumIndex);

        EXPECT_THAT(replica.GetNodeId(), nodeId);
        EXPECT_THAT(replica.GetReplicaIndex(), replicaIndex);
        EXPECT_THAT(replica.GetMediumIndex(), mediumIndex);

        ui64 value;
        ToProto(&value, replica);

        TChunkReplicaWithMedium replica1;
        FromProto(&replica1, value);

        EXPECT_THAT(replica.GetNodeId(), replica1.GetNodeId());
        EXPECT_THAT(replica.GetReplicaIndex(), replica1.GetReplicaIndex());
        EXPECT_THAT(replica.GetMediumIndex(), replica1.GetMediumIndex());

        TChunkReplica replica2(replica);
        EXPECT_THAT(replica.GetNodeId(), replica2.GetNodeId());
        EXPECT_THAT(replica.GetReplicaIndex(), replica2.GetReplicaIndex());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
