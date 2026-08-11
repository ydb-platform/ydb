#include "partition_actor_id.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionActorIdTest)
{
    Y_UNIT_TEST(RoundTripPreservesPoolId)
    {
        const NActors::TActorId original(50000, /*poolId=*/1, 12345, 2313);
        UNIT_ASSERT_VALUES_EQUAL(original.PoolID(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(original.NodeId(), 50000u);

        const TString serialized = SerializePartitionActorId(original);
        UNIT_ASSERT_VALUES_EQUAL(serialized, "[50000:1:12345:2313]");

        NActors::TActorId parsed;
        UNIT_ASSERT(TryDeserializePartitionActorId(serialized, parsed));
        UNIT_ASSERT_VALUES_EQUAL(parsed.NodeId(), original.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(parsed.PoolID(), original.PoolID());
        UNIT_ASSERT_VALUES_EQUAL(parsed.LocalId(), original.LocalId());
        UNIT_ASSERT_VALUES_EQUAL(parsed.Hint(), original.Hint());
        UNIT_ASSERT_EQUAL(parsed, original);
    }

    Y_UNIT_TEST(RejectsLegacyThreeFieldFormat)
    {
        NActors::TActorId parsed;
        UNIT_ASSERT(
            !TryDeserializePartitionActorId("[50000:12345:2313]", parsed));
    }

    Y_UNIT_TEST(RejectsMalformedInput)
    {
        NActors::TActorId parsed;
        UNIT_ASSERT(!TryDeserializePartitionActorId("", parsed));
        UNIT_ASSERT(!TryDeserializePartitionActorId("[]", parsed));
        UNIT_ASSERT(!TryDeserializePartitionActorId(
            "[50000:1:12345:2313:extra]",
            parsed));
        UNIT_ASSERT(
            !TryDeserializePartitionActorId("[50000:abc:12345:2313]", parsed));
    }
}

}   // namespace NYdb::NBS::NBlockStore
