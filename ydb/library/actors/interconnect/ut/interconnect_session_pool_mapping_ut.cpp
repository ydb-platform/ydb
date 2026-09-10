#include <ydb/library/actors/interconnect/interconnect_session_pool_mapping.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>

namespace NActors {

Y_UNIT_TEST_SUITE(InterconnectSessionPoolMapping) {

Y_UNIT_TEST(RejectsEmptyMapping) {
    UNIT_ASSERT_EXCEPTION_CONTAINS(
        TInterconnectSessionPoolMapping({}),
        yexception,
        "must not be empty");
}

Y_UNIT_TEST(SelectsPoolByPeerNodeModulo) {
    const TInterconnectSessionPoolMapping mapping({10, 20, 30});

    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(0), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(1), 20);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(2), 30);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(3), 10);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(100), 20);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(std::numeric_limits<ui32>::max()), 10);
}

Y_UNIT_TEST(SinglePoolMappingIsStableForEveryPeer) {
    const TInterconnectSessionPoolMapping mapping({42});

    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(0), 42);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(123456789), 42);
    UNIT_ASSERT_VALUES_EQUAL(mapping.GetPoolId(std::numeric_limits<ui32>::max()), 42);
}

} // Y_UNIT_TEST_SUITE(InterconnectSessionPoolMapping)

} // namespace NActors
