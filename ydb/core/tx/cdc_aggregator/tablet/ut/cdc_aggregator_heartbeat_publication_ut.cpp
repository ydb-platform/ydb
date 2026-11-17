#include <ydb/core/tx/cdc_aggregator/tablet/cdc_aggregator_heartbeat_publication.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/cast.h>

namespace NKikimr::NCdcAggregator {

namespace {

NPQ::THeartbeat MakeHeartbeat(ui64 step) {
    return NPQ::THeartbeat{.Version = TRowVersion(step, 0), .Data = ToString(step)};
}

} // namespace

Y_UNIT_TEST_SUITE(TCdcAggregatorHeartbeatPublicationTest) {
    Y_UNIT_TEST(ShouldEmitToAllPartitionsWhenHeartbeatIsSet) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0, 1});
        publication.SetHeartbeat(MakeHeartbeat(100));

        // when
        const auto partitionIds = publication.TakePartitionsToEmit();

        // then
        UNIT_ASSERT_VALUES_EQUAL(partitionIds, TVector<ui32>({0, 1}));
    }

    Y_UNIT_TEST(ShouldNotEmitWhenHeartbeatIsNotSet) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0, 1});

        // when
        const auto partitionIds = publication.TakePartitionsToEmit();

        // then
        UNIT_ASSERT(partitionIds.empty());
    }

    Y_UNIT_TEST(ShouldNotEmitAgainWhenEmitIsInFlight) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();
        publication.SetHeartbeat(MakeHeartbeat(200));

        // when
        const auto partitionIds = publication.TakePartitionsToEmit();

        // then
        UNIT_ASSERT(partitionIds.empty());
    }

    Y_UNIT_TEST(ShouldKeepNewestHeartbeatWhenOlderIsSet) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(200));

        // when
        publication.SetHeartbeat(MakeHeartbeat(100));

        // then
        UNIT_ASSERT_VALUES_EQUAL(publication.GetHeartbeat()->Version, TRowVersion(200, 0));
    }

    Y_UNIT_TEST(ShouldEmitNewestWhenHeartbeatAdvancedDuringEmit) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();
        publication.SetHeartbeat(MakeHeartbeat(200));

        // when
        const bool emitAgain = publication.CompleteEmit(0, TRowVersion(100, 0));

        // then
        UNIT_ASSERT(emitAgain);
    }

    Y_UNIT_TEST(ShouldNotEmitAgainWhenPartitionIsUpToDate) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();

        // when
        const bool emitAgain = publication.CompleteEmit(0, TRowVersion(100, 0));

        // then
        UNIT_ASSERT(!emitAgain);
        UNIT_ASSERT(publication.TakePartitionsToEmit().empty());
    }

    Y_UNIT_TEST(ShouldSkipPartitionWhenItEmittedNewerVersion) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();
        publication.CompleteEmit(0, TRowVersion(300, 0));
        publication.SetHeartbeat(MakeHeartbeat(200));

        // when
        const auto partitionIds = publication.TakePartitionsToEmit();

        // then
        UNIT_ASSERT(partitionIds.empty());
    }

    Y_UNIT_TEST(ShouldWaitForRetryWhenPartitionDidNotEmitHeartbeat) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();

        // when
        const bool emitAgain = publication.CompleteEmit(0, TRowVersion::Min());

        // then
        UNIT_ASSERT(!emitAgain);
        UNIT_ASSERT(publication.HasFailedEmits());
        UNIT_ASSERT(publication.TakePartitionsToEmit().empty());
    }

    Y_UNIT_TEST(ShouldWaitForRetryWhenEmitFailed) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0, 1});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();

        // when
        publication.FailEmit(0);

        // then
        UNIT_ASSERT(publication.HasFailedEmits());
        UNIT_ASSERT(publication.TakePartitionsToEmit().empty());
    }

    Y_UNIT_TEST(ShouldEmitAgainWhenFailedEmitsAreRetried) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0, 1});
        publication.SetHeartbeat(MakeHeartbeat(100));
        publication.TakePartitionsToEmit();
        publication.FailEmit(0);
        publication.RetryFailedEmits();

        // when
        const auto partitionIds = publication.TakePartitionsToEmit();

        // then
        UNIT_ASSERT_VALUES_EQUAL(partitionIds, TVector<ui32>({0}));
    }

    Y_UNIT_TEST(ShouldIgnoreFailureWhenNothingIsInFlight) {
        // given
        TCdcAggregatorHeartbeatPublication publication({0});
        publication.SetHeartbeat(MakeHeartbeat(100));

        // when
        publication.FailEmit(0);

        // then
        UNIT_ASSERT(!publication.HasFailedEmits());
        UNIT_ASSERT_VALUES_EQUAL(publication.TakePartitionsToEmit(), TVector<ui32>({0}));
    }
}

} // namespace NKikimr::NCdcAggregator
