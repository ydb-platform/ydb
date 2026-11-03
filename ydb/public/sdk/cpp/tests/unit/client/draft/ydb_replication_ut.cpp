#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(ReplicationDescription) {
    using TDescription = NReplication::TReplicationDescription;
    using EKind = TDescription::EAvailabilityPeriodKind;

    Y_UNIT_TEST(AvailabilityPeriodUnknown) {
        const TDescription desc{Ydb::Replication::DescribeReplicationResult()};
        UNIT_ASSERT(desc.GetAvailabilityPeriodKind() == EKind::Unknown);
        UNIT_ASSERT(!desc.GetAvailabilityPeriod());
    }

    Y_UNIT_TEST(AvailabilityPeriodUnlimited) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_availability_period()->mutable_unlimited();
        const TDescription desc(proto);
        UNIT_ASSERT(desc.GetAvailabilityPeriodKind() == EKind::Unlimited);
        UNIT_ASSERT(!desc.GetAvailabilityPeriod());
    }

    Y_UNIT_TEST(AvailabilityPeriodFinite) {
        Ydb::Replication::DescribeReplicationResult proto;
        auto& duration = *proto.mutable_availability_period()->mutable_duration();
        duration.set_seconds(10);
        duration.set_nanos(1000000);
        const TDescription desc(proto);
        UNIT_ASSERT(desc.GetAvailabilityPeriodKind() == EKind::Finite);
        UNIT_ASSERT_VALUES_EQUAL(*desc.GetAvailabilityPeriod(), TDuration::MilliSeconds(10001));
    }

    Y_UNIT_TEST(AvailabilityPeriodFiniteBounds) {
        Ydb::Replication::DescribeReplicationResult proto;
        auto& duration = *proto.mutable_availability_period()->mutable_duration();
        duration.set_nanos(1);
        const TDescription submillisecond(proto);
        UNIT_ASSERT(submillisecond.GetAvailabilityPeriodKind() == EKind::Finite);
        UNIT_ASSERT_VALUES_EQUAL(*submillisecond.GetAvailabilityPeriod(), TDuration::MilliSeconds(1));
        duration.set_seconds(315576000000LL);
        duration.set_nanos(999000000);
        const TDescription maximum(proto);
        UNIT_ASSERT(maximum.GetAvailabilityPeriodKind() == EKind::Finite);
        UNIT_ASSERT_VALUES_EQUAL(*maximum.GetAvailabilityPeriod(),
            TDuration::Seconds(315576000000ULL) + TDuration::MilliSeconds(999));
    }

    Y_UNIT_TEST(AvailabilityPeriodInvalid) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_availability_period();
        const auto check = [&] {
            const TDescription desc(proto);
            UNIT_ASSERT(desc.GetAvailabilityPeriodKind() == EKind::Invalid);
            UNIT_ASSERT(!desc.GetAvailabilityPeriod());
        };
        check();
        auto& duration = *proto.mutable_availability_period()->mutable_duration();
        check();
        duration.set_seconds(-1);
        check();
        duration.set_seconds(0);
        duration.set_nanos(-1);
        check();
        duration.set_seconds(1);
        check();
        duration.set_nanos(1000000000);
        check();
        duration.set_nanos(0);
        duration.set_seconds(9223372036854775807LL);
        check();
        duration.set_seconds(315576000000LL);
        duration.set_nanos(999999999);
        check();
    }
} // Y_UNIT_TEST_SUITE(ReplicationDescription)
