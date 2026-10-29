#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>

namespace NYdb::NDump {

Y_UNIT_TEST_SUITE(ReplicationDump) {
    TString Query(const Ydb::Replication::DescribeReplicationResult& proto) {
        return BuildCreateReplicationQuery("/Root", "/backup", "replication", NReplication::TReplicationDescription(proto));
    }

    Y_UNIT_TEST(AvailabilityPeriodUnlimited) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_availability_period()->mutable_unlimited();
        UNIT_ASSERT_STRING_CONTAINS(Query(proto), "AVAILABILITY_PERIOD = 'UNLIMITED'");
    }

    Y_UNIT_TEST(AvailabilityPeriodFinitePrecision) {
        Ydb::Replication::DescribeReplicationResult proto;
        auto& duration = *proto.mutable_availability_period()->mutable_duration();
        duration.set_nanos(1000000);
        UNIT_ASSERT_STRING_CONTAINS(Query(proto), "AVAILABILITY_PERIOD = Interval('PT0.001S')");
        duration.set_seconds(10);
        UNIT_ASSERT_STRING_CONTAINS(Query(proto), "AVAILABILITY_PERIOD = Interval('PT10.001S')");
        duration.set_nanos(0);
        UNIT_ASSERT_STRING_CONTAINS(Query(proto), "AVAILABILITY_PERIOD = Interval('PT10S')");
    }

    Y_UNIT_TEST(AvailabilityPeriodUnknownOrInvalidFails) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT_EXCEPTION_CONTAINS(Query(proto), yexception, "availability period is unknown");
        proto.mutable_availability_period();
        UNIT_ASSERT_EXCEPTION_CONTAINS(Query(proto), yexception, "invalid availability period");
    }
}

}
