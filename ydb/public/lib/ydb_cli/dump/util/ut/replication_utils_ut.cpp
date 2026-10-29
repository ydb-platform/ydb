#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/string/builder.h>

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

    Y_UNIT_TEST(AvailabilityPeriodSqlIntervalBoundary) {
        using EKind = NReplication::TReplicationDescription::EAvailabilityPeriodKind;
        const auto lastValid = TDuration::MilliSeconds((NYql::NUdf::MAX_TIMESTAMP - 1) / 1000);
        Ydb::Replication::DescribeReplicationResult proto;
        auto& duration = *proto.mutable_availability_period()->mutable_duration();
        duration.set_seconds(lastValid.Seconds());
        duration.set_nanos(lastValid.NanoSecondsOfSecond());
        UNIT_ASSERT_STRING_CONTAINS(Query(proto), TStringBuilder()
            << "AVAILABILITY_PERIOD = Interval('PT" << lastValid.Seconds() << ".999S')");

        for (const auto value : {lastValid + TDuration::MicroSeconds(1),
                TDuration::MicroSeconds(NYql::NUdf::MAX_TIMESTAMP), TDuration::Days(100000)})
        {
            duration.set_seconds(value.Seconds());
            duration.set_nanos(value.NanoSecondsOfSecond());
            const NReplication::TReplicationDescription desc(proto);
            UNIT_ASSERT(desc.GetAvailabilityPeriodKind() == EKind::Finite);
            UNIT_ASSERT(!BuildCreateReplicationQuery("/Root", "/backup", "replication", desc).Contains("AVAILABILITY_PERIOD"));
        }
    }

    Y_UNIT_TEST(AvailabilityPeriodUnknownOrInvalidUsesDefault) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT(!Query(proto).Contains("AVAILABILITY_PERIOD"));
        proto.mutable_availability_period();
        UNIT_ASSERT(!Query(proto).Contains("AVAILABILITY_PERIOD"));
        auto* duration = proto.mutable_availability_period()->mutable_duration();
        duration->set_seconds(-1);
        UNIT_ASSERT(!Query(proto).Contains("AVAILABILITY_PERIOD"));
        duration->set_seconds(0);
        UNIT_ASSERT(!Query(proto).Contains("AVAILABILITY_PERIOD"));
    }
}

}
