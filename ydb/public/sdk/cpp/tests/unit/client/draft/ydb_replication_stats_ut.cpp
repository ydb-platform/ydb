#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(ReplicationStats) {
    Y_UNIT_TEST(OldServerAndExplicitZero) {
        Ydb::Replication::DescribeReplicationResult::Stats proto;
        const NReplication::TStats oldServer(proto);
        UNIT_ASSERT(!oldServer.GetWriteSpeedBytesPerSecond());
        UNIT_ASSERT(!oldServer.GetInitialScanEta());
        proto.set_write_speed_bytes_per_second(0);
        proto.mutable_initial_scan_eta()->set_seconds(0);
        const NReplication::TStats completed(proto);
        UNIT_ASSERT(completed.GetWriteSpeedBytesPerSecond());
        UNIT_ASSERT_DOUBLES_EQUAL(*completed.GetWriteSpeedBytesPerSecond(), 0, 1e-9);
        UNIT_ASSERT(completed.GetInitialScanEta());
        UNIT_ASSERT_VALUES_EQUAL(*completed.GetInitialScanEta(), TDuration::Zero());
    }

    Y_UNIT_TEST(PositiveNanosecondDoesNotBecomeCompletion) {
        Ydb::Replication::DescribeReplicationResult::Stats proto;
        proto.mutable_initial_scan_eta()->set_nanos(1);
        const NReplication::TStats stats(proto);
        UNIT_ASSERT_VALUES_EQUAL(*stats.GetInitialScanEta(), TDuration::MicroSeconds(1));
    }

    Y_UNIT_TEST(ItemAndOverallPreserveFractionalDuration) {
        Ydb::Replication::DescribeReplicationResult proto;
        proto.mutable_running()->mutable_stats()->set_write_speed_bytes_per_second(1234.5);
        proto.mutable_running()->mutable_stats()->mutable_initial_scan_eta()->set_seconds(42);
        auto* first = proto.add_items();
        first->set_id(1);
        first->mutable_stats()->set_write_speed_bytes_per_second(234.5);
        first->mutable_stats()->mutable_initial_scan_eta()->set_nanos(1000);
        proto.add_items()->set_id(2); // A target reported by an older server.

        const auto serialized = proto.SerializeAsString();
        Ydb::Replication::DescribeReplicationResult parsed;
        UNIT_ASSERT(parsed.ParseFromString(serialized));
        const NReplication::TReplicationDescription description(parsed);
        const auto& overall = description.GetRunningState().GetStats();
        UNIT_ASSERT_DOUBLES_EQUAL(*overall.GetWriteSpeedBytesPerSecond(), 1234.5, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(*overall.GetInitialScanEta(), TDuration::Seconds(42));
        const auto& items = description.GetItems();
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
        UNIT_ASSERT_DOUBLES_EQUAL(*items[0].Stats.GetWriteSpeedBytesPerSecond(), 234.5, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(*items[0].Stats.GetInitialScanEta(), TDuration::MicroSeconds(1));
        UNIT_ASSERT(!items[1].Stats.GetWriteSpeedBytesPerSecond());
        UNIT_ASSERT(!items[1].Stats.GetInitialScanEta());
    }
} // Y_UNIT_TEST_SUITE(ReplicationStats)
