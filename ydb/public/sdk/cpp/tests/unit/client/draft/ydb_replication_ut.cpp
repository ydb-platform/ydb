#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_replication.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(ReplicationDescription) {
    using TDescription = NReplication::TReplicationDescription;
    using EKind = TDescription::EAvailabilityPeriodKind;

    Y_UNIT_TEST(ChecksumStatusPreservesLastResult) {
        Ydb::Replication::DescribeReplicationResult proto;
        auto* status = proto.add_items()->mutable_checksum_status();
        status->set_status(Ydb::StatusIds::TIMEOUT);
        status->add_issues()->set_message("latest attempt timed out");
        status->mutable_finished_at()->set_seconds(200);
        auto* result = status->mutable_last_result();
        result->mutable_source()->set_row_count(1);
        result->mutable_source()->set_hash("18446744073709551615");
        result->mutable_destination()->set_row_count(0);
        result->set_matches(false);
        result->mutable_finished_at()->set_seconds(100);
        NYdb::NReplication::TReplicationDescription description(proto);
        const auto actual = description.GetItems().at(0).ChecksumStatus;
        UNIT_ASSERT(actual && actual->LastResult);
        UNIT_ASSERT_VALUES_EQUAL(actual->Status, NYdb::EStatus::TIMEOUT);
        UNIT_ASSERT_VALUES_EQUAL(actual->FinishedAt, TInstant::Seconds(200));
        UNIT_ASSERT_VALUES_EQUAL(actual->LastResult->FinishedAt, TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(actual->LastResult->Source.Hash, "18446744073709551615");
        UNIT_ASSERT(actual->LastResult->Destination.Hash.empty());
        UNIT_ASSERT(!actual->LastResult->Matches);
        UNIT_ASSERT_VALUES_EQUAL(actual->Issues.Size(), 1);
    }

    Y_UNIT_TEST(ChecksumStatusPresence) {
        Ydb::Replication::DescribeReplicationResult proto;
        auto* item = proto.add_items();
        UNIT_ASSERT(!TDescription(proto).GetItems().at(0).ChecksumStatus);
        item->mutable_checksum_status()->set_status(Ydb::StatusIds::TIMEOUT);
        const auto status = TDescription(proto).GetItems().at(0).ChecksumStatus;
        UNIT_ASSERT(status);
        UNIT_ASSERT_VALUES_EQUAL(status->Status, EStatus::TIMEOUT);
        UNIT_ASSERT(!status->LastResult);
    }

    Y_UNIT_TEST(ChecksumInterval) {
        Ydb::Replication::DescribeReplicationResult proto;
        UNIT_ASSERT(!TDescription(proto).GetChecksumInterval());
        proto.mutable_checksum_interval();
        const auto disabled = TDescription(proto).GetChecksumInterval();
        UNIT_ASSERT(disabled);
        UNIT_ASSERT_VALUES_EQUAL(*disabled, TDuration::Zero());
        proto.mutable_checksum_interval()->set_seconds(3600);
        proto.mutable_checksum_interval()->set_nanos(1000000);
        const auto enabled = TDescription(proto).GetChecksumInterval();
        UNIT_ASSERT(enabled);
        UNIT_ASSERT_VALUES_EQUAL(*enabled, TDuration::MilliSeconds(3600001));
    }

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
