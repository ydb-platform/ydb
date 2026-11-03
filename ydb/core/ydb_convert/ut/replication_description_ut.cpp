#include <ydb/core/ydb_convert/replication_description.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/draft/ydb_replication.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(ReplicationDescription) {
    Y_UNIT_TEST(AvailabilityPeriodPresence) {
        NKikimrReplication::TEvDescribeReplicationResult controller;
        NKikimrSchemeOp::TReplicationDescription scheme;
        scheme.MutableConfig()->MutableSpecific();
        const auto check = [&] {
            Ydb::Replication::DescribeReplicationResult fromController;
            FillReplicationDescription(fromController, controller);
            Ydb::Replication::DescribeReplicationResult fromScheme;
            Ydb::StatusIds_StatusCode status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT_C(FillReplicationDescription(fromScheme, scheme, NKikimrSchemeOp::TDirEntry(), status, error), error);
            UNIT_ASSERT_VALUES_EQUAL(fromController.has_availability_period(), controller.HasAvailabilityPeriod());
            UNIT_ASSERT_VALUES_EQUAL(fromScheme.has_availability_period(), scheme.GetConfig().HasAvailabilityPeriod());
            UNIT_ASSERT_VALUES_EQUAL(fromController.availability_period().SerializeAsString(),
                fromScheme.availability_period().SerializeAsString());
            return fromController;
        };
        UNIT_ASSERT(!check().has_availability_period());
        controller.MutableAvailabilityPeriod()->MutableUnlimited();
        scheme.MutableConfig()->MutableAvailabilityPeriod()->CopyFrom(controller.GetAvailabilityPeriod());
        UNIT_ASSERT(check().availability_period().has_unlimited());
        auto& duration = *controller.MutableAvailabilityPeriod()->MutableDuration();
        duration.set_seconds(10);
        duration.set_nanos(1000000);
        scheme.MutableConfig()->MutableAvailabilityPeriod()->CopyFrom(controller.GetAvailabilityPeriod());
        UNIT_ASSERT_VALUES_EQUAL(check().availability_period().duration().SerializeAsString(), duration.SerializeAsString());
        controller.MutableAvailabilityPeriod()->Clear();
        scheme.MutableConfig()->MutableAvailabilityPeriod()->Clear();
        Ydb::Replication::DescribeReplicationResult reused;
        reused.mutable_availability_period()->mutable_unlimited();
        NKikimrReplication::TEvDescribeReplicationResult legacyController;
        FillReplicationDescription(reused, legacyController);
        UNIT_ASSERT(!reused.has_availability_period());
        reused.mutable_availability_period()->mutable_unlimited();
        NKikimrSchemeOp::TReplicationDescription legacyScheme;
        legacyScheme.MutableConfig()->MutableSpecific();
        Ydb::StatusIds_StatusCode status = Ydb::StatusIds::SUCCESS;
        TString error;
        UNIT_ASSERT(FillReplicationDescription(reused, legacyScheme, NKikimrSchemeOp::TDirEntry(), status, error));
        UNIT_ASSERT(!reused.has_availability_period());
        const auto invalid = check();
        UNIT_ASSERT(invalid.has_availability_period());
        UNIT_ASSERT(invalid.availability_period().value_case() == Ydb::Replication::AvailabilityPeriod::VALUE_NOT_SET);
    }
}
