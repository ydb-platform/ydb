#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TRateLimiterResourceValidation) {
    Y_UNIT_TEST(NoMeteringDoesNotRequirePermission) {
        Ydb::RateLimiter::Resource resource;
        resource.set_resource_path("a");
        resource.mutable_hierarchical_drr()->set_max_units_per_second(10);

        Ydb::StatusIds::StatusCode status;
        TString error;
        UNIT_ASSERT(ValidateRateLimiterResourceProperties(resource, false, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(MissingTypeIsReportedBeforeMeteringPermission) {
        Ydb::RateLimiter::Resource resource;
        resource.mutable_metering_config()->set_enabled(false);

        Ydb::StatusIds::StatusCode status;
        TString error;
        UNIT_ASSERT(!ValidateRateLimiterResourceProperties(resource, false, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(error, "No resource properties.");
    }

    Y_UNIT_TEST(DisabledMeteringStillRequiresPermissionBeforeCheckingMetrics) {
        Ydb::RateLimiter::Resource resource;
        resource.mutable_hierarchical_drr();
        auto* metering = resource.mutable_metering_config();
        metering->set_enabled(false);
        auto& fields = *metering->mutable_provisioned()->mutable_metric_fields()->mutable_fields();
        fields["cloud_id"].set_number_value(1);

        Ydb::StatusIds::StatusCode status;
        TString error;
        UNIT_ASSERT(!ValidateRateLimiterResourceProperties(resource, false, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(error, "Setting metering is allowed only for administrators");

        fields["cloud_id"].set_string_value("cloud");
        UNIT_ASSERT(ValidateRateLimiterResourceProperties(resource, true, status, error));
        UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(error.empty());
    }

    Y_UNIT_TEST(AllMetricKindsRejectFieldsThatConversionWouldDiscard) {
        Ydb::RateLimiter::Resource resource;
        resource.mutable_hierarchical_drr();
        auto* metering = resource.mutable_metering_config();
        for (auto* metric : {metering->mutable_provisioned(), metering->mutable_on_demand(), metering->mutable_overshoot()}) {
            auto& fields = *metric->mutable_metric_fields()->mutable_fields();
            Ydb::StatusIds::StatusCode status;
            TString error;

            fields["unknown"].set_string_value("value");
            UNIT_ASSERT(!ValidateRateLimiterResourceProperties(resource, true, status, error));
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(error, "Unsupported key for metric. Key: unknown.");

            fields.clear();
            fields["cloud_id"].set_number_value(1);
            UNIT_ASSERT(!ValidateRateLimiterResourceProperties(resource, true, status, error));
            UNIT_ASSERT_VALUES_EQUAL(status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(error, "Unsupported type for metric. Key: cloud_id.");

            fields["cloud_id"].set_string_value("cloud");
            UNIT_ASSERT(ValidateRateLimiterResourceProperties(resource, true, status, error));
        }
    }
}

} // namespace NKikimr
