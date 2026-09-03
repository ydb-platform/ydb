#include "resource_pool_classifier_settings.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/aclib/aclib.h>


namespace NKikimr {

using namespace NResourcePool;


Y_UNIT_TEST_SUITE(ResourcePoolClassifierTest) {
    Y_UNIT_TEST(SettingsValidationMissingPoolAndAction) {
        TClassifierSettings settings;
        // Neither ResourcePool nor Action set — should fail
        UNIT_ASSERT(settings.Validate().has_value());
        UNIT_ASSERT_STRING_CONTAINS(*settings.Validate(), "either resource pool or action must be specified");
    }

    Y_UNIT_TEST(SettingsValidationBothPoolAndAction) {
        TClassifierSettings settings;
        settings.ResourcePool = "some_pool";
        settings.Action = EClassifierAction::Reject;
        // Both ResourcePool and Action set — should fail
        UNIT_ASSERT(settings.Validate().has_value());
        UNIT_ASSERT_STRING_CONTAINS(*settings.Validate(), "resource pool must not be used for Reject action");
    }

    Y_UNIT_TEST(SettingsValidationSystemUser) {
        TClassifierSettings settings;
        settings.ResourcePool = "some_pool";
        settings.MemberName = BUILTIN_ACL_METADATA;
        UNIT_ASSERT(settings.Validate().has_value());
        UNIT_ASSERT_STRING_CONTAINS(*settings.Validate(), TStringBuilder() << "cannot create classifier for system user " << settings.MemberName);
    }

    Y_UNIT_TEST(SettingsValidationOkWithPool) {
        TClassifierSettings settings;
        settings.ResourcePool = "some_pool";
        UNIT_ASSERT(!settings.Validate().has_value());
    }

    Y_UNIT_TEST(SettingsValidationOkWithAction) {
        TClassifierSettings settings;
        settings.Action = EClassifierAction::Reject;
        UNIT_ASSERT(!settings.Validate().has_value());
    }
}

}  // namespace NKikimr
