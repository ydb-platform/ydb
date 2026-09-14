#include <yql/essentials/providers/common/config/yql_activation_groups.h>
#include <yql/essentials/providers/common/gateways_utils/gateways_utils.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NCommon {

namespace {

TActivationGroup* AddActivationGroup(TGatewaysConfig& config, TString name = "test_group", ui32 percentage = 0) {
    auto* group = config.AddActivationGroup();
    group->SetName(name);
    group->MutableActivation()->SetPercentage(percentage);
    return group;
}

TAttr* AddDefaultSetting(TGatewaysConfig& config, TString name, TString activationGroup = {}) {
    auto* setting = config.MutableYt()->AddDefaultSettings();
    setting->SetName(name);
    setting->SetValue("true");
    if (!activationGroup.empty()) {
        setting->MutableActivation()->SetActivationGroup(activationGroup);
    }
    return setting;
}

TAttr* AddClusterSetting(TGatewaysConfig& config, TString name, TString activationGroup = {}) {
    auto* cluster = config.MutableYt()->MutableClusterMapping(0);
    auto* setting = cluster->AddSettings();
    setting->SetName(name);
    setting->SetValue("true");
    if (!activationGroup.empty()) {
        setting->MutableActivation()->SetActivationGroup(activationGroup);
    }
    return setting;
}

void AddYtCluster(TGatewaysConfig& config) {
    auto* cluster = config.MutableYt()->AddClusterMapping();
    cluster->SetName("hahn");
}

TAttr* AddDqSetting(TGatewaysConfig& config, TString name, TString activationGroup) {
    auto* setting = config.MutableDq()->AddDefaultSettings();
    setting->SetName(name);
    setting->SetValue("true");
    setting->MutableActivation()->SetActivationGroup(activationGroup);
    return setting;
}

NProto::TRuntimeSetting* AddRuntimeSetting(TGatewaysConfig& config, TString name, TString activationGroup) {
    auto* setting = config.MutableRuntimeSettings()->AddHostSettings();
    setting->SetName(name);
    setting->SetValue("true");
    setting->MutableActivation()->SetActivationGroup(activationGroup);
    return setting;
}

TCoreAttr* AddCoreFlag(
    google::protobuf::RepeatedPtrField<TCoreAttr>* flags,
    TString name,
    TString activationGroup)
{
    auto* flag = flags->Add();
    flag->SetName(name);
    flag->MutableActivation()->SetActivationGroup(activationGroup);
    return flag;
}

template <typename TSetting>
void AssertActivatedMarker(const TSetting& setting) {
    UNIT_ASSERT(setting.HasActivation());
    UNIT_ASSERT(!setting.GetActivation().HasActivationGroup());
    UNIT_ASSERT_VALUES_EQUAL(100U, setting.GetActivation().GetPercentage());
    UNIT_ASSERT(!setting.GetActivation().GetExcludeRobots());
}

TVector<TString> ApplyActivationGroupsInplace(TGatewaysConfig& config, const TString& username = {}) {
    return ::NYql::NCommon::ApplyActivationGroupsInplace(config, username, TCredentials::TPtr{}, TQContext{});
}

void AssertRejectedWithoutMutation(TGatewaysConfig& config, TStringBuf message) {
    const auto serialized = config.SerializePartialAsString();
    UNIT_ASSERT_EXCEPTION_CONTAINS(ApplyActivationGroupsInplace(config), yexception, message);
    UNIT_ASSERT_VALUES_EQUAL(serialized, config.SerializePartialAsString());
}

} // namespace

Y_UNIT_TEST_SUITE(TActivationGroupsTest) {

Y_UNIT_TEST(AppliesSelectedGroupsAndRemovesRejectedGroups) {
    TGatewaysConfig config;
    AddYtCluster(config);
    AddActivationGroup(config, "selected")->MutableActivation()->AddIncludeUsers("robot");
    AddActivationGroup(config, "rejected")->MutableActivation()->SetExcludeRobots(false);
    AddActivationGroup(config, "unused", 100)->MutableActivation()->SetExcludeRobots(false);

    AddDefaultSetting(config, "FirstDirectSetting");
    AddDefaultSetting(config, "RejectedSetting", "rejected");
    AddDefaultSetting(config, "SelectedSetting", "selected");
    auto* directSetting = AddDefaultSetting(config, "DirectActivationSetting");
    directSetting->MutableActivation()->SetPercentage(17);
    AddDefaultSetting(config, "LastDirectSetting");
    AddClusterSetting(config, "RejectedClusterSetting", "rejected");
    AddClusterSetting(config, "SelectedClusterSetting", "selected");
    AddDqSetting(config, "RejectedDqSetting", "rejected");
    AddDqSetting(config, "SelectedDqSetting", "selected");
    AddRuntimeSetting(config, "RejectedRuntimeSetting", "rejected");
    AddRuntimeSetting(config, "SelectedRuntimeSetting", "selected");
    AddCoreFlag(config.MutableYqlCore()->MutableFlags(), "RejectedCoreFlag", "rejected");
    AddCoreFlag(config.MutableYqlCore()->MutableFlags(), "SelectedCoreFlag", "selected");
    AddCoreFlag(config.MutableSqlCore()->MutableExtendedTranslationFlags(), "RejectedSqlFlag", "rejected");
    AddCoreFlag(config.MutableSqlCore()->MutableExtendedTranslationFlags(), "SelectedSqlFlag", "selected");

    auto credentials = MakeIntrusive<TCredentials>();
    credentials->SetIsRobot(true);

    const auto activatedGroups = ::NYql::NCommon::ApplyActivationGroupsInplace(
        config,
        "robot",
        credentials,
        TQContext{});

    UNIT_ASSERT_VALUES_EQUAL(TVector<TString>{"selected"}, activatedGroups);
    UNIT_ASSERT_VALUES_EQUAL(0, config.ActivationGroupSize());
    UNIT_ASSERT_VALUES_EQUAL(4, config.GetYt().DefaultSettingsSize());
    UNIT_ASSERT_VALUES_EQUAL("FirstDirectSetting", config.GetYt().GetDefaultSettings(0).GetName());
    UNIT_ASSERT_VALUES_EQUAL("SelectedSetting", config.GetYt().GetDefaultSettings(1).GetName());
    AssertActivatedMarker(config.GetYt().GetDefaultSettings(1));
    UNIT_ASSERT_VALUES_EQUAL("DirectActivationSetting", config.GetYt().GetDefaultSettings(2).GetName());
    UNIT_ASSERT_VALUES_EQUAL(17U, config.GetYt().GetDefaultSettings(2).GetActivation().GetPercentage());
    UNIT_ASSERT_VALUES_EQUAL("LastDirectSetting", config.GetYt().GetDefaultSettings(3).GetName());
    UNIT_ASSERT_VALUES_EQUAL(1, config.GetYt().GetClusterMapping(0).SettingsSize());
    UNIT_ASSERT_VALUES_EQUAL("SelectedClusterSetting", config.GetYt().GetClusterMapping(0).GetSettings(0).GetName());
    AssertActivatedMarker(config.GetYt().GetClusterMapping(0).GetSettings(0));
    UNIT_ASSERT_VALUES_EQUAL(1, config.GetDq().DefaultSettingsSize());
    UNIT_ASSERT_VALUES_EQUAL("SelectedDqSetting", config.GetDq().GetDefaultSettings(0).GetName());
    AssertActivatedMarker(config.GetDq().GetDefaultSettings(0));
    UNIT_ASSERT_VALUES_EQUAL(1, config.GetRuntimeSettings().HostSettingsSize());
    UNIT_ASSERT_VALUES_EQUAL("SelectedRuntimeSetting", config.GetRuntimeSettings().GetHostSettings(0).GetName());
    AssertActivatedMarker(config.GetRuntimeSettings().GetHostSettings(0));
    UNIT_ASSERT_VALUES_EQUAL(1, config.GetYqlCore().FlagsSize());
    UNIT_ASSERT_VALUES_EQUAL("SelectedCoreFlag", config.GetYqlCore().GetFlags(0).GetName());
    AssertActivatedMarker(config.GetYqlCore().GetFlags(0));
    UNIT_ASSERT_VALUES_EQUAL(1, config.GetSqlCore().ExtendedTranslationFlagsSize());
    UNIT_ASSERT_VALUES_EQUAL("SelectedSqlFlag", config.GetSqlCore().GetExtendedTranslationFlags(0).GetName());
    AssertActivatedMarker(config.GetSqlCore().GetExtendedTranslationFlags(0));
}

Y_UNIT_TEST(SqlCoreGroupsAreResolvedBeforeCommonPreprocessing) {
    TGatewaysConfig config;
    AddActivationGroup(config, "selected", 100);
    AddActivationGroup(config, "rejected", 0);
    AddCoreFlag(config.MutableSqlCore()->MutableExtendedTranslationFlags(), "SelectedSqlFlag", "selected");
    AddCoreFlag(config.MutableSqlCore()->MutableExtendedTranslationFlags(), "RejectedSqlFlag", "rejected");

    const auto flags = TGatewaySQLFlags::From(
                           config,
                           [](const TActivationPercentage& activation) {
                               return activation.GetPercentage() == 100;
                           })
                           .ToMap();

    UNIT_ASSERT(flags.contains("SelectedSqlFlag"));
    UNIT_ASSERT(!flags.contains("RejectedSqlFlag"));
}

Y_UNIT_TEST(RejectsInvalidConfigurationWithoutMutation) {
    {
        TGatewaysConfig config;
        config.AddActivationGroup()->MutableActivation()->SetPercentage(10);
        AssertRejectedWithoutMutation(config, "Activation group name must not be empty");
    }
    {
        TGatewaysConfig config;
        config.AddActivationGroup()->SetName("missing_activation");
        AssertRejectedWithoutMutation(config, "has no activation");
    }
    {
        TGatewaysConfig config;
        AddActivationGroup(config, "duplicate", 10);
        AddActivationGroup(config, "duplicate", 10);
        AssertRejectedWithoutMutation(config, "Duplicate activation group 'duplicate'");
    }
    {
        TGatewaysConfig config;
        AddDefaultSetting(config, "Setting", "missing");
        AssertRejectedWithoutMutation(config, "Unknown activation group 'missing'");
    }
    {
        TGatewaysConfig config;
        AddActivationGroup(config, "test_group", 100);
        auto* setting = AddDefaultSetting(config, "Setting", "test_group");
        setting->MutableActivation()->SetPercentage(10);
        AssertRejectedWithoutMutation(config, "cannot be combined with a direct activation");
    }
    {
        TGatewaysConfig config;
        auto* group = AddActivationGroup(config, "nested");
        group->MutableActivation()->SetActivationGroup("other");
        AssertRejectedWithoutMutation(config, "cannot reference another activation group");
    }
}

} // Y_UNIT_TEST_SUITE(TActivationGroupsTest)

} // namespace NYql::NCommon
