#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NCommon {

namespace {

class TTestSettings {
public:
    TConfSetting<TString, EConfSettingType::Dynamic> MySetting;
    TConfSetting<TString, EConfSettingType::Static> StaticString;
    TConfSetting<i64, EConfSettingType::Dynamic> Integer;
    TConfSetting<TString, EConfSettingType::StaticPerCluster> PerClusterString;
    TConfSetting<TString, EConfSettingType::Dynamic> OtherString;
    TConfSetting<TString, EConfSettingType::Dynamic> ThirdString;
    TConfSetting<i64, EConfSettingType::Static> StaticInteger;
    TConfSetting<bool, EConfSettingType::Static> StaticBoolean;
    TConfSetting<TString, EConfSettingType::Static> OtherStaticString;
    TConfSetting<TString, EConfSettingType::Static> ThirdStaticString;
};

struct TTestErrorCollector {
    TString LastMessage;
    bool LastIsError = false;
    bool ShouldContinue = true;

    ISettingDispatcher::TErrorCallback MakeCallback() {
        return [this](const TString& msg, bool isError) -> bool {
            LastMessage = msg;
            LastIsError = isError;
            return ShouldContinue;
        };
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TConfSettingTest) {

Y_UNIT_TEST(DynamicSettingEmpty) {
    TConfSetting<TString> setting;
    UNIT_ASSERT(!setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT(!setting.Get("cluster1"));
    UNIT_ASSERT(setting.IsRuntime());
    UNIT_ASSERT(setting.IsPerCluster());
}

Y_UNIT_TEST(DynamicSettingAllClusters) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "global";
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster1"));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("any_other"));
}

Y_UNIT_TEST(DynamicSettingPerClusterOverride) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "global";
    setting["cluster1"] = "local1";
    UNIT_ASSERT_VALUES_EQUAL("local1", *setting.Get("cluster1"));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster2"));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(DynamicSettingAllClustersClears) {
    TConfSetting<TString> setting;
    setting["cluster1"] = "local";
    setting[ALL_CLUSTERS] = "global";
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingAssignClears) {
    TConfSetting<TString> setting;
    setting["cluster1"] = "local";
    setting = TString("global");
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingClearAll) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "val";
    setting["cluster1"] = "local";
    setting.Clear();
    UNIT_ASSERT(!setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT(!setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingClearCluster) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "global";
    setting["cluster1"] = "local";
    setting.Clear("cluster1");
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingClearAllClustersKey) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "global";
    setting["cluster1"] = "local";
    setting.Clear(ALL_CLUSTERS);
    UNIT_ASSERT(!setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT(!setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingUpdateAll) {
    TConfSetting<TString> setting;
    setting[ALL_CLUSTERS] = "base";
    setting["cluster1"] = "cl1";
    setting.UpdateAll([](const TString&, TString& val) { val += "_suffix"; });
    UNIT_ASSERT_VALUES_EQUAL("base_suffix", *setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("cl1_suffix", *setting.Get("cluster1"));
}

Y_UNIT_TEST(DynamicSettingUpdateAllInsertsAllClusters) {
    TConfSetting<TString> setting;
    setting["cluster1"] = "cl1";
    setting.UpdateAll([](const TString&, TString& val) { val += "_x"; });
    UNIT_ASSERT(setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(StaticSettingBasic) {
    TConfSetting<TString, EConfSettingType::Static> setting;
    UNIT_ASSERT(!setting.Get());
    UNIT_ASSERT(!setting.IsRuntime());
    UNIT_ASSERT(!setting.IsPerCluster());

    setting[ALL_CLUSTERS] = "value";
    UNIT_ASSERT_VALUES_EQUAL("value", *setting.Get());
}

Y_UNIT_TEST(StaticSettingAssign) {
    TConfSetting<TString, EConfSettingType::Static> setting;
    setting = TString("assigned");
    UNIT_ASSERT_VALUES_EQUAL("assigned", *setting.Get());
}

Y_UNIT_TEST(StaticSettingClusterThrows) {
    TConfSetting<TString, EConfSettingType::Static> setting;
    UNIT_ASSERT_EXCEPTION(setting["cluster1"], yexception);
}

Y_UNIT_TEST(StaticSettingClear) {
    TConfSetting<TString, EConfSettingType::Static> setting;
    setting = TString("val");
    setting.Clear();
    UNIT_ASSERT(!setting.Get());
}

Y_UNIT_TEST(StaticPerClusterSettingBasic) {
    TConfSetting<TString, EConfSettingType::StaticPerCluster> setting;
    UNIT_ASSERT(!setting.IsRuntime());
    UNIT_ASSERT(setting.IsPerCluster());

    setting[ALL_CLUSTERS] = "global";
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster1"));

    setting["cluster1"] = "local";
    UNIT_ASSERT_VALUES_EQUAL("local", *setting.Get("cluster1"));
    UNIT_ASSERT_VALUES_EQUAL("global", *setting.Get("cluster2"));
}

} // Y_UNIT_TEST_SUITE(TConfSettingTest)

Y_UNIT_TEST_SUITE(TSettingDispatcherTest) {

Y_UNIT_TEST(AddAndDispatchConfig) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.MySetting;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("hello"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("hello", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(AddDuplicateThrows) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);
    UNIT_ASSERT_EXCEPTION(dispatcher.AddSetting("MySetting", &TTestSettings::MySetting), yexception);
}

Y_UNIT_TEST(UnknownSettingAtConfigIgnored) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(errors.LastMessage.empty());
}

Y_UNIT_TEST(UnknownSettingAtStaticErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown setting name"));
}

Y_UNIT_TEST(UnknownSettingAtRuntimeErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), ISettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown setting name"));
}

Y_UNIT_TEST(StaticSettingWithSpecificClusterErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("StaticSetting", &TTestSettings::StaticString);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch("cluster1", "StaticSetting", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be set for specific cluster"));
}

Y_UNIT_TEST(InvalidClusterErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch("invalid_cluster", "MySetting", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown cluster name"));
}

Y_UNIT_TEST(NullValueForStaticSettingErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("StaticSetting", &TTestSettings::StaticString);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "StaticSetting", Nothing(), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be reset to default"));
}

Y_UNIT_TEST(NullValueForDynamicSettingWithoutFreezeErrors) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", Nothing(), ISettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Cannot restore"));
}

Y_UNIT_TEST(NullValueForDynamicSettingRestoresDefault) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.MySetting;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);

    setting[ALL_CLUSTERS] = "original";
    dispatcher.FreezeDefaults();

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("changed"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("changed", *setting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", Nothing(), ISettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("original", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(FreezeAndRestore) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& s1 = dispatcher.MySetting;
    auto& s2 = dispatcher.Integer;
    dispatcher.AddSetting("S1", &TTestSettings::MySetting);
    dispatcher.AddSetting("S2", &TTestSettings::Integer);

    s1[ALL_CLUSTERS] = "initial";
    s2[ALL_CLUSTERS] = 42;
    dispatcher.FreezeDefaults();

    s1[ALL_CLUSTERS] = "modified";
    s2[ALL_CLUSTERS] = 99;
    UNIT_ASSERT_VALUES_EQUAL("modified", *s1.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL(99, *s2.Get(ALL_CLUSTERS));

    dispatcher.Restore();
    UNIT_ASSERT_VALUES_EQUAL("initial", *s1.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL(42, *s2.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(IsRuntime) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("Dynamic", &TTestSettings::MySetting);
    dispatcher.AddSetting("Static", &TTestSettings::StaticString);
    dispatcher.AddSetting("StaticPerCluster", &TTestSettings::PerClusterString);

    UNIT_ASSERT(dispatcher.IsRuntime("Dynamic"));
    UNIT_ASSERT(!dispatcher.IsRuntime("Static"));
    UNIT_ASSERT(!dispatcher.IsRuntime("StaticPerCluster"));
    UNIT_ASSERT(!dispatcher.IsRuntime("Unknown"));
}

Y_UNIT_TEST(StageStaticAppliesOnlyNonRuntime) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& dynamicSetting = dispatcher.MySetting;
    auto& staticSetting = dispatcher.StaticString;
    dispatcher.AddSetting("Dynamic", &TTestSettings::MySetting);
    dispatcher.AddSetting("Static", &TTestSettings::StaticString);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!dynamicSetting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("val", *staticSetting.Get());
}

Y_UNIT_TEST(StageRuntimeAppliesOnlyRuntime) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& dynamicSetting = dispatcher.MySetting;
    auto& staticSetting = dispatcher.StaticString;
    dispatcher.AddSetting("Dynamic", &TTestSettings::MySetting);
    dispatcher.AddSetting("Static", &TTestSettings::StaticString);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("dyn_val"), ISettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("dyn_val", *dynamicSetting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("sta_val"), ISettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!staticSetting.Get());
}

Y_UNIT_TEST(StageConfigAlwaysAppliesBoth) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& dynamicSetting = dispatcher.MySetting;
    auto& staticSetting = dispatcher.StaticString;
    dispatcher.AddSetting("Dynamic", &TTestSettings::MySetting);
    dispatcher.AddSetting("Static", &TTestSettings::StaticString);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("dyn"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("sta"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("dyn", *dynamicSetting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("sta", *staticSetting.Get());
}

Y_UNIT_TEST(CaseInsensitiveSettingNames) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.MySetting;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "mysetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("val", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorLower) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.Integer;
    dispatcher.AddSetting("MySetting", &TTestSettings::Integer).Lower(0LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("-1"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("less than"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("0"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(0L, *setting.Get(ALL_CLUSTERS));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("5"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(5L, *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorUpper) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.Integer;
    dispatcher.AddSetting("MySetting", &TTestSettings::Integer).Upper(100LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("200"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("greater than"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("100"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(100L, *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorLowerAndUpperBothApply) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::Integer).Lower(0LL).Upper(10LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("-1"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("11"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("5"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
}

Y_UNIT_TEST(ValidatorEnumInitializerList) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.MySetting;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Enum({"a", "b", "c"});

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("d"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("not in set of allowed values"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("b"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("b", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorEnumContainer) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    TVector<TString> allowed = {"x", "y"};
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Enum(allowed);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("z"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("x"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
}

Y_UNIT_TEST(ValidatorNonEmpty) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).NonEmpty();

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString(""), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("empty"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("nonempty"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(ValidatorGlobalOnly) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).GlobalOnly();
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch("cluster1", "MySetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be used with specific cluster"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(ValidatorCustom) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Validator([](const TTestSettings&, const TString&, const TString& val) {
        if (val == "forbidden") {
            throw yexception() << "Value is forbidden";
        }
    });

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("forbidden"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("forbidden"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("allowed"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(WarningCallback) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.MySetting;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Warning("Use new setting instead");

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(!errors.LastIsError);
    UNIT_ASSERT_VALUES_EQUAL("Use new setting instead", errors.LastMessage);
    UNIT_ASSERT_VALUES_EQUAL("val", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(DeprecatedSettingProducesWarning) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Deprecated();

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(!errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("deprecated"));
}

Y_UNIT_TEST(DeprecatedSettingCustomMessage) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).Deprecated("Please use NewSetting instead");

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("Please use NewSetting instead", errors.LastMessage);
}

Y_UNIT_TEST(EnumerateSkipsDeprecatedAndUnderscoreNames) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("Normal", &TTestSettings::MySetting);
    dispatcher.AddSetting("DeprecatedOne", &TTestSettings::OtherString).Deprecated();
    dispatcher.AddSetting("_HiddenInternal", &TTestSettings::ThirdString);

    TVector<TString> names;
    dispatcher.Enumerate([&](std::string_view name) { names.emplace_back(name); });

    UNIT_ASSERT(Find(names, "Normal") != names.end());
    UNIT_ASSERT(Find(names, "DeprecatedOne") == names.end());
    UNIT_ASSERT(Find(names, "_HiddenInternal") == names.end());
    UNIT_ASSERT_VALUES_EQUAL(1U, names.size());
}

Y_UNIT_TEST(ValidClustersManagement) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    UNIT_ASSERT(!dispatcher.IsValidCluster("cluster1"));

    dispatcher.AddValidCluster("cluster1");
    dispatcher.AddValidCluster("cluster2");
    UNIT_ASSERT(dispatcher.IsValidCluster("cluster1"));
    UNIT_ASSERT(dispatcher.IsValidCluster("cluster2"));
    UNIT_ASSERT(!dispatcher.IsValidCluster("cluster3"));
    UNIT_ASSERT_VALUES_EQUAL(2U, dispatcher.GetValidClusters().size());
}

Y_UNIT_TEST(SetValidClusters) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddValidCluster("old_cluster");
    UNIT_ASSERT(dispatcher.IsValidCluster("old_cluster"));

    TVector<TString> newClusters = {"new1", "new2"};
    dispatcher.SetValidClusters(newClusters);
    UNIT_ASSERT(!dispatcher.IsValidCluster("old_cluster"));
    UNIT_ASSERT(dispatcher.IsValidCluster("new1"));
    UNIT_ASSERT(dispatcher.IsValidCluster("new2"));
}

Y_UNIT_TEST(ConstructorWithValidClusters) {
    TVector<TString> clusters = {"c1", "c2", "c3"};
    TDispatcherAndSettings<TTestSettings> dispatcher(clusters);
    UNIT_ASSERT(dispatcher.IsValidCluster("c1"));
    UNIT_ASSERT(dispatcher.IsValidCluster("c2"));
    UNIT_ASSERT(!dispatcher.IsValidCluster("c4"));
}

Y_UNIT_TEST(LevenshteinSuggestionForCluster) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    dispatcher.Dispatch("clusteR1", "MySetting", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(errors.LastMessage.Contains("did you mean"));
    UNIT_ASSERT(errors.LastMessage.Contains("cluster1"));
}

Y_UNIT_TEST(LevenshteinSuggestionForSettingName) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySettnig", TString("val"), ISettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(errors.LastMessage.Contains("did you mean"));
    UNIT_ASSERT(errors.LastMessage.Contains("MySetting"));
}

Y_UNIT_TEST(CustomValueSetter) {
    TDispatcher<TTestSettings> dispatcher;
    TString captured;
    dispatcher.AddSetting("MySetting", &TTestSettings::MySetting).ValueSetter([&captured](TTestSettings& settings, const TString&, TString val) {
        captured = "custom_" + val;
        settings.MySetting[ALL_CLUSTERS] = captured;
    });

    TTestSettings firstSettings;
    TTestSettings secondSettings;
    const auto& configuredDispatcher = dispatcher;
    TTestErrorCollector errors;
    UNIT_ASSERT(configuredDispatcher.Dispatch(firstSettings, ALL_CLUSTERS, "MySetting", TString("hello"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback(), {}));
    UNIT_ASSERT_VALUES_EQUAL("custom_hello", *firstSettings.MySetting.Get(ALL_CLUSTERS));
    UNIT_ASSERT(!secondSettings.MySetting.Get(ALL_CLUSTERS));

    UNIT_ASSERT(configuredDispatcher.Dispatch(secondSettings, ALL_CLUSTERS, "MySetting", TString(), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback(), {}));
    UNIT_ASSERT_VALUES_EQUAL("custom_", *secondSettings.MySetting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("custom_hello", *firstSettings.MySetting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(RegisterSettingMacro) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& MySetting = dispatcher.MySetting;
    REGISTER_SETTING(dispatcher, MySetting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("macro_test"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("macro_test", *MySetting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(UnknownSettingAtConfigErrorsInStrictMode) {
    TDispatcherAndSettings<TTestSettings> dispatcher("", {}, /*strictConfigValidation=*/true);
    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown setting name"));
}

Y_UNIT_TEST(StrictModeDefaultsToFalse) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), ISettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(errors.LastMessage.empty());
}

} // Y_UNIT_TEST_SUITE(TSettingDispatcherTest)

Y_UNIT_TEST_SUITE(TDefaultParserTest) {

Y_UNIT_TEST(ParseString) {
    auto parser = NPrivate::GetDefaultParser<TString>();
    UNIT_ASSERT_VALUES_EQUAL("hello world", parser("hello world"));
    UNIT_ASSERT_VALUES_EQUAL("", parser(""));
}

Y_UNIT_TEST(ParseBoolTrueAndFalse) {
    auto parser = NPrivate::GetDefaultParser<bool>();
    UNIT_ASSERT_VALUES_EQUAL(true, parser("true"));
    UNIT_ASSERT_VALUES_EQUAL(true, parser("1"));
    UNIT_ASSERT_VALUES_EQUAL(false, parser("false"));
    UNIT_ASSERT_VALUES_EQUAL(false, parser("0"));
}

Y_UNIT_TEST(ParseBoolEmptyIsTrue) {
    auto parser = NPrivate::GetDefaultParser<bool>();
    UNIT_ASSERT_VALUES_EQUAL(true, parser(""));
}

Y_UNIT_TEST(ParseBoolInvalidThrows) {
    auto parser = NPrivate::GetDefaultParser<bool>();
    UNIT_ASSERT_EXCEPTION(parser("maybe"), yexception);
    UNIT_ASSERT_EXCEPTION(parser("2"), yexception);
}

Y_UNIT_TEST(ParseGuidRoundTrip) {
    TGUID original;
    CreateGuid(&original);
    TString guidStr = GetGuidAsString(original);
    auto parser = NPrivate::GetDefaultParser<TGUID>();
    TGUID parsed = parser(guidStr);
    UNIT_ASSERT_VALUES_EQUAL(GetGuidAsString(original), GetGuidAsString(parsed));
}

Y_UNIT_TEST(ParseGuidInvalidThrows) {
    auto parser = NPrivate::GetDefaultParser<TGUID>();
    UNIT_ASSERT_EXCEPTION(parser("not-a-guid"), yexception);
}

Y_UNIT_TEST(ParseUnsignedIntegers) {
    UNIT_ASSERT_VALUES_EQUAL(42U, NPrivate::GetDefaultParser<ui8>()("42"));
    UNIT_ASSERT_VALUES_EQUAL(1000U, NPrivate::GetDefaultParser<ui16>()("1000"));
    UNIT_ASSERT_VALUES_EQUAL(100000U, NPrivate::GetDefaultParser<ui32>()("100000"));
    UNIT_ASSERT_VALUES_EQUAL(1000000000000ULL, NPrivate::GetDefaultParser<ui64>()("1000000000000"));
}

Y_UNIT_TEST(ParseSignedIntegers) {
    UNIT_ASSERT_VALUES_EQUAL(-5, NPrivate::GetDefaultParser<i8>()("-5"));
    UNIT_ASSERT_VALUES_EQUAL(-1000, NPrivate::GetDefaultParser<i16>()("-1000"));
    UNIT_ASSERT_VALUES_EQUAL(-100000, NPrivate::GetDefaultParser<i32>()("-100000"));
    UNIT_ASSERT_VALUES_EQUAL(-1000000000000LL, NPrivate::GetDefaultParser<i64>()("-1000000000000"));
}

Y_UNIT_TEST(ParseIntegerInvalidThrows) {
    UNIT_ASSERT_EXCEPTION(NPrivate::GetDefaultParser<ui32>()("abc"), yexception);
    UNIT_ASSERT_EXCEPTION(NPrivate::GetDefaultParser<ui32>()(""), yexception);
}

Y_UNIT_TEST(ParseFloatingPoint) {
    UNIT_ASSERT_DOUBLES_EQUAL(3.14, NPrivate::GetDefaultParser<double>()("3.14"), 1e-9);
    UNIT_ASSERT_DOUBLES_EQUAL(1.5F, NPrivate::GetDefaultParser<float>()("1.5"), 1e-5);
}

Y_UNIT_TEST(ParseDuration) {
    auto parser = NPrivate::GetDefaultParser<TDuration>();
    UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(1), parser("1s"));
    UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(500), parser("500ms"));
    UNIT_ASSERT_VALUES_EQUAL(TDuration::Minutes(2), parser("2m"));
}

Y_UNIT_TEST(ParseInstantValid) {
    auto parser = NPrivate::GetDefaultParser<TInstant>();
    TInstant t = parser("2023-01-15T10:30:00Z");
    Y_UNUSED(t);
}

Y_UNIT_TEST(ParseInstantInvalidThrows) {
    auto parser = NPrivate::GetDefaultParser<TInstant>();
    UNIT_ASSERT_EXCEPTION(parser("not-a-date"), yexception);
    UNIT_ASSERT_EXCEPTION(parser(""), yexception);
}

Y_UNIT_TEST(ParseSize) {
    auto parser = NPrivate::GetDefaultParser<NSize::TSize>();
    NSize::TSize sz = parser("1k");
    Y_UNUSED(sz);
    sz = parser("2m");
    Y_UNUSED(sz);
}

Y_UNIT_TEST(ParseVectorStringComma) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("a,b,c");
    UNIT_ASSERT_VALUES_EQUAL(3U, result.size());
    UNIT_ASSERT_VALUES_EQUAL("a", result[0]);
    UNIT_ASSERT_VALUES_EQUAL("b", result[1]);
    UNIT_ASSERT_VALUES_EQUAL("c", result[2]);
}

Y_UNIT_TEST(ParseVectorStringSemicolon) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("x;y;z");
    UNIT_ASSERT_VALUES_EQUAL(3U, result.size());
}

Y_UNIT_TEST(ParseVectorStringPipe) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("foo|bar");
    UNIT_ASSERT_VALUES_EQUAL(2U, result.size());
    UNIT_ASSERT_VALUES_EQUAL("foo", result[0]);
    UNIT_ASSERT_VALUES_EQUAL("bar", result[1]);
}

Y_UNIT_TEST(ParseVectorStringSpace) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("hello world");
    UNIT_ASSERT_VALUES_EQUAL(2U, result.size());
}

Y_UNIT_TEST(ParseVectorStringEmptyItemThrows) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    UNIT_ASSERT_EXCEPTION(parser("a,,b"), yexception);
    UNIT_ASSERT_EXCEPTION(parser(",a"), yexception);
    UNIT_ASSERT_EXCEPTION(parser("a,"), yexception);
}

Y_UNIT_TEST(ParseSetStringDeduplicates) {
    auto parser = NPrivate::GetDefaultParser<TSet<TString>>();
    auto result = parser("b,a,c,a");
    UNIT_ASSERT_VALUES_EQUAL(3U, result.size());
    UNIT_ASSERT(result.contains("a"));
    UNIT_ASSERT(result.contains("b"));
    UNIT_ASSERT(result.contains("c"));
}

Y_UNIT_TEST(ParseHashSetString) {
    auto parser = NPrivate::GetDefaultParser<THashSet<TString>>();
    auto result = parser("foo|bar|baz");
    UNIT_ASSERT_VALUES_EQUAL(3U, result.size());
    UNIT_ASSERT(result.contains("foo"));
    UNIT_ASSERT(result.contains("bar"));
    UNIT_ASSERT(result.contains("baz"));
}

Y_UNIT_TEST(ParseContainerEmptyItemThrows) {
    UNIT_ASSERT_EXCEPTION(NPrivate::GetDefaultParser<TSet<TString>>()("a;;b"), yexception);
    UNIT_ASSERT_EXCEPTION(NPrivate::GetDefaultParser<THashSet<TString>>()("a||b"), yexception);
}

} // Y_UNIT_TEST_SUITE(TDefaultParserTest)

Y_UNIT_TEST_SUITE(TDefaultSerializerTest) {

Y_UNIT_TEST(SerializeString) {
    auto s = NPrivate::GetDefaultSerializer<TString>();
    UNIT_ASSERT_VALUES_EQUAL("hello", s("hello"));
    UNIT_ASSERT_VALUES_EQUAL("", s(""));
    UNIT_ASSERT_VALUES_EQUAL("with spaces", s("with spaces"));
}

Y_UNIT_TEST(SerializeBoolTrue) {
    auto s = NPrivate::GetDefaultSerializer<bool>();
    UNIT_ASSERT_VALUES_EQUAL("true", s(true));
}

Y_UNIT_TEST(SerializeBoolFalse) {
    auto s = NPrivate::GetDefaultSerializer<bool>();
    UNIT_ASSERT_VALUES_EQUAL("false", s(false));
}

Y_UNIT_TEST(SerializeBoolRoundTrip) {
    auto parser = NPrivate::GetDefaultParser<bool>();
    auto ser = NPrivate::GetDefaultSerializer<bool>();
    UNIT_ASSERT_VALUES_EQUAL(true, parser(ser(true)));
    UNIT_ASSERT_VALUES_EQUAL(false, parser(ser(false)));
}

Y_UNIT_TEST(SerializeGuidRoundTrip) {
    TGUID original;
    CreateGuid(&original);
    auto ser = NPrivate::GetDefaultSerializer<TGUID>();
    auto parser = NPrivate::GetDefaultParser<TGUID>();
    TString serialized = ser(original);
    TGUID parsed = parser(serialized);
    UNIT_ASSERT_VALUES_EQUAL(GetGuidAsString(original), GetGuidAsString(parsed));
}

Y_UNIT_TEST(SerializeInstantRoundTrip) {
    TInstant original = TInstant::Seconds(1700000000); // a fixed timestamp
    auto ser = NPrivate::GetDefaultSerializer<TInstant>();
    auto parser = NPrivate::GetDefaultParser<TInstant>();
    TString serialized = ser(original);
    TInstant parsed = parser(serialized);
    UNIT_ASSERT_VALUES_EQUAL(original, parsed);
}

Y_UNIT_TEST(SerializeSizeToBytes) {
    auto ser = NPrivate::GetDefaultSerializer<NSize::TSize>();
    UNIT_ASSERT_VALUES_EQUAL("1024", ser(NSize::FromKiloBytes(1)));
    UNIT_ASSERT_VALUES_EQUAL("0", ser(NSize::TSize(0)));
    UNIT_ASSERT_VALUES_EQUAL("42", ser(NSize::TSize(42)));
}

Y_UNIT_TEST(SerializeUnsignedIntegers) {
    UNIT_ASSERT_VALUES_EQUAL("42", NPrivate::GetDefaultSerializer<ui8>()(42));
    UNIT_ASSERT_VALUES_EQUAL("1000", NPrivate::GetDefaultSerializer<ui16>()(1000));
    UNIT_ASSERT_VALUES_EQUAL("100000", NPrivate::GetDefaultSerializer<ui32>()(100000));
    UNIT_ASSERT_VALUES_EQUAL("1000000000000", NPrivate::GetDefaultSerializer<ui64>()(1000000000000ULL));
}

Y_UNIT_TEST(SerializeSignedIntegers) {
    UNIT_ASSERT_VALUES_EQUAL("-5", NPrivate::GetDefaultSerializer<i8>()(-5));
    UNIT_ASSERT_VALUES_EQUAL("-1000", NPrivate::GetDefaultSerializer<i16>()(-1000));
    UNIT_ASSERT_VALUES_EQUAL("-100000", NPrivate::GetDefaultSerializer<i32>()(-100000));
    UNIT_ASSERT_VALUES_EQUAL("-1000000000000", NPrivate::GetDefaultSerializer<i64>()(-1000000000000LL));
}

Y_UNIT_TEST(SerializeIntegersRoundTrip) {
    auto serI64 = NPrivate::GetDefaultSerializer<i64>();
    auto parseI64 = NPrivate::GetDefaultParser<i64>();
    UNIT_ASSERT_VALUES_EQUAL(-42LL, parseI64(serI64(-42LL)));
    UNIT_ASSERT_VALUES_EQUAL(0LL, parseI64(serI64(0LL)));

    auto serUi64 = NPrivate::GetDefaultSerializer<ui64>();
    auto parseUi64 = NPrivate::GetDefaultParser<ui64>();
    UNIT_ASSERT_VALUES_EQUAL(123456789ULL, parseUi64(serUi64(123456789ULL)));
}

Y_UNIT_TEST(SerializeDoubleRoundTrip) {
    auto ser = NPrivate::GetDefaultSerializer<double>();
    auto parser = NPrivate::GetDefaultParser<double>();
    UNIT_ASSERT_DOUBLES_EQUAL(3.14159, parser(ser(3.14159)), 1e-9);
    UNIT_ASSERT_DOUBLES_EQUAL(-2.5, parser(ser(-2.5)), 1e-9);
    UNIT_ASSERT_DOUBLES_EQUAL(0.0, parser(ser(0.0)), 1e-15);
}

Y_UNIT_TEST(SerializeDurationRoundTrip) {
    auto ser = NPrivate::GetDefaultSerializer<TDuration>();
    auto parser = NPrivate::GetDefaultParser<TDuration>();
    UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), parser(ser(TDuration::Seconds(5))));
    UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(500), parser(ser(TDuration::MilliSeconds(500))));
    UNIT_ASSERT_VALUES_EQUAL(TDuration::Zero(), parser(ser(TDuration::Zero())));
}

Y_UNIT_TEST(SerializeVectorString) {
    auto ser = NPrivate::GetDefaultSerializer<TVector<TString>>();
    UNIT_ASSERT_VALUES_EQUAL("a,b,c", ser({"a", "b", "c"}));
    UNIT_ASSERT_VALUES_EQUAL("single", ser({"single"}));
    UNIT_ASSERT_VALUES_EQUAL("", ser({}));
}

Y_UNIT_TEST(SerializeVectorStringRoundTrip) {
    auto ser = NPrivate::GetDefaultSerializer<TVector<TString>>();
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    TVector<TString> original = {"foo", "bar", "baz"};
    auto result = parser(ser(original));
    UNIT_ASSERT_VALUES_EQUAL(original.size(), result.size());
    for (size_t i = 0; i < original.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(original[i], result[i]);
    }
}

Y_UNIT_TEST(SerializeSetString) {
    auto ser = NPrivate::GetDefaultSerializer<TSet<TString>>();
    // TSet is sorted, so output is deterministic
    TSet<TString> s = {"c", "a", "b"};
    UNIT_ASSERT_VALUES_EQUAL("a,b,c", ser(s));
}

Y_UNIT_TEST(SerializeSetStringRoundTrip) {
    auto ser = NPrivate::GetDefaultSerializer<TSet<TString>>();
    auto parser = NPrivate::GetDefaultParser<TSet<TString>>();
    TSet<TString> original = {"x", "y", "z"};
    auto result = parser(ser(original));
    UNIT_ASSERT_VALUES_EQUAL(original, result);
}

Y_UNIT_TEST(SerializeHashSetStringRoundTrip) {
    auto ser = NPrivate::GetDefaultSerializer<THashSet<TString>>();
    auto parser = NPrivate::GetDefaultParser<THashSet<TString>>();
    THashSet<TString> original = {"foo", "bar", "baz"};
    auto result = parser(ser(original));
    UNIT_ASSERT_VALUES_EQUAL(original, result);
}

} // Y_UNIT_TEST_SUITE(TDefaultSerializerTest)

Y_UNIT_TEST_SUITE(TSettingDispatcherSerializeTest) {

Y_UNIT_TEST(StaticSettingWithValueIsEmitted) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.StaticString;
    dispatcher.AddSetting("MyStatic", &TTestSettings::StaticString);
    setting[ALL_CLUSTERS] = "hello";

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(1U, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("MyStatic", collected[0].first);
    UNIT_ASSERT_VALUES_EQUAL("hello", collected[0].second);
}

Y_UNIT_TEST(StaticSettingWithoutValueNotEmitted) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MyStatic", &TTestSettings::StaticString);
    // setting is not set

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(0U, collected.size());
}

Y_UNIT_TEST(MixedSettingsOnlyStaticEmitted) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& staticSetting = dispatcher.StaticString;
    auto& dynamicSetting = dispatcher.MySetting;
    auto& perClusterSetting = dispatcher.PerClusterString;
    dispatcher.AddSetting("Static", &TTestSettings::StaticString);
    dispatcher.AddSetting("Dynamic", &TTestSettings::MySetting);
    dispatcher.AddSetting("PerCluster", &TTestSettings::PerClusterString);

    staticSetting[ALL_CLUSTERS] = "static_val";
    dynamicSetting[ALL_CLUSTERS] = "dynamic_val";
    perClusterSetting[ALL_CLUSTERS] = "per_cluster_val";

    TVector<std::pair<TString, TString>> collected;
    UNIT_ASSERT_EXCEPTION_SATISFIES(dispatcher.SerializeStaticSettings([&](const TString&, const TString&) {}), yexception, [](const yexception& e) {
        return TString(e.what()).Contains("Only serialization for static settings is supported now");
    });
}

Y_UNIT_TEST(MultipleStaticSettingsAllEmitted) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& s1 = dispatcher.StaticString;
    auto& s2 = dispatcher.StaticInteger;
    auto& s3 = dispatcher.StaticBoolean;
    dispatcher.AddSetting("StrSetting", &TTestSettings::StaticString);
    dispatcher.AddSetting("IntSetting", &TTestSettings::StaticInteger);
    dispatcher.AddSetting("BoolSetting", &TTestSettings::StaticBoolean);

    s1[ALL_CLUSTERS] = "text";
    s2[ALL_CLUSTERS] = 42LL;
    s3[ALL_CLUSTERS] = true;

    THashMap<TString, TString> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected[name] = value;
    });

    UNIT_ASSERT_VALUES_EQUAL(3U, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("text", collected["StrSetting"]);
    UNIT_ASSERT_VALUES_EQUAL("42", collected["IntSetting"]);
    UNIT_ASSERT_VALUES_EQUAL("true", collected["BoolSetting"]);
}

Y_UNIT_TEST(CustomSerializerOverride) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& setting = dispatcher.StaticString;
    dispatcher.AddSetting("MySetting", &TTestSettings::StaticString)
        .Serializer([](const TString& v) -> TString { return "custom_" + v; });

    setting[ALL_CLUSTERS] = "value";

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(1U, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("custom_value", collected[0].second);
}

Y_UNIT_TEST(SerializationOrderIsAlphabeticRegardlessOfRegistrationOrder) {
    TDispatcherAndSettings<TTestSettings> dispatcherReversed;
    TDispatcherAndSettings<TTestSettings> dispatcherForward;
    auto& alphaSettingReversed = dispatcherReversed.StaticString;
    auto& betaSettingReversed = dispatcherReversed.OtherStaticString;
    auto& charlieSettingReversed = dispatcherReversed.ThirdStaticString;
    dispatcherReversed.AddSetting("Charlie", &TTestSettings::ThirdStaticString);
    dispatcherReversed.AddSetting("Beta", &TTestSettings::OtherStaticString);
    dispatcherReversed.AddSetting("Alpha", &TTestSettings::StaticString);
    alphaSettingReversed[ALL_CLUSTERS] = "alpha_value";
    betaSettingReversed[ALL_CLUSTERS] = "beta_value";
    charlieSettingReversed[ALL_CLUSTERS] = "charlie_value";

    auto& alphaSettingForward = dispatcherForward.StaticString;
    auto& betaSettingForward = dispatcherForward.OtherStaticString;
    auto& charlieSettingForward = dispatcherForward.ThirdStaticString;
    dispatcherForward.AddSetting("Alpha", &TTestSettings::StaticString);
    dispatcherForward.AddSetting("Beta", &TTestSettings::OtherStaticString);
    dispatcherForward.AddSetting("Charlie", &TTestSettings::ThirdStaticString);
    alphaSettingForward[ALL_CLUSTERS] = "alpha_value";
    betaSettingForward[ALL_CLUSTERS] = "beta_value";
    charlieSettingForward[ALL_CLUSTERS] = "charlie_value";

    TVector<std::pair<TString, TString>> reversedResult;
    TVector<std::pair<TString, TString>> forwardResult;
    dispatcherReversed.SerializeStaticSettings([&](const TString& name, const TString& value) {
        reversedResult.emplace_back(name, value);
    });
    dispatcherForward.SerializeStaticSettings([&](const TString& name, const TString& value) {
        forwardResult.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(3U, reversedResult.size());
    UNIT_ASSERT_VALUES_EQUAL(3U, forwardResult.size());

    UNIT_ASSERT_VALUES_EQUAL(reversedResult[0].first, forwardResult[0].first);
    UNIT_ASSERT_VALUES_EQUAL(reversedResult[1].first, forwardResult[1].first);
    UNIT_ASSERT_VALUES_EQUAL(reversedResult[2].first, forwardResult[2].first);

    UNIT_ASSERT_VALUES_EQUAL("Alpha", reversedResult[0].first);
    UNIT_ASSERT_VALUES_EQUAL("Beta", reversedResult[1].first);
    UNIT_ASSERT_VALUES_EQUAL("Charlie", reversedResult[2].first);

    UNIT_ASSERT_VALUES_EQUAL("alpha_value", reversedResult[0].second);
    UNIT_ASSERT_VALUES_EQUAL("beta_value", reversedResult[1].second);
    UNIT_ASSERT_VALUES_EQUAL("charlie_value", reversedResult[2].second);
}

Y_UNIT_TEST(CountSerializableStaticSettingsEmpty) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    dispatcher.AddSetting("MySetting", &TTestSettings::StaticString);

    UNIT_ASSERT_VALUES_EQUAL(0U, dispatcher.CountSerializableStaticSettings());
}

Y_UNIT_TEST(CountSerializableStaticSettingsWithValue) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& stringSetting = dispatcher.StaticString;
    auto& integerSetting = dispatcher.StaticInteger;
    auto& anotherStringSetting = dispatcher.OtherStaticString;
    dispatcher.AddSetting("StringSetting", &TTestSettings::StaticString);
    dispatcher.AddSetting("IntegerSetting", &TTestSettings::StaticInteger);
    dispatcher.AddSetting("AnotherStringSetting", &TTestSettings::OtherStaticString);

    stringSetting[ALL_CLUSTERS] = "string_value";
    integerSetting[ALL_CLUSTERS] = 42LL;

    UNIT_ASSERT_VALUES_EQUAL(2U, dispatcher.CountSerializableStaticSettings());

    anotherStringSetting[ALL_CLUSTERS] = "another_string_value";

    UNIT_ASSERT_VALUES_EQUAL(3U, dispatcher.CountSerializableStaticSettings());
}

Y_UNIT_TEST(CountSerializableStaticSettingsMatchesSerializeCount) {
    TDispatcherAndSettings<TTestSettings> dispatcher;
    auto& stringSetting = dispatcher.StaticString;
    auto& integerSetting = dispatcher.StaticInteger;
    dispatcher.AddSetting("Alpha", &TTestSettings::StaticString);
    dispatcher.AddSetting("Beta", &TTestSettings::StaticInteger);
    stringSetting[ALL_CLUSTERS] = "hello";
    integerSetting[ALL_CLUSTERS] = 7LL;

    ui64 serializedCount = 0;
    dispatcher.SerializeStaticSettings([&](const TString&, const TString&) { ++serializedCount; });

    UNIT_ASSERT_VALUES_EQUAL(serializedCount, dispatcher.CountSerializableStaticSettings());
}

} // Y_UNIT_TEST_SUITE(TSettingDispatcherSerializeTest)

} // namespace NYql::NCommon
