#include <yql/essentials/providers/common/config/yql_dispatch.h>
#include <yql/essentials/providers/common/config/yql_setting.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NCommon {

namespace {

struct TTestErrorCollector {
    TString LastMessage;
    bool LastIsError = false;
    bool ShouldContinue = true;

    TSettingDispatcher::TErrorCallback MakeCallback() {
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
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("hello"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("hello", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(AddDuplicateThrows) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);
    UNIT_ASSERT_EXCEPTION(dispatcher.AddSetting("MySetting", setting), yexception);
}

Y_UNIT_TEST(UnknownSettingAtConfigIgnored) {
    TSettingDispatcher dispatcher;
    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(errors.LastMessage.empty());
}

Y_UNIT_TEST(UnknownSettingAtStaticErrors) {
    TSettingDispatcher dispatcher;
    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown setting name"));
}

Y_UNIT_TEST(UnknownSettingAtRuntimeErrors) {
    TSettingDispatcher dispatcher;
    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "Unknown", TString("val"), TSettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown setting name"));
}

Y_UNIT_TEST(StaticSettingWithSpecificClusterErrors) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> setting;
    dispatcher.AddSetting("StaticSetting", setting);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch("cluster1", "StaticSetting", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be set for specific cluster"));
}

Y_UNIT_TEST(InvalidClusterErrors) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch("invalid_cluster", "MySetting", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Unknown cluster name"));
}

Y_UNIT_TEST(NullValueForStaticSettingErrors) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> setting;
    dispatcher.AddSetting("StaticSetting", setting);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "StaticSetting", Nothing(), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be reset to default"));
}

Y_UNIT_TEST(NullValueForDynamicSettingWithoutFreezeErrors) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", Nothing(), TSettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("Cannot restore"));
}

Y_UNIT_TEST(NullValueForDynamicSettingRestoresDefault) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);

    setting[ALL_CLUSTERS] = "original";
    dispatcher.FreezeDefaults();

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("changed"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("changed", *setting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", Nothing(), TSettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("original", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(FreezeAndRestore) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> s1;
    TConfSetting<i64> s2;
    dispatcher.AddSetting("S1", s1);
    dispatcher.AddSetting("S2", s2);

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
    TSettingDispatcher dispatcher;
    TConfSetting<TString> dynamicSetting;
    TConfSetting<TString, EConfSettingType::Static> staticSetting;
    TConfSetting<TString, EConfSettingType::StaticPerCluster> staticPerClusterSetting;
    dispatcher.AddSetting("Dynamic", dynamicSetting);
    dispatcher.AddSetting("Static", staticSetting);
    dispatcher.AddSetting("StaticPerCluster", staticPerClusterSetting);

    UNIT_ASSERT(dispatcher.IsRuntime("Dynamic"));
    UNIT_ASSERT(!dispatcher.IsRuntime("Static"));
    UNIT_ASSERT(!dispatcher.IsRuntime("StaticPerCluster"));
    UNIT_ASSERT(!dispatcher.IsRuntime("Unknown"));
}

Y_UNIT_TEST(StageStaticAppliesOnlyNonRuntime) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> dynamicSetting;
    TConfSetting<TString, EConfSettingType::Static> staticSetting;
    dispatcher.AddSetting("Dynamic", dynamicSetting);
    dispatcher.AddSetting("Static", staticSetting);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(!dynamicSetting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("val", *staticSetting.Get());
}

Y_UNIT_TEST(StageRuntimeAppliesOnlyRuntime) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> dynamicSetting;
    TConfSetting<TString, EConfSettingType::Static> staticSetting;
    dispatcher.AddSetting("Dynamic", dynamicSetting);
    dispatcher.AddSetting("Static", staticSetting);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("dyn_val"), TSettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("dyn_val", *dynamicSetting.Get(ALL_CLUSTERS));

    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("sta_val"), TSettingDispatcher::EStage::RUNTIME, errors.MakeCallback());
    UNIT_ASSERT(!staticSetting.Get());
}

Y_UNIT_TEST(StageConfigAlwaysAppliesBoth) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> dynamicSetting;
    TConfSetting<TString, EConfSettingType::Static> staticSetting;
    dispatcher.AddSetting("Dynamic", dynamicSetting);
    dispatcher.AddSetting("Static", staticSetting);

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "Dynamic", TString("dyn"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    dispatcher.Dispatch(ALL_CLUSTERS, "Static", TString("sta"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("dyn", *dynamicSetting.Get(ALL_CLUSTERS));
    UNIT_ASSERT_VALUES_EQUAL("sta", *staticSetting.Get());
}

Y_UNIT_TEST(CaseInsensitiveSettingNames) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "mysetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("val", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorLower) {
    TSettingDispatcher dispatcher;
    TConfSetting<i64> setting;
    dispatcher.AddSetting("MySetting", setting).Lower(0LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("-1"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("less than"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("0"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(0L, *setting.Get(ALL_CLUSTERS));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("5"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(5L, *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorUpper) {
    TSettingDispatcher dispatcher;
    TConfSetting<i64> setting;
    dispatcher.AddSetting("MySetting", setting).Upper(100LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("200"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("greater than"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("100"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL(100L, *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorLowerAndUpperBothApply) {
    TSettingDispatcher dispatcher;
    TConfSetting<i64> setting;
    dispatcher.AddSetting("MySetting", setting).Lower(0LL).Upper(10LL);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("-1"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("11"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("5"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
}

Y_UNIT_TEST(ValidatorEnumInitializerList) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).Enum({"a", "b", "c"});

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("d"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("not in set of allowed values"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("b"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("b", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(ValidatorEnumContainer) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    TVector<TString> allowed = {"x", "y"};
    dispatcher.AddSetting("MySetting", setting).Enum(allowed);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    UNIT_ASSERT(!dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("z"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
    UNIT_ASSERT(dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("x"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback()));
}

Y_UNIT_TEST(ValidatorNonEmpty) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).NonEmpty();

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString(""), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("empty"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("nonempty"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(ValidatorGlobalOnly) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).GlobalOnly();
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch("cluster1", "MySetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("cannot be used with specific cluster"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(ValidatorCustom) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).Validator([](const TString&, const TString& val) {
        if (val == "forbidden") {
            throw yexception() << "Value is forbidden";
        }
    });

    TTestErrorCollector errors;
    errors.ShouldContinue = false;

    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("forbidden"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(!ok);
    UNIT_ASSERT(errors.LastMessage.Contains("forbidden"));

    ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("allowed"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
}

Y_UNIT_TEST(WarningCallback) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).Warning("Use new setting instead");

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(!errors.LastIsError);
    UNIT_ASSERT_VALUES_EQUAL("Use new setting instead", errors.LastMessage);
    UNIT_ASSERT_VALUES_EQUAL("val", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(DeprecatedSettingProducesWarning) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).Deprecated();

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT(!errors.LastIsError);
    UNIT_ASSERT(errors.LastMessage.Contains("deprecated"));
}

Y_UNIT_TEST(DeprecatedSettingCustomMessage) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting).Deprecated("Please use NewSetting instead");

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("val"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("Please use NewSetting instead", errors.LastMessage);
}

Y_UNIT_TEST(EnumerateSkipsDeprecatedAndUnderscoreNames) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> s1;
    TConfSetting<TString> s2;
    TConfSetting<TString> s3;
    dispatcher.AddSetting("Normal", s1);
    dispatcher.AddSetting("DeprecatedOne", s2).Deprecated();
    dispatcher.AddSetting("_HiddenInternal", s3);

    TVector<TString> names;
    dispatcher.Enumerate([&](std::string_view name) { names.emplace_back(name); });

    UNIT_ASSERT(Find(names, "Normal") != names.end());
    UNIT_ASSERT(Find(names, "DeprecatedOne") == names.end());
    UNIT_ASSERT(Find(names, "_HiddenInternal") == names.end());
    UNIT_ASSERT_VALUES_EQUAL(1u, names.size());
}

Y_UNIT_TEST(ValidClustersManagement) {
    TSettingDispatcher dispatcher;
    UNIT_ASSERT(!dispatcher.IsValidCluster("cluster1"));

    dispatcher.AddValidCluster("cluster1");
    dispatcher.AddValidCluster("cluster2");
    UNIT_ASSERT(dispatcher.IsValidCluster("cluster1"));
    UNIT_ASSERT(dispatcher.IsValidCluster("cluster2"));
    UNIT_ASSERT(!dispatcher.IsValidCluster("cluster3"));
    UNIT_ASSERT_VALUES_EQUAL(2u, dispatcher.GetValidClusters().size());
}

Y_UNIT_TEST(SetValidClusters) {
    TSettingDispatcher dispatcher;
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
    TSettingDispatcher dispatcher(clusters);
    UNIT_ASSERT(dispatcher.IsValidCluster("c1"));
    UNIT_ASSERT(dispatcher.IsValidCluster("c2"));
    UNIT_ASSERT(!dispatcher.IsValidCluster("c4"));
}

Y_UNIT_TEST(LevenshteinSuggestionForCluster) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);
    dispatcher.AddValidCluster("cluster1");

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    dispatcher.Dispatch("clusteR1", "MySetting", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(errors.LastMessage.Contains("did you mean"));
    UNIT_ASSERT(errors.LastMessage.Contains("cluster1"));
}

Y_UNIT_TEST(LevenshteinSuggestionForSettingName) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    dispatcher.AddSetting("MySetting", setting);

    TTestErrorCollector errors;
    errors.ShouldContinue = false;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySettnig", TString("val"), TSettingDispatcher::EStage::STATIC, errors.MakeCallback());
    UNIT_ASSERT(errors.LastMessage.Contains("did you mean"));
    UNIT_ASSERT(errors.LastMessage.Contains("MySetting"));
}

Y_UNIT_TEST(CustomValueSetter) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> setting;
    TString captured;
    dispatcher.AddSetting("MySetting", setting).ValueSetter([&](const TString&, TString val) {
        captured = "custom_" + val;
        setting[ALL_CLUSTERS] = captured;
    });

    TTestErrorCollector errors;
    dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("hello"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT_VALUES_EQUAL("custom_hello", *setting.Get(ALL_CLUSTERS));
}

Y_UNIT_TEST(RegisterSettingMacro) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString> MySetting;
    REGISTER_SETTING(dispatcher, MySetting);

    TTestErrorCollector errors;
    bool ok = dispatcher.Dispatch(ALL_CLUSTERS, "MySetting", TString("macro_test"), TSettingDispatcher::EStage::CONFIG, errors.MakeCallback());
    UNIT_ASSERT(ok);
    UNIT_ASSERT_VALUES_EQUAL("macro_test", *MySetting.Get(ALL_CLUSTERS));
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
    UNIT_ASSERT_VALUES_EQUAL(42u, NPrivate::GetDefaultParser<ui8>()("42"));
    UNIT_ASSERT_VALUES_EQUAL(1000u, NPrivate::GetDefaultParser<ui16>()("1000"));
    UNIT_ASSERT_VALUES_EQUAL(100000u, NPrivate::GetDefaultParser<ui32>()("100000"));
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
    UNIT_ASSERT_DOUBLES_EQUAL(1.5f, NPrivate::GetDefaultParser<float>()("1.5"), 1e-5);
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
    UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
    UNIT_ASSERT_VALUES_EQUAL("a", result[0]);
    UNIT_ASSERT_VALUES_EQUAL("b", result[1]);
    UNIT_ASSERT_VALUES_EQUAL("c", result[2]);
}

Y_UNIT_TEST(ParseVectorStringSemicolon) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("x;y;z");
    UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
}

Y_UNIT_TEST(ParseVectorStringPipe) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("foo|bar");
    UNIT_ASSERT_VALUES_EQUAL(2u, result.size());
    UNIT_ASSERT_VALUES_EQUAL("foo", result[0]);
    UNIT_ASSERT_VALUES_EQUAL("bar", result[1]);
}

Y_UNIT_TEST(ParseVectorStringSpace) {
    auto parser = NPrivate::GetDefaultParser<TVector<TString>>();
    auto result = parser("hello world");
    UNIT_ASSERT_VALUES_EQUAL(2u, result.size());
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
    UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
    UNIT_ASSERT(result.contains("a"));
    UNIT_ASSERT(result.contains("b"));
    UNIT_ASSERT(result.contains("c"));
}

Y_UNIT_TEST(ParseHashSetString) {
    auto parser = NPrivate::GetDefaultParser<THashSet<TString>>();
    auto result = parser("foo|bar|baz");
    UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
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
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> setting;
    dispatcher.AddSetting("MyStatic", setting);
    setting[ALL_CLUSTERS] = "hello";

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(1u, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("MyStatic", collected[0].first);
    UNIT_ASSERT_VALUES_EQUAL("hello", collected[0].second);
}

Y_UNIT_TEST(StaticSettingWithoutValueNotEmitted) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> setting;
    dispatcher.AddSetting("MyStatic", setting);
    // setting is not set

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(0u, collected.size());
}

Y_UNIT_TEST(MixedSettingsOnlyStaticEmitted) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> staticSetting;
    TConfSetting<TString> dynamicSetting;
    TConfSetting<TString, EConfSettingType::StaticPerCluster> perClusterSetting;
    dispatcher.AddSetting("Static", staticSetting);
    dispatcher.AddSetting("Dynamic", dynamicSetting);
    dispatcher.AddSetting("PerCluster", perClusterSetting);

    staticSetting[ALL_CLUSTERS] = "static_val";
    dynamicSetting[ALL_CLUSTERS] = "dynamic_val";
    perClusterSetting[ALL_CLUSTERS] = "per_cluster_val";

    TVector<std::pair<TString, TString>> collected;
    UNIT_ASSERT_EXCEPTION_SATISFIES(dispatcher.SerializeStaticSettings([&](const TString&, const TString&) {}), yexception, [](const yexception& e) {
        return TString(e.what()).Contains("Only serialization for static settings is supported now");
    });
}

Y_UNIT_TEST(MultipleStaticSettingsAllEmitted) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> s1;
    TConfSetting<i64, EConfSettingType::Static> s2;
    TConfSetting<bool, EConfSettingType::Static> s3;
    dispatcher.AddSetting("StrSetting", s1);
    dispatcher.AddSetting("IntSetting", s2);
    dispatcher.AddSetting("BoolSetting", s3);

    s1[ALL_CLUSTERS] = "text";
    s2[ALL_CLUSTERS] = 42LL;
    s3[ALL_CLUSTERS] = true;

    THashMap<TString, TString> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected[name] = value;
    });

    UNIT_ASSERT_VALUES_EQUAL(3u, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("text", collected["StrSetting"]);
    UNIT_ASSERT_VALUES_EQUAL("42", collected["IntSetting"]);
    UNIT_ASSERT_VALUES_EQUAL("true", collected["BoolSetting"]);
}

Y_UNIT_TEST(CustomSerializerOverride) {
    TSettingDispatcher dispatcher;
    TConfSetting<TString, EConfSettingType::Static> setting;
    dispatcher.AddSetting("MySetting", setting)
        .Serializer([](const TString& v) -> TString { return "custom_" + v; });

    setting[ALL_CLUSTERS] = "value";

    TVector<std::pair<TString, TString>> collected;
    dispatcher.SerializeStaticSettings([&](const TString& name, const TString& value) {
        collected.emplace_back(name, value);
    });

    UNIT_ASSERT_VALUES_EQUAL(1u, collected.size());
    UNIT_ASSERT_VALUES_EQUAL("custom_value", collected[0].second);
}

} // Y_UNIT_TEST_SUITE(TSettingDispatcherSerializeTest)

} // namespace NYql::NCommon
