#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TRequiredSingletonConfig);
DECLARE_REFCOUNTED_STRUCT(TOptionalSingletonConfig);
DECLARE_REFCOUNTED_STRUCT(TDefaultNewSingletonConfig);
DECLARE_REFCOUNTED_STRUCT(TReconfigurableSingletonConfig);
DECLARE_REFCOUNTED_STRUCT(TReconfigurableSingletonDynamicConfig);

YT_DECLARE_CONFIGURABLE_SINGLETON(TRequiredSingletonConfig);
YT_DECLARE_CONFIGURABLE_SINGLETON(TOptionalSingletonConfig);
YT_DECLARE_CONFIGURABLE_SINGLETON(TDefaultNewSingletonConfig);
YT_DECLARE_RECONFIGURABLE_SINGLETON(TReconfigurableSingletonConfig, TReconfigurableSingletonDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

struct TRequiredSingletonConfig
    : public TYsonStruct
{
    int Speed;

    REGISTER_YSON_STRUCT(TRequiredSingletonConfig);

    static void Register(TRegistrar registarar)
    {
        registarar.Parameter("speed", &TThis::Speed);
    }
};

DEFINE_REFCOUNTED_TYPE(TRequiredSingletonConfig)

int ConfiguredSpeed = -1;

void SetupSingletonConfigParameter(TYsonStructParameter<TRequiredSingletonConfigPtr>& /*parameter*/)
{ }

void ConfigureSingleton(const TRequiredSingletonConfigPtr& config)
{
    ConfiguredSpeed = config->Speed;
}

YT_DEFINE_CONFIGURABLE_SINGLETON("required", TRequiredSingletonConfig);

////////////////////////////////////////////////////////////////////////////////

struct TOptionalSingletonConfig
    : public TYsonStruct
{
    int Depth;

    REGISTER_YSON_STRUCT(TOptionalSingletonConfig);

    static void Register(TRegistrar registarar)
    {
        registarar.Parameter("depth", &TThis::Depth);
    }
};

DEFINE_REFCOUNTED_TYPE(TOptionalSingletonConfig)

int ConfiguredDepth = -1;

void SetupSingletonConfigParameter(TYsonStructParameter<TOptionalSingletonConfigPtr>& parameter)
{
    parameter.Optional();
}

void ConfigureSingleton(const TOptionalSingletonConfigPtr& config)
{
    if (config) {
        ConfiguredDepth = config->Depth;
    }
}

YT_DEFINE_CONFIGURABLE_SINGLETON("optional", TOptionalSingletonConfig);

////////////////////////////////////////////////////////////////////////////////

struct TDefaultNewSingletonConfig
    : public TYsonStruct
{
    int Width;

    REGISTER_YSON_STRUCT(TDefaultNewSingletonConfig);

    static void Register(TRegistrar registarar)
    {
        registarar.Parameter("width", &TThis::Width)
            .Default(456);
    }
};

DEFINE_REFCOUNTED_TYPE(TDefaultNewSingletonConfig)

int ConfiguredWidth = -1;

void SetupSingletonConfigParameter(TYsonStructParameter<TDefaultNewSingletonConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TDefaultNewSingletonConfigPtr& config)
{
    ConfiguredWidth = config->Width;
}

YT_DEFINE_CONFIGURABLE_SINGLETON("default_new", TDefaultNewSingletonConfig);

////////////////////////////////////////////////////////////////////////////////

struct TReconfigurableSingletonConfig
    : public TYsonStruct
{
    int Cost;

    REGISTER_YSON_STRUCT(TReconfigurableSingletonConfig);

    static void Register(TRegistrar registarar)
    {
        registarar.Parameter("cost", &TThis::Cost)
            .Default(777);
    }
};

DEFINE_REFCOUNTED_TYPE(TReconfigurableSingletonConfig)

struct TReconfigurableSingletonDynamicConfig
    : public TYsonStruct
{
    std::optional<int> Cost;

    REGISTER_YSON_STRUCT(TReconfigurableSingletonDynamicConfig);

    static void Register(TRegistrar registarar)
    {
        registarar.Parameter("cost", &TThis::Cost)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TReconfigurableSingletonDynamicConfig)

int ConfiguredCost = -1;

void SetupSingletonConfigParameter(TYsonStructParameter<TReconfigurableSingletonConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void SetupSingletonConfigParameter(TYsonStructParameter<TReconfigurableSingletonDynamicConfigPtr>& parameter)
{
    parameter.DefaultNew();
}

void ConfigureSingleton(const TReconfigurableSingletonConfigPtr& config)
{
    ConfiguredCost = config->Cost;
}

void ReconfigureSingleton(
    const TReconfigurableSingletonConfigPtr& config,
    const TReconfigurableSingletonDynamicConfigPtr& dynamicConfig)
{
    ConfiguredCost = dynamicConfig->Cost.value_or(config->Cost);
}

YT_DEFINE_RECONFIGURABLE_SINGLETON(
    "reconfigurable",
    TReconfigurableSingletonConfig,
    TReconfigurableSingletonDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

TEST(TConfigurableSingletonTest, Run)
{
    auto config = ConvertTo<TSingletonsConfigPtr>(NYson::TYsonString(TString(R"""({
        required = {
            speed = 123;
        };
    })""")));
    auto dynamicConfig1 = ConvertTo<TSingletonsDynamicConfigPtr>(NYson::TYsonString(TString(R"""({
        reconfigurable = {
            cost = 888;
        };
    })""")));
    auto dynamicConfig2 = ConvertTo<TSingletonsDynamicConfigPtr>(NYson::TYsonString(TString(R"""({
        reconfigurable = {
            cost = 999;
        };
    })""")));

    EXPECT_THROW_WITH_SUBSTRING(TSingletonManager::Reconfigure(dynamicConfig1), "Singletons are not configured yet");

    EXPECT_EQ(ConfiguredSpeed, -1);
    EXPECT_EQ(ConfiguredDepth, -1);
    EXPECT_EQ(ConfiguredWidth, -1);
    EXPECT_EQ(ConfiguredCost, -1);

    TSingletonManager::Configure(config);

    EXPECT_EQ(ConfiguredSpeed, 123);
    EXPECT_EQ(ConfiguredDepth, -1);
    EXPECT_EQ(ConfiguredWidth, 456);
    EXPECT_EQ(ConfiguredCost, 777);

    EXPECT_THROW_WITH_SUBSTRING(TSingletonManager::Configure(config), "Singletons have already been configured");

    TSingletonManager::Reconfigure(dynamicConfig1);

    EXPECT_EQ(ConfiguredSpeed, 123);
    EXPECT_EQ(ConfiguredDepth, -1);
    EXPECT_EQ(ConfiguredWidth, 456);
    EXPECT_EQ(ConfiguredCost, 888);

    TSingletonManager::Reconfigure(dynamicConfig2);

    EXPECT_EQ(ConfiguredSpeed, 123);
    EXPECT_EQ(ConfiguredDepth, -1);
    EXPECT_EQ(ConfiguredWidth, 456);
    EXPECT_EQ(ConfiguredCost, 999);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
