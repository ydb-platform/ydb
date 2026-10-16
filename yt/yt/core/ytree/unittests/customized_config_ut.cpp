#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/customized_config.h>

namespace NYT::NYTree {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionConfig
    : public TYsonStruct
{
    i64 Timeout;
    std::string Role;

    REGISTER_YSON_STRUCT(TConnectionConfig);

    static void Register(TRegistrar registrar);
};

void TConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(10);
    registrar.Parameter("role", &TThis::Role)
        .Default("worker");
}

////////////////////////////////////////////////////////////////////////////////

struct TConnectionConfigLite
    : public TYsonStructLite
{
    i64 Timeout;
    std::string Role;

    REGISTER_YSON_STRUCT_LITE(TConnectionConfigLite);

    static void Register(TRegistrar registrar);
};

void TConnectionConfigLite::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(10);
    registrar.Parameter("role", &TThis::Role)
        .Default("worker");
}

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TConfig>
const TConfig& GetConfig(const TConfig& config)
{
    return config;
}

template <CYsonStruct TConfig>
const TConfig& GetConfig(const TIntrusivePtr<TConfig>& config)
{
    return *config;
}

using TCustomizedConfigTypes = ::testing::Types<
    TIntrusivePtr<TConnectionConfig>,
    TConnectionConfigLite>;

template <class TConfig>
class TCustomizedConfigTypedTest
    : public ::testing::Test
{ };

TYPED_TEST_SUITE(TCustomizedConfigTypedTest, TCustomizedConfigTypes);

TYPED_TEST(TCustomizedConfigTypedTest, OverridesUseOwnDefaults)
{
    auto config = ConvertTo<TCustomizedConfig<TypeParam>>(NYson::TYsonStringBuf(R"({
        default={timeout=30;role=pando;};
        overrides={hahn={timeout=60;};};
    })"));
    const auto& connection = GetConfig(config.Get("hahn"));
    const auto& defaultConnection = GetConfig(config.Get("arnold"));

    EXPECT_EQ(connection.Timeout, 60);
    EXPECT_EQ(connection.Role, "worker");
    EXPECT_EQ(defaultConnection.Timeout, 30);
    EXPECT_EQ(defaultConnection.Role, "pando");
}

////////////////////////////////////////////////////////////////////////////////

struct TDerivedCustomizedConfigLite
    : public TCustomizedConfig<i64>
{
    i64 Limit;

    REGISTER_YSON_STRUCT_LITE(TDerivedCustomizedConfigLite);

    static void Register(TRegistrar registrar);
};

void TDerivedCustomizedConfigLite::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default(5);
}

TEST(TCustomizedConfigTest, InheritedLiteWrapper)
{
    auto config = ConvertTo<TDerivedCustomizedConfigLite>(NYson::TYsonStringBuf(R"({
        default=30;
        overrides={hahn=60;};
    })"));

    EXPECT_EQ(config.Get("hahn"), 60);
    EXPECT_EQ(config.Get("arnold"), 30);
    EXPECT_EQ(config.Limit, 5);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCustomizedConfigTest, ScalarValues)
{
    using TScalarConfig = TCustomizedConfig<i64>;
    TScalarConfig emptyConfig;
    EXPECT_EQ(emptyConfig.Get("hahn"), 0);

    auto config = ConvertTo<TScalarConfig>(NYson::TYsonStringBuf(R"({
        default=30;
        overrides={hahn=60;arnold=90;};
    })"));

    EXPECT_EQ(config.Get("hahn"), 60);
    EXPECT_EQ(config.Get("arnold"), 90);
    EXPECT_EQ(config.Get("vanga"), 30);
    EXPECT_EQ(&config.Get("vanga"), &config.Get("unknown"));
    EXPECT_EQ(&config.Get("hahn"), &config.Get("hahn"));
}

inline constexpr char PerClusterOverridesFieldName[] = "per_cluster";

TEST(TCustomizedConfigTest, RenamedOverridesWithIntegerKeysRoundTrip)
{
    using TPerClusterConfig = TCustomizedConfig<i64, i32, PerClusterOverridesFieldName>;
    auto config = ConvertTo<TPerClusterConfig>(NYson::TYsonStringBuf(R"({
        default=30;
        per_cluster={"42"=60;};
    })"));
    auto node = ConvertToNode(config);
    auto restored = ConvertTo<TPerClusterConfig>(node);

    EXPECT_TRUE(node->AsMap()->FindChild("per_cluster"));
    EXPECT_FALSE(node->AsMap()->FindChild("overrides"));
    EXPECT_EQ(restored.Get(42), 60);
    EXPECT_EQ(restored.Get(43), 30);
}

TEST(TCustomizedConfigTest, MapOverrideReplacesDefault)
{
    using TMapConfig = TCustomizedConfig<THashMap<std::string, i64>>;
    auto config = ConvertTo<TMapConfig>(NYson::TYsonStringBuf(R"({
        default={first=1;second=2;};
        overrides={hahn={first=3;};};
    })"));
    const auto& value = config.Get("hahn");

    EXPECT_EQ(value, (THashMap<std::string, i64>{{"first", 3}}));
    EXPECT_EQ(config.Get("arnold"), (THashMap<std::string, i64>{{"first", 1}, {"second", 2}}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NYTree
