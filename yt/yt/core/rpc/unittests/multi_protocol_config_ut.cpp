#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/backend.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/endpoint_address.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <algorithm>

namespace NYT::NRpc {
namespace {

using namespace NYT::NBus::NTcp;

////////////////////////////////////////////////////////////////////////////////

constexpr auto UnregisteredProtocol = "no-such-protocol"_sb;

////////////////////////////////////////////////////////////////////////////////

// Exercises the typed/untyped config accessors of TProtocolMapConfigBase through
// TMultiProtocolServerConfig over the always-registered "yt-tcp" backend.
TBusServerConfigPtr MakeBusServerConfig(int port)
{
    auto config = New<TBusServerConfig>();
    config->Port = port;
    return config;
}

TMultiProtocolServerConfigPtr MakeServerConfig(int port)
{
    return ConvertTo<TMultiProtocolServerConfigPtr>(NYTree::BuildYsonNodeFluently()
        .BeginMap()
            .Item(DefaultProtocolName).BeginMap()
                .Item("port").Value(port)
            .EndMap()
        .EndMap());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMultiProtocolConfigTest, FindConfiguredProtocol)
{
    auto config = MakeServerConfig(123);

    auto typedConfig = config->FindTypedConfig<TBusServerConfig>(DefaultProtocolName);
    ASSERT_TRUE(typedConfig);
    EXPECT_EQ(123, typedConfig->Port);

    auto orThrow = config->GetTypedConfigOrThrow<TBusServerConfig>(DefaultProtocolName);
    EXPECT_EQ(typedConfig, orThrow);

    auto untyped = config->FindUntypedConfig(DefaultProtocolName);
    ASSERT_TRUE(untyped.has_value());
    EXPECT_EQ(typedConfig, std::any_cast<TBusServerConfigPtr>(untyped));

    EXPECT_EQ(typedConfig, std::any_cast<TBusServerConfigPtr>(config->GetUntypedConfig(DefaultProtocolName)));
}

TEST(TMultiProtocolConfigTest, RegisteredButUnconfiguredProtocol)
{
    // A freshly created config has the "yt-tcp" entry registered, but null.
    auto config = New<TMultiProtocolServerConfig>();

    EXPECT_FALSE(config->FindTypedConfig<TBusServerConfig>(DefaultProtocolName));

    // FindUntypedConfig treats a null entry as unconfigured.
    EXPECT_FALSE(config->FindUntypedConfig(DefaultProtocolName).has_value());

    EXPECT_THROW_WITH_SUBSTRING(
        config->GetTypedConfigOrThrow<TBusServerConfig>(DefaultProtocolName),
        "is not configured");

    // GetUntypedConfig still returns the (null) entry without crashing.
    EXPECT_FALSE(std::any_cast<TBusServerConfigPtr>(config->GetUntypedConfig(DefaultProtocolName)));
}

TEST(TMultiProtocolConfigTest, UnregisteredProtocol)
{
    auto config = New<TMultiProtocolServerConfig>();

    EXPECT_FALSE(config->FindTypedConfig<TBusServerConfig>(UnregisteredProtocol));
    EXPECT_FALSE(config->FindUntypedConfig(UnregisteredProtocol).has_value());
    EXPECT_THROW_WITH_SUBSTRING(
        config->GetTypedConfigOrThrow<TBusServerConfig>(UnregisteredProtocol),
        "is not configured");
}

TEST(TMultiProtocolConfigTest, SetTypedConfig)
{
    auto config = New<TMultiProtocolServerConfig>();

    auto busConfig = MakeBusServerConfig(777);
    config->SetTypedConfig(DefaultProtocolName, busConfig);

    auto typedConfig = config->FindTypedConfig<TBusServerConfig>(DefaultProtocolName);
    EXPECT_EQ(busConfig, typedConfig);
    EXPECT_EQ(777, typedConfig->Port);
}

TEST(TMultiProtocolConfigTest, MutableTypedConfig)
{
    auto config = New<TMultiProtocolServerConfig>();

    // Creates the entry and lets the caller install a config in place.
    *config->MutableTypedConfig<TBusServerConfig>(DefaultProtocolName) = MakeBusServerConfig(555);
    EXPECT_EQ(555, config->FindTypedConfig<TBusServerConfig>(DefaultProtocolName)->Port);

    // The returned pointer aliases the stored config.
    (*config->MutableTypedConfig<TBusServerConfig>(DefaultProtocolName))->Port = 556;
    EXPECT_EQ(556, config->FindTypedConfig<TBusServerConfig>(DefaultProtocolName)->Port);
}

TEST(TMultiProtocolConfigTest, GetConfiguredProtocols)
{
    EXPECT_TRUE(
        New<TMultiProtocolServerConfig>()->GetConfiguredProtocols().empty());

    EXPECT_EQ(
        std::vector{std::string(DefaultProtocolName)},
        MakeServerConfig(123)->GetConfiguredProtocols());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBackendTest, BuildLocalEndpointAddress)
{
    auto* backend = TBackendRegistry::FindBackend(DefaultProtocolName);
    ASSERT_TRUE(backend);

    auto address = backend->BuildLocalEndpointAddress(std::any(MakeBusServerConfig(9000)));
    auto parsed = ParseEndpointAddress(address);
    EXPECT_EQ(DefaultProtocolName, parsed.Protocol);
    EXPECT_TRUE(parsed.Address.EndsWith(":9000")) << address;
}

TEST(TBackendTest, BuildLocalEndpointAddressThrowsWithoutPort)
{
    auto* backend = TBackendRegistry::FindBackend(DefaultProtocolName);
    ASSERT_TRUE(backend);

    EXPECT_THROW_WITH_SUBSTRING(
        backend->BuildLocalEndpointAddress(std::any(New<TBusServerConfig>())),
        "not bound to a port");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
