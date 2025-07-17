#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/auth.pb.h>

#include <ydb/core/security/xds_bootstrap_config_builder.h>

namespace NKikimr {

struct TConfigInfo {
    TString TypeConfig;
    TString Zone;
    TString NodeId;
};

NKikimrProto::TXdsBootstrap CreateXdsBootstrapConfig(const TConfigInfo& configInfo = TConfigInfo()) {
    NKikimrProto::TXdsBootstrap config;
    auto xdsServers = config.AddXdsServers();
    xdsServers->SetServerUri("xds-provider.bootstrap.net:13579");
    *(xdsServers->AddServerFeatures()) = "xds_v3";
    auto channelCreds = xdsServers->AddChannelCreds();
    channelCreds->SetType("insecure");
    if (!configInfo.TypeConfig.empty()) {
        channelCreds->SetConfig(configInfo.TypeConfig);
    }

    auto node = config.MutableNode();
    if (!configInfo.NodeId.empty()) {
        node->SetId(configInfo.NodeId);
    }
    node->SetCluster("testing");
    node->SetMeta("{\"service\":\"ydb\"}");
    if (!configInfo.Zone.empty()) {
        auto locality = node->MutableLocality();
        locality->SetZone(configInfo.Zone);
    }
    return config;
}

Y_UNIT_TEST_SUITE(XdsBootstrapConfigBuilderTest) {
    Y_UNIT_TEST(SetMetadata) {
        NKikimrProto::TXdsBootstrap config = CreateXdsBootstrapConfig({
            .TypeConfig = "{\"k1\":\"v1\"}",
            .Zone = "zone1",
            .NodeId = "50000"
        });

        TString expectedConfiguration = "{"
                                            "\"node\":{"
                                                "\"cluster\":\"testing\","
                                                "\"locality\":{"
                                                    "\"zone\":\"zone1\""
                                                "},"
                                                "\"metadata\":{"
                                                    "\"service\":\"ydb\""
                                                "},"
                                                "\"id\":\"50000\""
                                            "},"
                                            "\"xds_servers\":["
                                                "{"
                                                    "\"channel_creds\":["
                                                        "{"
                                                            "\"config\":{"
                                                                "\"k1\":\"v1\""
                                                            "},"
                                                            "\"type\":\"insecure\""
                                                        "}"
                                                    "],"
                                                    "\"server_uri\":\"xds-provider.bootstrap.net:13579\","
                                                    "\"server_features\":[\"xds_v3\"]"
                                                "}"
                                            "]"
                                        "}";

        TXdsBootstrapConfigBuilder builder(config, "dc1", "12345");
        UNIT_ASSERT_STRINGS_EQUAL(expectedConfiguration, builder.Build());
    }

    Y_UNIT_TEST(SetNodeInfoToConfig) {
        NKikimrProto::TXdsBootstrap config = CreateXdsBootstrapConfig();

        TString expectedConfiguration = "{"
                                            "\"node\":{"
                                                "\"cluster\":\"testing\","
                                                "\"locality\":{"
                                                    "\"zone\":\"dc1\""
                                                "},"
                                                "\"metadata\":{"
                                                    "\"service\":\"ydb\""
                                                "},"
                                                "\"id\":\"12345\""
                                            "},"
                                            "\"xds_servers\":["
                                                "{"
                                                    "\"channel_creds\":["
                                                        "{"
                                                            "\"type\":\"insecure\""
                                                        "}"
                                                    "],"
                                                    "\"server_uri\":\"xds-provider.bootstrap.net:13579\","
                                                    "\"server_features\":[\"xds_v3\"]"
                                                "}"
                                            "]"
                                        "}";

        TXdsBootstrapConfigBuilder builder(config, "dc1", "12345");
        UNIT_ASSERT_STRINGS_EQUAL(expectedConfiguration, builder.Build());
    }
}

} // NKikimr
