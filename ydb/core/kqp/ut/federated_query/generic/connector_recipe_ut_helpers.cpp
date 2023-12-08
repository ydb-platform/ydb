#include "connector_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NTestUtils {

    TString GetConnectorHost() {
        return GetEnv("YDB_CONNECTOR_RECIPE_GRPC_HOST", "localhost");
    }

    ui32 GetConnectorPort() {
        const TString port = GetEnv("YDB_CONNECTOR_RECIPE_GRPC_PORT");
        UNIT_ASSERT_C(port, "No connector port specified");
        return FromString<ui32>(port);
    }

    std::shared_ptr<NKikimr::NKqp::TKikimrRunner> MakeKikimrRunnerWithConnector() {
        NYql::TGenericConnectorConfig clientCfg;
        clientCfg.MutableEndpoint()->set_host(GetConnectorHost());
        clientCfg.MutableEndpoint()->set_port(GetConnectorPort());

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(true);

        auto kikimr = NKikimr::NKqp::NFederatedQueryTest::MakeKikimrRunner(
            NYql::IHTTPGateway::Make(),
            NYql::NConnector::MakeClientGRPC(clientCfg),
            nullptr,
            appCfg);
        kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        return kikimr;
    }

} // namespace NTestUtils
