#include "connector_recipe_ut_helpers.h"

#include <util/string/cast.h>
#include <util/system/env.h>

namespace NTestUtils {

    TString GetConnectorHost() {
        return "localhost";
    }

    ui32 GetConnectorPort() {
        return 50051;
    }

    std::shared_ptr<NKikimr::NKqp::TKikimrRunner> MakeKikimrRunnerWithConnector() {
        NYql::TGenericConnectorConfig clientCfg;
        clientCfg.MutableEndpoint()->set_host(GetConnectorHost());
        clientCfg.MutableEndpoint()->set_port(GetConnectorPort());

        NKikimrConfig::TAppConfig appCfg;
        appCfg.MutableFeatureFlags()->SetEnableExternalDataSources(true);

        auto kikimr = NKikimr::NKqp::NFederatedQueryTest::MakeKikimrRunner(
            true,
            NYql::NConnector::MakeClientGRPC(clientCfg),
            nullptr,
            appCfg);
        kikimr->GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.SetEnableExternalDataSources(true);
        return kikimr;
    }

} // namespace NTestUtils
