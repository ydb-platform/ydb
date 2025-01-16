#include "helpers/grpc_server.h"
#include "helpers/grpc_services/scripting.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NScripting;

Y_UNIT_TEST_SUITE(ResponseHeaders) {
    Y_UNIT_TEST(PassHeader) {
        TMockSlyDbProxy slyDbProxy;

        TString addr = "localhost:2135";

        auto server = StartGrpcServer(addr, slyDbProxy);

        auto config = TDriverConfig()
            .SetEndpoint(addr);
        TDriver driver(config);
        TScriptingClient client(driver);

        auto result = client.ExecuteYqlScript("SMTH").GetValueSync();
        auto metadata = result.GetResponseMetadata();

        UNIT_ASSERT(metadata.find("key") != metadata.end());
        UNIT_ASSERT_VALUES_EQUAL(metadata.find("key")->second, "value");
    }
}
