#include "helpers/grpc_server.h"
#include "helpers/grpc_services/scripting.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(ResponseHeaders) {
    Y_UNIT_TEST(PassHeader) {
        NScripting::TMockSlyDbProxy slyDbProxy;

        std::string addr = "localhost:10000";

        auto server = StartGrpcServer(addr, slyDbProxy);

        auto config = TDriverConfig()
            .SetEndpoint(addr);
        TDriver driver(config);
        NScripting::TScriptingClient client(driver);

        auto result = client.ExecuteYqlScript("SMTH").GetValueSync();
        auto metadata = result.GetResponseMetadata();

        UNIT_ASSERT(metadata.find("key") != metadata.end());
        UNIT_ASSERT_VALUES_EQUAL(metadata.find("key")->second, "value");
    }
}
