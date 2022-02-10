#include <ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/extensions/discovery_mutator/discovery_mutator.h>
#include <ydb/public/sdk/cpp/client/ydb_extension/extension.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
 
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NYdb;
using namespace NDiscoveryMutator;

Y_UNIT_TEST_SUITE(DiscoveryMutator) {
    Y_UNIT_TEST(Simple) {

        std::unordered_set<TString> dbs;

        auto mutator = [&dbs](Ydb::Discovery::ListEndpointsResult* proto, TStatus status, const TString& database) {
            UNIT_ASSERT_VALUES_EQUAL("localhost:100", status.GetEndpoint());
            dbs.insert(database);
            Y_UNUSED(proto);
            return status;
        };
        auto driver = TDriver(
            TDriverConfig()
                .SetDatabase("db1")
                .SetEndpoint("localhost:100"));

        driver.AddExtension<TDiscoveryMutator>(TDiscoveryMutator::TParams(std::move(mutator)));

        auto clientSettings = NTable::TClientSettings();
        clientSettings.Database("db2");

        // By default this is sync operation
        auto client = NTable::TTableClient(driver, clientSettings);
        UNIT_ASSERT(dbs.find("db2") != dbs.end());
    }
}
