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

        std::unordered_set<std::string_view> dbs;
        TString discoveryEndpont = "localhost:100";

        auto mutator = [&](Ydb::Discovery::ListEndpointsResult* proto, TStatus status, const IDiscoveryMutatorApi::TAuxInfo& aux) {
            UNIT_ASSERT_VALUES_EQUAL(discoveryEndpont, status.GetEndpoint());
            UNIT_ASSERT_VALUES_EQUAL(discoveryEndpont, aux.DiscoveryEndpoint);
            dbs.insert(aux.Database);
            Y_UNUSED(proto);
            return status;
        };
        auto driver = TDriver(
            TDriverConfig()
                .SetDatabase("db1")
                .SetEndpoint(discoveryEndpont));

        driver.AddExtension<TDiscoveryMutator>(TDiscoveryMutator::TParams(std::move(mutator)));

        auto clientSettings = NTable::TClientSettings();
        clientSettings.Database("db2");

        // By default this is sync operation
        auto client = NTable::TTableClient(driver, clientSettings);
        UNIT_ASSERT(dbs.find("db2") != dbs.end());
    }
}
