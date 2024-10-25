#include "helpers/grpc_server.h"
#include "helpers/grpc_services/view.h"

#include <ydb/public/sdk/cpp/client/draft/ydb_view.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NView;

Y_UNIT_TEST_SUITE(ViewClient) {
    Y_UNIT_TEST(Basic) {
        TString addr = "localhost:2135";
        TViewDummyService viewService;

        auto server = StartGrpcServer(addr, viewService);

        auto config = TDriverConfig().SetEndpoint(addr);
        TDriver driver(config);
        TViewClient client(driver);

        auto result = client.DescribeView("any").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto queryText = result.GetViewDescription().GetQueryText();
        UNIT_ASSERT_STRINGS_EQUAL(queryText, DummyQueryText);
    }
}
