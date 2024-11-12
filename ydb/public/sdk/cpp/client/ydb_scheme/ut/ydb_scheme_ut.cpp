#include <ydb/public/sdk/cpp/client/ut/helpers/grpc_server.h>
#include <ydb/public/sdk/cpp/client/ut/helpers/grpc_services/scheme.h>

#include <ydb/public/sdk/cpp/client/ydb_scheme/descriptions/view.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NScheme;

Y_UNIT_TEST_SUITE(SchemeClient) {

    Y_UNIT_TEST(ViewQueryText) {
        constexpr const char* QueryText = "select 42";
        Ydb::Scheme::DescribeSchemeObjectResult expectedResult;
        *expectedResult.mutable_view_description()->mutable_query_text() = QueryText;
        TSchemeDummyService schemeService(expectedResult);

        constexpr const char* Address = "localhost:2135";
        auto server = StartGrpcServer(Address, schemeService);

        auto config = TDriverConfig().SetEndpoint(Address);
        TDriver driver(config);
        TSchemeClient client(driver);

        auto result = client.DescribeSchemeObject("any").ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto receivedQueryText = result.GetViewDescription().GetQueryText();
        UNIT_ASSERT_STRINGS_EQUAL(receivedQueryText, QueryText);
    }

}
