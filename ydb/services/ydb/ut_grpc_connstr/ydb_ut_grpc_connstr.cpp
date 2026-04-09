#include <ydb/services/ydb/ut_common/ydb_ut_test_includes.h>
#include <ydb/services/ydb/ut_common/ydb_ut_common.h>
#include <ydb/services/ydb/ydb_common_ut.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

void IncorrectConnectionStringPending(const std::string& incorrectLocation) {
    auto connection = NYdb::TDriver(incorrectLocation);
    auto client = NYdb::NTable::TTableClient(connection);
    auto session = client.CreateSession().ExtractValueSync().GetSession();
}


Y_UNIT_TEST_SUITE(GrpcConnectionStringParserTest) {
    Y_UNIT_TEST(NoDatabaseFlag) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        bool done = false;

        {
            std::string location = TStringBuilder() << "localhost:" << grpc;

            // by default, location won't have database path
            auto connection = NYdb::TDriver(location);
            auto client = NYdb::NTable::TTableClient(connection);
            auto session = client.CreateSession().ExtractValueSync().GetSession();

            done = true;
        }

        UNIT_ASSERT(done);
    }

    Y_UNIT_TEST(IncorrectConnectionString) {
        TString incorrectLocation = "thisIsNotURL::::";
        UNIT_CHECK_GENERATED_EXCEPTION(IncorrectConnectionStringPending(incorrectLocation), std::exception);
    }

    Y_UNIT_TEST(CommonClientSettingsFromConnectionString) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();

        bool done = false;

        {
            TString location = TStringBuilder() << "localhost:" << grpc;

            // by default, location won't have database path
            auto settings = GetClientSettingsFromConnectionString(location);

            done = true;
        }

        UNIT_ASSERT(done);
    }
}

} // namespace NKikimr

