#include "ydb_common_ut.h"

#include <ydb/public/api/grpc/ydb_monitoring_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_monitoring/monitoring.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

using namespace NYdb;

Y_UNIT_TEST_SUITE(YdbMonitoring) {
     Y_UNIT_TEST(SelfCheck) {
        TKikimrWithGrpcAndRootSchema server;
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location));
        auto client = NYdb::NMonitoring::TMonitoringClient(connection);
        auto result = client.SelfCheck().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& proto = NYdb::TProtoAccessor::GetProto(result);
        Cerr << proto.DebugString() << Endl;
  }
}
