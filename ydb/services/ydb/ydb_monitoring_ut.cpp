#include "ydb_common_ut.h"

#include <ydb/library/actors/interconnect/interconnect_impl.h>

#include <ydb/core/grpc_services/db_metadata_cache.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>

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
                .SetEndpoint(location)
                .SetDatabase("/Root"));
        auto client = NYdb::NMonitoring::TMonitoringClient(connection);
        auto result = client.SelfCheck().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto& proto = NYdb::TProtoAccessor::GetProto(result);
        Cerr << proto.DebugString() << Endl;
  }

     Y_UNIT_TEST(SelfCheckWithNodesDying) {
        TKikimrWithGrpcAndRootSchema server({}, {}, {}, false, nullptr, nullptr, 1, 3);
        ui16 grpc = server.GetPort();
        TString location = TStringBuilder() << "localhost:" << grpc;
        auto connection = NYdb::TDriver(
            TDriverConfig()
                .SetEndpoint(location)
                .SetDatabase("/Root"));
        auto client = NYdb::NMonitoring::TMonitoringClient(connection);
        Sleep(TDuration::Seconds(5));
        ui32 nodeIdxBegin = 0;
        ui32 nodeIdxEnd = server.GetServer().StaticNodes() + server.GetServer().DynamicNodes();
        const TActorId killer = server.GetRuntime()->AllocateEdgeActor(0);
        for (ui32 nodeIdx = nodeIdxBegin; nodeIdx + 1 < nodeIdxEnd; ++nodeIdx) {
            Cerr << "Killing node " << server.GetRuntime()->GetNodeId(nodeIdx) << Endl;
            if (nodeIdx < server.GetServer().StaticNodes()) {
                auto cache = MakeDatabaseMetadataCacheId(server.GetRuntime()->GetNodeId(nodeIdx));
                server.GetRuntime()->Send(new IEventHandle(cache, killer, new TEvents::TEvPoison), nodeIdx);
            } else {
                server.GetServer().DestroyDynamicLocalService(nodeIdx);
            }
            Sleep(TDuration::Seconds(5));
            auto result = client.SelfCheck().GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
  }
}
