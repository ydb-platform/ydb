#pragma once
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/tools/federation_recipe/proto/config_manager_admin.pb.h>
#include <ydb/public/tools/federation_recipe/proto/config_manager_admin_service.grpc.pb.h>
#include <ydb/public/tools/federation_recipe/proto/common.pb.h>
#include <ydb/public/tools/federation_recipe/proto/ydb_operation.pb.h>
#include <ydb/public/tools/federation_recipe/proto/ydb_status_codes.pb.h>
#include <library/cpp/testing/unittest/registar.h>


using namespace NYdb;
using namespace NYdb::NTopic;
using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

namespace NFederationTests {

struct TClusterEndpoints {
    TClusterEndpoints() {
        const char* rawCmPort = std::getenv("CM_PORT");
        const char* rawPortA = std::getenv("cluster_a_port");
        const char* rawPortB = std::getenv("cluster_b_port");
        UNIT_ASSERT_C(rawCmPort, "CM_PORT is not set");
        UNIT_ASSERT_C(rawPortA, "cluster_a_port is not set");
        UNIT_ASSERT_C(rawPortB, "cluster_b_port is not set");
        EndpointA = TStringBuilder() << "localhost:" << rawPortA;
        EndpointB = TStringBuilder() << "localhost:" << rawPortB;
        EndpointCM = TStringBuilder() << "localhost:" << rawCmPort;
    }

    TString EndpointA;
    TString EndpointB;
    TString EndpointCM;
};

TDriver MakeDriver(const TString& endpoint, const TString& database);

void WriteMessages(const TString& endpoint, const TString& database,
                   const TString& topicPath, const TString& producerId,
                   const std::vector<TString>& messages);

std::vector<TString> WriteLoadMessages(const TString& endpoint, const TString& database,
                     const TString& topicPath, const TString& producerId,
                     size_t count = 10000, size_t smallMessageSize = 2_MB, size_t bigMessageSize = 12_MB);

std::map<uint64_t, TString> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
                                          TDuration timeout = TDuration::Seconds(30));

std::map<std::pair<uint64_t, uint64_t>, TString> ReadAutoscaledTopicMessages(std::shared_ptr<IReadSession> session, size_t wantCount, TDuration timeout = TDuration::Seconds(60));

void SetClusterWriteEnabledYql(const TString& endpoint, const TString& clusterName, bool enabled);

size_t GetActivePartitionCount(const TString& endpoint, const TString& database, const TString& topicPath);

} // namespace
