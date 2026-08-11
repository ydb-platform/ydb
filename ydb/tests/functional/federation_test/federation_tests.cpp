#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/admin/config_manager_admin.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/grpc/config_manager_admin.grpc.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/common.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/ydb_operation.pb.h>
#include <ydb/public/tools/federation_recipe/proto/logbroker/public/api/common/ydb_status_codes.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>

#include <cstdlib>
#include <string>
#include <vector>
#include <util/datetime/base.h>

using namespace NYdb;
using namespace NYdb::NTopic;

namespace {

const TString prodDatabasePath  = "/Root/logbroker-federation/prod";
const TString testDatabasePath  = "/Root/logbroker-federation/test";
const TString prodTopicPath = "topic";
const TString testTopicPath = "topic";
const TString consumerName  = "consumer";

TDriver MakeDriver(const std::string& endpoint, const std::string& database) {
    return TDriver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", TLOG_DEBUG).Release()))
    );
}

void WriteMessages(const std::string& endpoint, const std::string& database,
                   const std::string& topicPath, const std::string& producerId,
                   const std::vector<std::string>& messages)
{
    TDriver driver = MakeDriver(endpoint, database);
    TTopicClient client(driver);
    auto session = client.CreateSimpleBlockingWriteSession(
        TWriteSessionSettings()
            .Path(topicPath)
            .MessageGroupId(producerId)
    );
    for (const auto& msg : messages) {
        UNIT_ASSERT(session->Write(msg));
    }
    session->Close();
    driver.Stop(true);
}

std::vector<std::string> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
                                          TDuration timeout = TDuration::Seconds(30))
  {
      std::vector<std::string> result;
      bool commitAckPending = false;
      TInstant deadline = TInstant::Now() + timeout;

      while (TInstant::Now() < deadline) {
          auto event = session->GetEvent(/*block=*/false);
          if (!event) {
              Sleep(TDuration::MilliSeconds(50));
              continue;
          }
          if (auto* e = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
              e->Confirm();
          } else if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
              for (const auto& msg : e->GetMessages()) {
                  result.push_back(std::string(msg.GetData()));
              }
              e->Commit();
              commitAckPending = true;
          } else if (std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&*event)) {
              commitAckPending = false;
          } else if (std::holds_alternative<TSessionClosedEvent>(*event)) {
              break;
          }

          if (result.size() >= wantCount && !commitAckPending) {
              break;
          }
      }
      return result;
  }

using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

NLogBroker::Operations::Operation WaitOperation(AdminStub& stub,
                                                 const NLogBroker::Operations::Operation& initial,
                                                 TDuration timeout = TDuration::Seconds(30))
{
    if (initial.ready()) {
        return initial;
    }
    TInstant deadline = TInstant::Now() + timeout;
    while (TInstant::Now() < deadline) {
        Sleep(TDuration::MilliSeconds(200));
        NLogBroker::Operations::GetOperationRequest req;
        req.set_id(initial.id());
        NLogBroker::Operations::GetOperationResponse resp;
        grpc::ClientContext ctx;
        if (stub.GetOperation(&ctx, req, &resp).ok() && resp.operation().ready()) {
            return resp.operation();
        }
    }
    return initial;
}

} // namespace

struct TClusterEndpoints {
    TClusterEndpoints() {
        const char* portA = std::getenv("cluster_a_port");
        const char* portB = std::getenv("cluster_b_port");
        UNIT_ASSERT_C(portA, "cluster_a_port is not set by federation_recipe");
        UNIT_ASSERT_C(portB, "cluster_b_port is not set by federation_recipe");
        ClusterA = std::string("localhost:") + portA;
        ClusterB = std::string("localhost:") + portB;
    }

    std::string ClusterA;
    std::string ClusterB;
};

Y_UNIT_TEST_SUITE(TFederationWriteReadTest) {

    Y_UNIT_TEST(WriteAndReadOnClusterA) {
        TClusterEndpoints env;
        WriteMessages(env.ClusterA, prodDatabasePath, prodTopicPath,
                      "ut-producer-a", {"hello from cluster_a"});

        TDriver driver = MakeDriver(env.ClusterA, prodDatabasePath);
        TTopicClient client(driver);

        std::vector<std::string> messages;
        {
            TTopicClient client(driver);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(prodTopicPath))
            );
            messages = ReadMessages(session, 1);
            session->Close(TDuration::Seconds(5));
        }
        Cerr << TInstant::Now() << " Session closed" << Endl;
        driver.Stop(true);
        Cerr << TInstant::Now() << " Driver stopped" << Endl;
    }

    Y_UNIT_TEST(CreateRemoteMirrorRule) {
        const TString cmPort = std::getenv("CM_PORT");
        const TString portA = std::getenv("cluster_a_port");
        const TString portB = std::getenv("cluster_b_port");
        UNIT_ASSERT_C(cmPort, "CM_PORT is not set");
        UNIT_ASSERT_C(portA, "cluster_a_port is not set");
        UNIT_ASSERT_C(portB, "cluster_b_port is not set");

        const TString endpointA = std::string("localhost:") + portA;
        const TString endpointB = std::string("localhost:") + portB;

        auto channel = grpc::CreateChannel(
            std::string("localhost:") + cmPort,
            grpc::InsecureChannelCredentials()
        );
        auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

        NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
        req.set_comment("unittest: create remote mirror rule");

        auto* action = req.add_actions();
        auto* mirror = action->mutable_create_remote_mirror_rule();

        mirror->mutable_remote_mirror_rule()->mutable_topic()->set_path("prod/topic");
        mirror->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

        auto* props = mirror->mutable_properties();
        props->mutable_src_cluster_endpoint()->set_user_defined(
            std::string("localhost:") + portA
        );
        props->mutable_src_database()->set_user_defined("/Root/logbroker-federation/prod");
        props->mutable_src_topic()->set_user_defined("topic");
        props->mutable_src_consumer()->set_user_defined("consumer");
        props->mutable_credentials()->set_oauth_token("root@builtin");

        Cerr << TInstant::Now() << " Executing command" << Endl;

        NLogBroker::ExecuteModifyCommandsResponse resp;
        {
            grpc::ClientContext ctx;
            auto status = stub->ExecuteModifyCommands(&ctx, req, &resp);
            UNIT_ASSERT_C(status.ok(), status.error_message());
        }
        Cerr << TInstant::Now() << " waiting for cm operation" << Endl;
        auto op = WaitOperation(*stub, resp.operation());
        UNIT_ASSERT_C(op.ready(), "operation never became ready");
        UNIT_ASSERT_VALUES_EQUAL_C((int)op.status(), (int)NLogBroker::StatusIds::SUCCESS, "CM returned non-success status");

        const std::vector<std::string> written = {"mirror-msg-0", "mirror-msg-1", "mirror-msg-2"};
        WriteMessages(endpointA, prodDatabasePath, prodTopicPath, "mirror-producer", written);

        TDriver driverB = MakeDriver(endpointB, prodDatabasePath);
        std::vector<std::string> received;
        {
            TTopicClient client(driverB);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(prodTopicPath))
            );
            received = ReadMessages(session, written.size(), TDuration::Seconds(60));
            session->Close(TDuration::Seconds(5));
        }
        driverB.Stop(true);

        UNIT_ASSERT_VALUES_EQUAL_C(received.size(), written.size(),
            "Expected " + std::to_string(written.size()) + " mirrored messages on cluster_b, got " + std::to_string(received.size()));

        for (size_t i = 0; i < written.size(); i++) {
            UNIT_ASSERT_EQUAL(written[i], received[i]);
        }



      }
}
