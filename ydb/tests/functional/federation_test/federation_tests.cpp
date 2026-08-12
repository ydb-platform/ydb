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
const TString gapTopicPath = "gap-topic";
const TString gapTopicCMPath = "/prod/gap-topic";
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

std::vector<std::string> WriteLoadMessages(const std::string& endpoint, const std::string& database,
                     const std::string& topicPath, const std::string& producerId,
                     size_t count = 10000, size_t smallMessageSize = 2_MB, size_t bigMessageSize = 12_MB)
{
    TDriver driver = MakeDriver(endpoint, database);
    TTopicClient client(driver);
    auto session = client.CreateSimpleBlockingWriteSession(
        TWriteSessionSettings()
            .Path(topicPath)
            .MessageGroupId(producerId)
            .Codec(ECodec::RAW)
    );
    std::vector<std::string> payloads;
    for (size_t i = 0; i < count; ++i) {
        size_t targetSize = (i % 5 == 0) ? bigMessageSize : smallMessageSize;
        std::string prefix = "msg-" + std::to_string(i) + ":";
        std::string payload = prefix;
        if (payload.size() < targetSize) {
            payload.append(targetSize - payload.size(), '-');
        }
        UNIT_ASSERT_C(session->Write(payload),
            "Verifiable write failed at index " + std::to_string(i));
        payloads.push_back(std::move(payload));
    }
    session->Close();
    driver.Stop(true);
    return payloads;
}

std::map<uint64_t, std::string> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
                                          TDuration timeout = TDuration::Seconds(30))
  {
      std::map<uint64_t, std::string> result;
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
                  result[msg.GetOffset()] = std::string(msg.GetData());
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

void ExecCmRequest(AdminStub& stub, NLogBroker::NAdmin::ExecuteModifyCommandsRequest& req,
            const TString& comment)
{
    NLogBroker::ExecuteModifyCommandsResponse resp;
    grpc::ClientContext ctx;
    auto grpcStatus = stub.ExecuteModifyCommands(&ctx, req, &resp);
    UNIT_ASSERT_C(grpcStatus.ok(),
        comment + ": gRPC error: " + grpcStatus.error_message());

    auto op = WaitOperation(stub, resp.operation());
    UNIT_ASSERT_C(op.ready(), comment + ": operation never became ready");
    UNIT_ASSERT_C((int)op.status() == (int)NLogBroker::StatusIds::SUCCESS,
        comment + ": CM status " + std::to_string((int)op.status()));
}

void CmCreateTopic(AdminStub& stub, const std::string& cmPath, const TString& comment)
{
    NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
    req.set_comment(comment);
    // req.mutable_credentials()->set_oauth_token("test-token");

    auto* action = req.add_actions();
    action->mutable_create_topic()->mutable_path()->set_path(cmPath);
    action->mutable_create_topic()->set_parent_template("default");
    action->mutable_create_topic()->mutable_properties()->mutable_partitions_count()->set_user_defined(1);
    action->mutable_create_topic()->mutable_properties()->mutable_auto_partitioning_strategy()->set_user_defined("disabled");
    action->mutable_create_topic()->mutable_properties()->mutable_supported_codecs()->set_user_defined("raw");

    ExecCmRequest(stub, req, comment);
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
        TString writtenMessage = "hello from cluster_a";
        WriteMessages(env.ClusterA, prodDatabasePath, prodTopicPath,
                      "ut-producer-a", {writtenMessage});

        TDriver driver = MakeDriver(env.ClusterA, prodDatabasePath);
        TTopicClient client(driver);

        std::map<uint64_t, std::string> messages;
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
        UNIT_ASSERT_EQUAL(writtenMessage, messages[0]);
    }

    // Y_UNIT_TEST(SimpleRemoteMirrorRuleWorks) {
    //     const TString cmPort = std::getenv("CM_PORT");
    //     const TString portA = std::getenv("cluster_a_port");
    //     const TString portB = std::getenv("cluster_b_port");
    //     UNIT_ASSERT_C(cmPort, "CM_PORT is not set");
    //     UNIT_ASSERT_C(portA, "cluster_a_port is not set");
    //     UNIT_ASSERT_C(portB, "cluster_b_port is not set");

    //     const TString endpointA = std::string("localhost:") + portA;
    //     const TString endpointB = std::string("localhost:") + portB;

    //     auto channel = grpc::CreateChannel(
    //         std::string("localhost:") + cmPort,
    //         grpc::InsecureChannelCredentials()
    //     );
    //     auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

    //     NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
    //     req.set_comment("unittest: create remote mirror rule");

    //     auto* action = req.add_actions();
    //     auto* mirror = action->mutable_create_remote_mirror_rule();

    //     mirror->mutable_remote_mirror_rule()->mutable_topic()->set_path("prod/topic");
    //     mirror->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

    //     auto* props = mirror->mutable_properties();
    //     props->mutable_src_cluster_endpoint()->set_user_defined(
    //         std::string("localhost:") + portA
    //     );
    //     props->mutable_src_database()->set_user_defined("/Root/logbroker-federation/prod");
    //     props->mutable_src_topic()->set_user_defined("topic");
    //     props->mutable_src_consumer()->set_user_defined("consumer");
    //     props->mutable_credentials()->set_oauth_token("root@builtin");

    //     Cerr << TInstant::Now() << " Executing command" << Endl;

    //     NLogBroker::ExecuteModifyCommandsResponse resp;
    //     {
    //         grpc::ClientContext ctx;
    //         auto status = stub->ExecuteModifyCommands(&ctx, req, &resp);
    //         UNIT_ASSERT_C(status.ok(), status.error_message());
    //     }
    //     Cerr << TInstant::Now() << " waiting for cm operation" << Endl;
    //     auto op = WaitOperation(*stub, resp.operation());
    //     UNIT_ASSERT_C(op.ready(), "operation never became ready");
    //     UNIT_ASSERT_VALUES_EQUAL_C((int)op.status(), (int)NLogBroker::StatusIds::SUCCESS, "CM returned non-success status");

    //     const std::vector<std::string> written = {"mirror-msg-0", "mirror-msg-1", "mirror-msg-2"};
    //     WriteMessages(endpointA, prodDatabasePath, prodTopicPath, "mirror-producer", written);

    //     TDriver driverB = MakeDriver(endpointB, prodDatabasePath);
    //     std::vector<std::string> received;
    //     {
            // TTopicClient client(driverB);
            // auto session = client.CreateReadSession(
            //     TReadSessionSettings()
            //         .ConsumerName(consumerName)
            //         .AppendTopics(TTopicReadSettings(prodTopicPath))
            // );
    //         received = ReadMessages(session, written.size(), TDuration::Seconds(60));
    //         session->Close(TDuration::Seconds(5));
    //     }
    //     driverB.Stop(true);

    //     UNIT_ASSERT_VALUES_EQUAL_C(received.size(), written.size(),
    //         "Expected " + std::to_string(written.size()) + " mirrored messages on cluster_b, got " + std::to_string(received.size()));

    //     for (size_t i = 0; i < written.size(); i++) {
    //         UNIT_ASSERT_EQUAL(written[i], received[i]);
    //     }
    // }

    Y_UNIT_TEST(CreateRemoteMirrorRuleWithGaps) {
        const TString cmPort = std::getenv("CM_PORT");
        const TString portA = std::getenv("cluster_a_port");
        const TString portB = std::getenv("cluster_b_port");
        UNIT_ASSERT_C(cmPort, "CM_PORT is not set");
        UNIT_ASSERT_C(portA, "cluster_a_port is not set");
        UNIT_ASSERT_C(portB, "cluster_b_port is not set");

        const TString endpointA = std::string("localhost:") + portA;
        const TString endpointB = std::string("localhost:") + portB;

        const TString kSrcTopicCmPath = "prod/gaps-src-topic";
        const TString kSrcTopicYdbPath = "gaps-src-topic";

        const TString kDstTopicCmPath = "prod/gaps-dst-topic";
        const TString kDstTopicYdbPath = "gaps-dst-topic";

        auto channel = grpc::CreateChannel(
            std::string("localhost:") + cmPort,
            grpc::InsecureChannelCredentials()
        );
        auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

        CmCreateTopic(*stub, kSrcTopicCmPath, "unittest: create gaps-src-topic");
        CmCreateTopic(*stub, kDstTopicCmPath, "unittest: create gaps-dst-topic");

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: set retention=1s on gaps-src-topic");
            req.set_token("root@builtin");

            auto* action = req.add_actions();
            action->mutable_update_topic()->mutable_path()->set_path(kSrcTopicCmPath);
            action->mutable_update_topic()->mutable_properties()->mutable_retention_period_sec()->set_user_defined(1);
            action->mutable_update_topic()->mutable_admin_properties()->mutable_max_partition_write_speed()->set_user_defined(40_MB);

            ExecCmRequest(*stub, req, "set retention on src topic");
        }

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: set write quota=10KB/s on gaps-dst-topic");
            req.set_token("root@builtin");

            auto* action = req.add_actions();
            action->mutable_update_topic()->mutable_path()->set_path(kDstTopicCmPath);
            action->mutable_update_topic()->mutable_admin_properties()->mutable_max_partition_write_speed()->set_user_defined(5_MB);

            ExecCmRequest(*stub, req, "set write quota on dst topic");
        }

        // {
        //     TDriver driver = MakeDriver(endpointA, prodDatabasePath);
        //     TTopicClient client(driver);
        //     auto result = client.AlterTopic(
        //         kSrcTopicYdbPath,
        //         TAlterTopicSettings().SetRetentionPeriod(TDuration::Seconds(1))
        //     ).GetValueSync();
        //     UNIT_ASSERT_C(result.IsSuccess(),
        //         "AlterTopic retention on cluster_a failed: " + result.GetIssues().ToString());
        //     driver.Stop(true);
        // }

        // {
        //     TDriver driver = MakeDriver(endpointB, prodDatabasePath);
        //     TTopicClient client(driver);
        //     const uint64_t kDstWriteQuota = 10 * 1024; // 10 KB/s
        //     auto result = client.AlterTopic(
        //         kDstTopicYdbPath,
        //         TAlterTopicSettings()
        //             .SetPartitionWriteSpeedBytesPerSecond(kDstWriteQuota)
        //             .SetPartitionWriteBurstBytes(kDstWriteQuota)
        //     ).GetValueSync();
        //     UNIT_ASSERT_C(result.IsSuccess(),
        //         "AlterTopic write quota on cluster_b failed: " + result.GetIssues().ToString());
        //     driver.Stop(true);
        // }

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: create mirror rule for gap test");

            auto* action = req.add_actions();
            auto* rule = action->mutable_create_remote_mirror_rule();
            rule->mutable_remote_mirror_rule()->mutable_topic()->set_path(kDstTopicCmPath);
            rule->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

            rule->mutable_properties()->mutable_src_cluster_endpoint()->set_user_defined(endpointA);
            rule->mutable_properties()->mutable_src_database()->set_user_defined(prodDatabasePath);
            rule->mutable_properties()->mutable_src_topic()->set_user_defined(kSrcTopicYdbPath);
            rule->mutable_properties()->mutable_src_consumer()->set_user_defined(consumerName);
            rule->mutable_properties()->mutable_credentials()->set_oauth_token("root@builtin");

            ExecCmRequest(*stub, req, "create mirror rule");
        }

        Sleep(TDuration::Seconds(30));

        std::vector<std::string> writtenPayloads = WriteLoadMessages(endpointA, prodDatabasePath, kSrcTopicYdbPath, "gaps-producer", 100);
        Sleep(TDuration::Seconds(5));

        {
            TDriver driverB = MakeDriver(endpointB, prodDatabasePath);
            TTopicClient client(driverB);
                auto session = client.CreateReadSession(
                    TReadSessionSettings()
                        .ConsumerName(consumerName)
                        .AppendTopics(TTopicReadSettings(kDstTopicYdbPath))
                );

            std::map<uint64_t, std::string> mirroredMessages = ReadMessages(session, 1000, TDuration::Seconds(10));
            Cerr << TInstant::Now() << " WrittenMessages.size()=" << writtenPayloads.size() << ", mirroredMessages.size()=" << mirroredMessages.size() << Endl;
            UNIT_ASSERT(mirroredMessages.size() < writtenPayloads.size());
            for (auto& [offset, data] : mirroredMessages) {
                Cerr << "Offset=" << offset << " mirrored: " << data.substr(0, 10) << ", size=" << data.size() << Endl;
                Cerr << "Offset=" << offset << " written: " << writtenPayloads[offset].substr(0, 10) << ", size=" << writtenPayloads[offset].size() << Endl;
                Cerr << "~~~~~~~~~~~~~" << Endl;
                UNIT_ASSERT_EQUAL(writtenPayloads[offset], data);
            }
        }
    }
}
