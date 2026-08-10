#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/testing/unittest/registar.h>

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

std::vector<std::string> DrainMessages(std::shared_ptr<IReadSession> session, size_t wantCount,
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
            messages = DrainMessages(session, 1);
            session->Close(TDuration::Seconds(5));
        }
        Cerr << TInstant::Now() << " Session closed" << Endl;
        driver.Stop(true);
        Cerr << TInstant::Now() << " Driver stopped" << Endl;

    }

    // Y_UNIT_TEST(WriteAndReadOnClusterB) {
    //     TClusterEndpoints env;
    //     WriteMessages(env.ClusterB, prodDatabasePath, prodTopicPath,
    //                   "ut-producer-b", {"hello from cluster_b"});

    //     TDriver driver = MakeDriver(env.ClusterB, prodDatabasePath);
    //     TTopicClient client(driver);

    //     auto session = client.CreateReadSession(
    //         TReadSessionSettings()
    //             .ConsumerName(consumerName)
    //             .AppendTopics(prodTopicPath)
    //     );
    //     auto messages = DrainMessages(session, 1);
    //     UNIT_ASSERT_C(!messages.empty(), "No messages received from cluster_b prod topic");
    //     session->Close();
    //     driver.Stop(true);
    // }

    // Y_UNIT_TEST(WriteAndReadMultipleMessages) {
    //     TClusterEndpoints env;
    //     const std::vector<std::string> written = {"msg-0", "msg-1", "msg-2"};
    //     WriteMessages(env.ClusterA, prodDatabasePath, prodTopicPath,
    //                   "ut-producer-multi", written);

    //     TDriver driver = MakeDriver(env.ClusterA, prodDatabasePath);
    //     TTopicClient client(driver);

    //     auto session = client.CreateReadSession(
    //         TReadSessionSettings()
    //             .ConsumerName(consumerName)
    //             .AppendTopics(prodTopicPath)
    //     );
    //     auto messages = DrainMessages(session, written.size());
    //     UNIT_ASSERT_C(messages.size() >= written.size(),
    //                   "Expected at least " + std::to_string(written.size()) +
    //                   " messages, got " + std::to_string(messages.size()));
    //     session->Close();
    //     driver.Stop(true);
    // }

    // Y_UNIT_TEST(WriteAndReadTestAccountTopic) {
    //     TClusterEndpoints env;
    //     WriteMessages(env.ClusterA, testDatabasePath, testTopicPath,
    //                   "ut-producer-test-account", {"hello test account"});

    //     TDriver driver = MakeDriver(env.ClusterA, testDatabasePath);
    //     TTopicClient client(driver);

    //     auto session = client.CreateReadSession(
    //         TReadSessionSettings()
    //             .ConsumerName(consumerName)
    //             .AppendTopics(testTopicPath)
    //     );
    //     auto messages = DrainMessages(session, 1);
    //     UNIT_ASSERT_C(!messages.empty(), "No messages received from test account topic");
    //     session->Close();
    //     driver.Stop(true);
    // }

}
