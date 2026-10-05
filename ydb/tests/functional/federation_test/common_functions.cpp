#include <ydb/tests/functional/federation_test/common_functions.h>

using namespace NYdb;
using namespace NYdb::NTopic;
namespace NFederationTests {
    TDriver MakeDriver(const TString& endpoint, const TString& database) {
    return TDriver(
        TDriverConfig()
            .SetEndpoint(endpoint)
            .SetDatabase(database)
            .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", TLOG_DEBUG).Release()))
    );
}

void WriteMessages(const TString& endpoint, const TString& database,
                   const TString& topicPath, const TString& producerId,
                   const std::vector<TString>& messages)
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

std::vector<TString> WriteLoadMessages(const TString& endpoint, const TString& database,
                     const TString& topicPath, const TString& producerId,
                     size_t count, size_t smallMessageSize, size_t bigMessageSize)
{
    TDriver driver = MakeDriver(endpoint, database);
    TTopicClient client(driver);
    auto session = client.CreateSimpleBlockingWriteSession(
        TWriteSessionSettings()
            .Path(topicPath)
            .MessageGroupId(producerId)
            .Codec(ECodec::RAW)
    );
    std::vector<TString> payloads;
    for (size_t i = 0; i < count; ++i) {
        size_t targetSize = (i % 5 == 0) ? bigMessageSize : smallMessageSize;
        TString prefix = "msg-" + std::to_string(i) + ":";
        TString payload = prefix;
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

std::map<uint64_t, TString> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount, TDuration timeout) {
    std::map<uint64_t, TString> result;
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
                result[msg.GetOffset()] = TString(msg.GetData());
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

std::map<std::pair<uint64_t, uint64_t>, TString> ReadAutoscaledTopicMessages(
                    std::shared_ptr<IReadSession> session, size_t wantCount,
                    TDuration timeout) {
    std::map<std::pair<uint64_t, uint64_t>, TString> result;
    TInstant deadline = TInstant::Now() + timeout;

    while (TInstant::Now() < deadline) {
        auto event = session->GetEvent(false);
        if (!event) {
            Sleep(TDuration::MilliSeconds(50));
            continue;
        }
        if (auto* e = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
            e->Confirm();
        } else if (auto* e = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&*event)) {
            uint64_t partitionId = e->GetPartitionSession()->GetPartitionId();
            for (const auto& msg : e->GetMessages()) {
                result[{partitionId, msg.GetOffset()}] = TString(msg.GetData());
            }
            e->Commit();
        } else if (std::holds_alternative<TSessionClosedEvent>(*event)) {
            break;
        }

        if (result.size() >= wantCount) {
            break;
        }
    }
    return result;
}

void SetClusterWriteEnabledYql(const TString& endpoint, const TString& clusterName, bool enabled)
  {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(endpoint).SetDatabase("/Root"));
        NYdb::NTable::TTableClient tableClient(driver);

        auto sessionResult = tableClient.GetSession().GetValueSync();
        UNIT_ASSERT_C(sessionResult.IsSuccess(),
            TString("GetSession failed: ") + sessionResult.GetIssues().ToString());

        const TString query1 =
            "UPDATE `/Root/PQ/Config/V2/Cluster` SET enabled = " +
            TString(enabled ? "true" : "false") +
            " WHERE name = \"" + clusterName + "\"";

        const TString query2 =
            "UPDATE `/Root/Clusters` SET Enabled = " +
            TString(enabled ? "true" : "false") +
            " WHERE Name = \"" + clusterName + "\"";

        auto result1 = sessionResult.GetSession().ExecuteDataQuery(
            query1, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();

        UNIT_ASSERT_C(result1.IsSuccess(), TString("SetClusterWriteEnabledYql failed: ") + result1.GetIssues().ToString());
        auto result2 = sessionResult.GetSession().ExecuteDataQuery(
            query2, NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx()
        ).GetValueSync();
        UNIT_ASSERT_C(result2.IsSuccess(), TString("SetClusterWriteEnabledYql failed: ") + result2.GetIssues().ToString());
        driver.Stop(true);
        Cerr << TInstant::Now() << "SetClusterWriteEnabledYql is successfull" << Endl;
  }

size_t GetActivePartitionCount(const TString& endpoint, const TString& database, const TString& topicPath) {
    TDriver driver = MakeDriver(endpoint, database);
    TTopicClient client(driver);
    auto result = client.DescribeTopic(topicPath).GetValueSync();
    size_t count = 0;
    if (result.IsSuccess()) {
        for (const auto& partition : result.GetTopicDescription().GetPartitions()) {
            if (partition.GetActive()) {
                ++count;
            }
        }
    }
    driver.Stop(true);
    return count;
}

} // namespace
