#include <ydb/tests/functional/federation_test/common_functions.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NFederationTests {
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

std::map<uint64_t, std::string> ReadMessages(std::shared_ptr<IReadSession> session, size_t wantCount, TDuration timeout) {
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

std::map<std::pair<uint64_t, uint64_t>, std::string> ReadAutoscaledTopicMessages(
                    std::shared_ptr<IReadSession> session, size_t wantCount,
                    TDuration timeout) {
    std::map<std::pair<uint64_t, uint64_t>, std::string> result;
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
                result[{partitionId, msg.GetOffset()}] = std::string(msg.GetData());
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

using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

NLogBroker::Operations::Operation WaitOperation(AdminStub& stub, const NLogBroker::Operations::Operation& initial, TDuration timeout)
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

void ExecCmRequest(AdminStub& stub, NLogBroker::NAdmin::ExecuteModifyCommandsRequest& req, const TString& comment)
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

void CmCreateTopic(AdminStub& stub, const std::string& cmPath, const TString& comment, bool autoSplit)
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

    if (autoSplit) {
        action->mutable_create_topic()->mutable_properties()->mutable_auto_partitioning_strategy()->set_user_defined("up");
        action->mutable_create_topic()->mutable_properties()->mutable_max_partitions_count()->set_user_defined(4);
        action->mutable_create_topic()->mutable_properties()->mutable_auto_partitioning_up_utilization_percent()->set_user_defined(50);
        action->mutable_create_topic()->mutable_properties()->mutable_auto_partitioning_stabilization_window_seconds()->set_user_defined(10);
        action->mutable_create_topic()->mutable_admin_properties()->mutable_max_partition_write_speed()->set_user_defined(1_MB);
    }

    ExecCmRequest(stub, req, comment);
}

void SetClusterWriteEnabled(AdminStub& stub, const std::string& clusterName, bool enabled) {
    NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
    req.set_comment(std::string(enabled ? "enable" : "disable") + " writes on " + clusterName);
    auto* action = req.add_actions();
    action->mutable_update_cluster()->set_name(clusterName);
    action->mutable_update_cluster()->mutable_properties()->mutable_write_enabled()->set_user_defined(enabled);
    ExecCmRequest(stub, req, "SetClusterWriteEnabled");
}

size_t GetActivePartitionCount(const std::string& endpoint, const std::string& database, const std::string& topicPath) {
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
