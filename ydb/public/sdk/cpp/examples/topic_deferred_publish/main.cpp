#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <iostream>

int main() {
    const std::string endpoint = "grpc://localhost:2135";
    const std::string database = "/local";
    const std::string topicPath = "my-topic";
    const std::string extPublicationId = "order-42";

    NYdb::TDriverConfig config;
    config.SetEndpoint(endpoint);
    config.SetDatabase(database);
    NYdb::TDriver driver(config);

    NYdb::NTopic::TTopicClient topicClient(driver);
    NYdb::NTopic::TDeferredPublishClient deferredClient(driver);

    // 1. BeginPublication
    auto begin = deferredClient.BeginPublication(extPublicationId).GetValueSync();
    if (!begin.IsSuccess()) {
        std::cerr << "BeginPublication failed: " << begin.GetIssues().ToString() << std::endl;
        return 1;
    }
    const auto& publication = begin.GetPublication();

    // 2. StreamWrite with deferred_publish
    auto writeSettings = NYdb::NTopic::TWriteSessionSettings()
        .Path(topicPath)
        .ProducerId("deferred-demo-producer");
    auto session = topicClient.CreateWriteSession(writeSettings);

    std::optional<NYdb::NTopic::TContinuationToken> token;
    while (!token.has_value()) {
        if (!session->WaitEvent().Wait(TDuration::Seconds(30))) {
            std::cerr << "Timeout waiting for write continuation token" << std::endl;
            return 1;
        }
        for (auto& event : session->GetEvents()) {
            if (auto* ready = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                token = std::move(ready->ContinuationToken);
            } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                std::cerr << "Write session closed: " << closed->GetIssues().ToString() << std::endl;
                return 1;
            }
        }
    }

    NYdb::NTopic::TWriteMessage message("payload");
    message.DeferredPublication(publication);
    session->Write(std::move(*token), std::move(message));

    // 3. Publish (SDK waits for write acks before the server RPC)
    auto publish = deferredClient.Publish(publication).GetValueSync();
    if (!publish.IsSuccess()) {
        std::cerr << "Publish failed: " << publish.GetIssues().ToString() << std::endl;
        return 1;
    }

    session->Close();

    std::cout << "Deferred publish demo completed for ext_publication_id=" << extPublicationId << std::endl;
    return 0;
}
