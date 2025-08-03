#include "local_partition.h"


namespace NYdb::inline Dev::NTopic::NTests {

TDriverConfig CreateConfig(ITopicTestSetup& setup, const std::string& discoveryAddr) {
    TDriverConfig config = setup.MakeDriverConfig();
    config.SetEndpoint(discoveryAddr);
    return config;
}

TWriteSessionSettings CreateWriteSessionSettings(ITopicTestSetup& setup) {
    return TWriteSessionSettings()
        .Path(setup.GetTopicPath())
        .ProducerId("test-producer")
        .PartitionId(0)
        .DirectWriteToPartition(true);
}

TReadSessionSettings CreateReadSessionSettings(ITopicTestSetup& setup) {
    return TReadSessionSettings()
        .ConsumerName(setup.GetConsumerName())
        .AppendTopics(setup.GetTopicPath());
}

void WriteMessage(ITopicTestSetup& setup, TTopicClient& client) {
    std::cerr << "=== Write message" << std::endl;

    auto writeSession = client.CreateSimpleBlockingWriteSession(CreateWriteSessionSettings(setup));
    Y_ENSURE(writeSession->Write("message"));
    writeSession->Close();
}

void ReadMessage(ITopicTestSetup& setup, TTopicClient& client, std::uint64_t expectedCommitedOffset) {
    std::cerr << "=== Read message" << std::endl;

    auto readSession = client.CreateReadSession(CreateReadSessionSettings(setup));

    std::optional<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
    Y_ENSURE(event);
    auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event.value());
    Y_ENSURE(startPartitionSession, DebugString(*event));

    startPartitionSession->Confirm();

    event = readSession->GetEvent(true);
    Y_ENSURE(event, DebugString(*event));
    auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&event.value());
    Y_ENSURE(dataReceived, DebugString(*event));

    dataReceived->Commit();

    auto& messages = dataReceived->GetMessages();
    Y_ENSURE(messages.size() == 1u);
    Y_ENSURE(messages[0].GetData() == "message");

    event = readSession->GetEvent(true);
    Y_ENSURE(event, DebugString(*event));
    auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event.value());
    Y_ENSURE(commitOffsetAck, DebugString(*event));
    Y_ENSURE(commitOffsetAck->GetCommittedOffset() == expectedCommitedOffset);
}

}
