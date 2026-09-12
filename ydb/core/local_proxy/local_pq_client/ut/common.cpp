#include "common.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/local_proxy/local_pq_client/local_topic_read_session.h>
#include <ydb/core/local_proxy/local_pq_client/local_topic_write_session.h>

#include <util/generic/guid.h>

namespace NKikimr::NKqp::NLocalTopicTests {

TTestWriter::TTestWriter(std::shared_ptr<IWriteSession> session)
    : Session(std::move(session))
{}

TWriteSessionEvent::TWriteAck TTestWriter::Write(TWriteMessage message, bool encoded) {
    if (!Token) {
        Token = TLocalTopicClientFixture::WaitForContinuationToken(*Session);
    }
    if (encoded) {
        Session->WriteEncoded(std::move(*Token), std::move(message));
    } else {
        Session->Write(std::move(*Token), std::move(message));
    }
    Token.reset();

    std::optional<TWriteSessionEvent::TWriteAck> ack;
    const auto deadline = TInstant::Now() + TEST_TIMEOUT;
    while (!ack || !Token) {
        auto event = TLocalTopicClientFixture::WaitForEvent(*Session, deadline);
        if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
            UNIT_ASSERT(!Token);
            Token = std::move(ready->ContinuationToken);
        } else if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
            UNIT_ASSERT(!ack);
            UNIT_ASSERT_VALUES_EQUAL(acks->Acks.size(), 1);
            ack = std::move(acks->Acks.front());
        } else {
            UNIT_FAIL("Unexpected write event: " << DebugString(event));
        }
    }
    return std::move(*ack);
}

TLocalTopicClientFixture::TLocalTopicClientFixture() = default;
TLocalTopicClientFixture::~TLocalTopicClientFixture() = default;

void TLocalTopicClientFixture::SetUp(NUnitTest::TTestContext&) {
    Kikimr = std::make_unique<TKikimrRunner>(TKikimrSettings()
        .SetWithSampleTables(false)
        .SetAuthToken(BUILTIN_ACL_ROOT)
        .SetEnableTopicDeferredPublish(true));
    TopicClient = std::make_unique<TTopicClient>(Kikimr->GetDriver());
    DeferredClient = std::make_unique<TDeferredPublishClient>(Kikimr->GetDriver());
    CreateTopic(TOPIC_PATH);
}

TLocalTopicClientSettings TLocalTopicClientFixture::LocalClientSettings() const {
    return {
        .ActorSystem = Kikimr->GetTestServer().GetRuntime()->GetActorSystem(0),
        .ChannelBufferSize = 1_MB,
    };
}

TTopicClientSettings TLocalTopicClientFixture::ClientSettings() {
    return TTopicClientSettings()
        .Database("/Root")
        .AuthToken(NACLib::TUserToken(BUILTIN_ACL_ROOT, {}).SerializeAsString());
}

TLocalTopicSessionSettings TLocalTopicClientFixture::LocalSessionSettings() const {
    return {
        .ActorSystem = LocalClientSettings().ActorSystem,
        .Database = "/Root",
        .CredentialsProvider = ClientSettings().CredentialsProviderFactory_.value()->CreateProvider(),
    };
}

TWriteSessionSettings TLocalTopicClientFixture::WriteSettings(const std::string& path) {
    return TWriteSessionSettings()
        .Path(path)
        .ProducerId("producer")
        .MessageGroupId("producer")
        .Codec(ECodec::RAW);
}

TReadSessionSettings TLocalTopicClientFixture::ReadSettings(const std::string& path) {
    return TReadSessionSettings().ConsumerName(CONSUMER).AppendTopics(path);
}

void TLocalTopicClientFixture::CreateTopic(const std::string& path, ui32 partitions) {
    const auto result = TopicClient->CreateTopic(path, TCreateTopicSettings()
        .PartitioningSettings(partitions, partitions)
        .BeginAddConsumer(CONSUMER).EndAddConsumer()
        .ClientTimeout(TEST_TIMEOUT)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

std::shared_ptr<IWriteSession> TLocalTopicClientFixture::CreateWriteSession(const std::string& path) {
    return CreateWriteSession(WriteSettings(path));
}

std::shared_ptr<IWriteSession> TLocalTopicClientFixture::CreateWriteSession(const TWriteSessionSettings& settings) {
    return CreateLocalTopicWriteSession(LocalSessionSettings(), settings);
}

std::shared_ptr<IReadSession> TLocalTopicClientFixture::CreateReadSession(const TReadSessionSettings& settings) {
    return CreateLocalTopicReadSession(LocalSessionSettings(), settings);
}

void TLocalTopicClientFixture::WriteTopicMessages(const std::vector<std::string>& messages, TWriteSessionSettings settings) {
    settings.ProducerId(CreateGuidAsString());
    if (!settings.PartitionId_) {
        settings.MessageGroupId(settings.ProducerId_);
    }
    auto session = TopicClient->CreateSimpleBlockingWriteSession(settings);
    for (const auto& message : messages) {
        UNIT_ASSERT(session->Write(TWriteMessage(message), nullptr, TEST_TIMEOUT));
    }
    UNIT_ASSERT(session->Close(TEST_TIMEOUT));
}

void TLocalTopicClientFixture::AssertTopicMessages(const std::vector<std::string>& expected, const std::string& path) {
    auto session = TopicClient->CreateReadSession(ReadSettings(path));
    const auto messages = ReadMessages(*session, expected.size());
    for (size_t i = 0; i < expected.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(messages[i].GetData(), expected[i]);
    }
    UNIT_ASSERT(session->Close(TEST_TIMEOUT));
}

void TLocalTopicClientFixture::AssertTopicEndOffset(ui64 expected, const std::string& path) {
    const auto result = TopicClient->DescribeTopic(path, TDescribeTopicSettings()
        .IncludeStats(true).ClientTimeout(TEST_TIMEOUT)).GetValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    const auto& partitions = result.GetTopicDescription().GetPartitions();
    UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
    const auto& stats = partitions[0].GetPartitionStats();
    UNIT_ASSERT(stats);
    UNIT_ASSERT_VALUES_EQUAL(stats->GetEndOffset(), expected);
}

TWriteSessionEvent::TEvent TLocalTopicClientFixture::WaitForEvent(IWriteSession& session, TInstant deadline) {
    UNIT_ASSERT_C(session.WaitEvent().Wait(deadline), "Timed out waiting for a write session event");
    auto event = session.GetEvent(false);
    UNIT_ASSERT(event);
    return std::move(*event);
}

TReadSessionEvent::TEvent TLocalTopicClientFixture::WaitForEvent(IReadSession& session, TInstant deadline) {
    UNIT_ASSERT_C(session.WaitEvent().Wait(deadline), "Timed out waiting for a read session event");
    auto event = session.GetEvent(false);
    UNIT_ASSERT(event);
    return std::move(*event);
}

TContinuationToken TLocalTopicClientFixture::WaitForContinuationToken(IWriteSession& session) {
    auto event = WaitForEvent(session);
    auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event);
    UNIT_ASSERT_C(ready, DebugString(event));
    return std::move(ready->ContinuationToken);
}

void TLocalTopicClientFixture::AssertSessionClosed(IWriteSession& session, EStatus expectedStatus) {
    auto event = WaitForEvent(session);
    const auto* closed = std::get_if<TSessionClosedEvent>(&event);
    UNIT_ASSERT_C(closed, DebugString(event));
    UNIT_ASSERT_VALUES_EQUAL_C(closed->GetStatus(), expectedStatus, closed->GetIssues().ToString());
    UNIT_ASSERT(session.Close(TDuration::Zero()));
}

void TLocalTopicClientFixture::AssertSessionClosed(IReadSession& session, EStatus expectedStatus) {
    const auto closed = WaitForReadEvent<TSessionClosedEvent>(session);
    UNIT_ASSERT_VALUES_EQUAL_C(closed.GetStatus(), expectedStatus, closed.GetIssues().ToString());
    UNIT_ASSERT(session.Close(TDuration::Zero()));
}

void TLocalTopicClientFixture::CloseSession(IWriteSession& session) {
    UNIT_ASSERT(!session.Close(TDuration::Zero()));
    // Consume the close event so the client drops its session actor id.
    AssertSessionClosed(session);
}

void TLocalTopicClientFixture::CloseSession(IReadSession& session) {
    UNIT_ASSERT(!session.Close(TDuration::Zero()));
    AssertSessionClosed(session);
}

std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> TLocalTopicClientFixture::ReadMessages(IReadSession& session, size_t count) {
    std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> result;
    const auto deadline = TInstant::Now() + TEST_TIMEOUT;
    while (result.size() < count) {
        auto event = WaitForReadEvent<TReadSessionEvent::TDataReceivedEvent>(session, deadline);
        for (auto& message : event.GetMessages()) {
            result.emplace_back(std::move(message));
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(result.size(), count);
    return result;
}

void TLocalTopicClientFixture::AssertAck(const TWriteSessionEvent::TWriteAck& ack, ui64 seqNo, ui64 offset, ui64 partition) {
    UNIT_ASSERT_VALUES_EQUAL(ack.SeqNo, seqNo);
    UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN);
    UNIT_ASSERT(ack.Stat);
    UNIT_ASSERT(ack.Details);
    UNIT_ASSERT_VALUES_EQUAL(ack.Details->Offset, offset);
    UNIT_ASSERT_VALUES_EQUAL(ack.Details->PartitionId, partition);
}

} // namespace NKikimr::NKqp::NLocalTopicTests
