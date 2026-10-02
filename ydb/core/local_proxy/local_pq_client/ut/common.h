#pragma once

#include <ydb/core/local_proxy/local_pq_client/local_topic_client_settings.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/deferred_publications.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace NKikimr::NKqp {
class TKikimrRunner;
}

namespace NKikimr::NKqp::NLocalTopicTests {

using namespace NYdb;
using namespace NYdb::NTopic;
using NYdb::NTopic::TReadSessionEvent;

inline constexpr TDuration TEST_TIMEOUT = TDuration::Seconds(30);

// Keeps the next continuation token while waiting for the acknowledgement.
class TTestWriter {
public:
    explicit TTestWriter(std::shared_ptr<IWriteSession> session);

    TWriteSessionEvent::TWriteAck Write(TWriteMessage message, bool encoded = false);

private:
    std::shared_ptr<IWriteSession> Session;
    std::optional<TContinuationToken> Token;
};

class TLocalTopicClientFixture : public NUnitTest::TBaseFixture {
public:
    inline static constexpr char TOPIC_PATH[] = "/Root/topic";
    inline static constexpr char CONSUMER[] = "consumer";

    TLocalTopicClientFixture();
    ~TLocalTopicClientFixture();

    void SetUp(NUnitTest::TTestContext&) override;

    TLocalTopicClientSettings LocalClientSettings() const;
    TLocalTopicSessionSettings LocalSessionSettings() const;
    static TTopicClientSettings ClientSettings();
    static TWriteSessionSettings WriteSettings(const std::string& path = TOPIC_PATH);
    static TReadSessionSettings ReadSettings(const std::string& path = TOPIC_PATH);

    void CreateTopic(const std::string& path, ui32 partitions = 1);
    std::shared_ptr<IWriteSession> CreateWriteSession(const std::string& path = TOPIC_PATH);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings = ReadSettings());

    // Use the regular SDK as an independent producer/consumer for local sessions.
    void WriteTopicMessages(const std::vector<std::string>& messages, TWriteSessionSettings settings = WriteSettings());
    void AssertTopicMessages(const std::vector<std::string>& expected, const std::string& path = TOPIC_PATH);
    void AssertTopicEndOffset(ui64 expected, const std::string& path = TOPIC_PATH);

    static TWriteSessionEvent::TEvent WaitForEvent(IWriteSession& session, TInstant deadline = TInstant::Now() + TEST_TIMEOUT);
    static TReadSessionEvent::TEvent WaitForEvent(IReadSession& session, TInstant deadline = TInstant::Now() + TEST_TIMEOUT);
    static TContinuationToken WaitForContinuationToken(IWriteSession& session);
    static void AssertSessionClosed(IWriteSession& session, EStatus expectedStatus = EStatus::SUCCESS);
    static void AssertSessionClosed(IReadSession& session, EStatus expectedStatus = EStatus::SUCCESS);
    static void CloseSession(IWriteSession& session);
    static void CloseSession(IReadSession& session);

    template <typename TEvent>
    static TEvent WaitForReadEvent(IReadSession& session, TInstant deadline = TInstant::Now() + TEST_TIMEOUT) {
        while (true) {
            auto event = WaitForEvent(session, deadline);
            if (auto* result = std::get_if<TEvent>(&event)) {
                return std::move(*result);
            }
            if (auto* start = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                start->Confirm();
            } else if (auto* stop = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                stop->Confirm();
            } else {
                UNIT_FAIL("Unexpected read event: " << DebugString(event));
            }
        }
    }

    static std::vector<TReadSessionEvent::TDataReceivedEvent::TMessage> ReadMessages(IReadSession& session, size_t count);
    static void AssertAck(const TWriteSessionEvent::TWriteAck& ack, ui64 seqNo, ui64 offset, ui64 partition = 0);

protected:
    std::unique_ptr<TKikimrRunner> Kikimr;
    std::unique_ptr<TTopicClient> TopicClient;
    std::unique_ptr<TDeferredPublishClient> DeferredClient;
};

} // namespace NKikimr::NKqp::NLocalTopicTests
