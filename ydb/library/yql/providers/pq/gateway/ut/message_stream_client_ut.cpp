#include <ydb/library/yql/providers/abstract/message_stream/message_stream_client.h>
#include <ydb/library/testlib/pq_helpers/mock_pq_gateway.h>
#include <ydb/library/yql/providers/pq/gateway/clients/message_stream/yql_pq_message_stream_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql {
namespace {

struct TObservedPartition final : NYdb::NTopic::TPartitionSessionControl {
    TObservedPartition() {
        PartitionId = 7;
        PartitionSessionId = 42;
    }

    void RequestStatus() override {}
    void Commit(ui64, ui64) override {}
    void ConfirmCreate(std::optional<ui64> offset, std::optional<ui64>, std::optional<ui64>) override {
        ++Starts;
        StartOffset = offset;
    }
    void ConfirmDestroy() override {}
    void ConfirmEnd(std::span<const ui32> children) override {
        ++Ends;
        Children.assign(children.begin(), children.end());
    }

    std::optional<ui64> StartOffset;
    ui32 Starts = 0;
    ui32 Ends = 0;
    std::vector<ui32> Children;
};

struct TStreamFixture {
    NYdb::TDriver Driver{NYdb::TDriverConfig().SetEndpoint("localhost:1")};
    NTestUtils::IMockPqGateway::TPtr Gateway = NTestUtils::CreateMockPqGateway();
    std::shared_ptr<NFq::IMessageStreamClient> Client = Gateway->GetTopicClient("topic", Driver, Gateway->GetTopicClientSettings());
    std::shared_ptr<NFq::IMessageStreamReadSession> Session = Client->CreateReadSession({
        .PartitionIds = {NFq::TMessageStreamPartitionId{7}}});
    NTestUtils::IMockPqReadSession::TPtr Mock = Gateway->ExtractReadSession("topic");
};

} // namespace

Y_UNIT_TEST_SUITE(TMessageStreamContract) {
    Y_UNIT_TEST(ValidateReadSettings) {
        NFq::TMessageStreamReadSessionSettings settings;
        UNIT_ASSERT_EXCEPTION(settings.Validate(), NFq::TMessageStreamException);
        settings.Consumer = "consumer";
        settings.Validate(); // Managed assignment with a consumer.
        settings.Consumer = "";
        UNIT_ASSERT_EXCEPTION(settings.Validate(), NFq::TMessageStreamException);
        settings.Consumer.reset();
        settings.PartitionIds = {NFq::TMessageStreamPartitionId{7}};
        settings.Validate(); // Explicit assignment without a consumer.
        settings.PartitionIds.push_back(NFq::TMessageStreamPartitionId{7});
        UNIT_ASSERT_EXCEPTION(settings.Validate(), NFq::TMessageStreamException);
    }

    Y_UNIT_TEST(ClientsAreBoundToDifferentStreams) {
        NYdb::TDriver driver{NYdb::TDriverConfig().SetEndpoint("localhost:1")};
        auto gateway = NTestUtils::CreateMockPqGateway();
        UNIT_ASSERT_EXCEPTION(gateway->GetTopicClient("", driver, {}), NFq::TMessageStreamException);
        auto first = gateway->GetTopicClient("first", driver, {});
        auto second = gateway->GetTopicClient("second", driver, {});
        UNIT_ASSERT_VALUES_EQUAL(first->GetStream(), "first");
        UNIT_ASSERT_VALUES_EQUAL(second->GetStream(), "second");
        const NFq::TMessageStreamReadSessionSettings settings{.PartitionIds = {{0}}};
        auto firstSession = first->CreateReadSession(settings);
        auto secondSession = second->CreateReadSession(settings);
        auto firstMock = gateway->ExtractReadSession("first");
        auto secondMock = gateway->ExtractReadSession("second");
        firstMock->AddDataReceivedEvent(0, "first payload");
        secondMock->AddDataReceivedEvent(0, "second payload");
        const auto firstEvents = firstSession->GetEvents({});
        const auto secondEvents = secondSession->GetEvents({});
        UNIT_ASSERT_VALUES_EQUAL(std::get<NFq::TMessageStreamDataEvent>(firstEvents.at(0)).Records.at(0).Data.value(), "first payload");
        UNIT_ASSERT_VALUES_EQUAL(std::get<NFq::TMessageStreamDataEvent>(secondEvents.at(0)).Records.at(0).Data.value(), "second payload");
        firstSession->Close().GetValueSync();
        secondSession->Close().GetValueSync();
    }

    Y_UNIT_TEST(MockClientRetainsGatewayAndBoundMetadata) {
        NYdb::TDriver driver{NYdb::TDriverConfig().SetEndpoint("localhost:1")};
        NTestUtils::TMockPqGatewaySettings config;
        config.Topics["first"].PartitionCount = 2;
        config.Topics["second"].PartitionCount = 3;
        auto gateway = NTestUtils::CreateMockPqGateway(config);
        auto first = gateway->GetTopicClient("first", driver, {});
        auto second = gateway->GetTopicClient("second", driver, {});
        gateway.Reset();
        UNIT_ASSERT_VALUES_EQUAL(first->DescribeStream().GetValueSync().Value.Partitions.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(second->DescribeStream().GetValueSync().Value.Partitions.size(), 3);
        auto session = first->CreateReadSession({.PartitionIds = {{1}}});
        session->Close().GetValueSync();
    }

    Y_UNIT_TEST(MockRejectsUnsupportedOperations) {
        NYdb::TDriver driver{NYdb::TDriverConfig().SetEndpoint("localhost:1")};
        auto gateway = NTestUtils::CreateMockPqGateway();
        auto client = gateway->GetTopicClient("topic", driver, {});
        UNIT_ASSERT(client->DescribeConsumer("consumer").GetValueSync().Status == NFq::EMessageStreamStatus::Unsupported);
        UNIT_ASSERT(client->DescribePartition({0}).GetValueSync().Status == NFq::EMessageStreamStatus::Unsupported);
        UNIT_ASSERT(client->CommitPosition({0}, "consumer", 1).GetValueSync().Status == NFq::EMessageStreamStatus::Unsupported);
        for (const NFq::TMessageStreamReadSessionSettings& settings : {
            NFq::TMessageStreamReadSessionSettings{.Consumer = "consumer"},
            NFq::TMessageStreamReadSessionSettings{.PartitionIds = {{0}, {1}}},
        }) {
            try {
                client->CreateReadSession(settings);
                UNIT_FAIL("Unsupported assignment must fail before creating a session");
            } catch (const NFq::TMessageStreamException& e) {
                UNIT_ASSERT(e.GetStatus() == NFq::EMessageStreamStatus::Unsupported);
            }
            UNIT_ASSERT(!gateway->ExtractReadSession("topic"));
        }
    }

    Y_UNIT_TEST(ResultRequiresExplicitSuccess) {
        using TResult = NFq::TMessageStreamResult<NFq::TMessageStreamPartitionDescription>;
        UNIT_ASSERT(!TResult{}.IsSuccess());
        const auto result = TResult::Success({.PartitionId = {7}, .StartOffset = 2, .EndOffset = 5});
        UNIT_ASSERT(result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.Value.PartitionId.Value, 7);
        UNIT_ASSERT(!TResult::Failure(NFq::EMessageStreamStatus::Unsupported).IsSuccess());
        UNIT_ASSERT_EXCEPTION(TResult::Failure(NFq::EMessageStreamStatus::Success), NFq::TMessageStreamException);
    }

    Y_UNIT_TEST(NullableAttributesPreserveOrderAndDuplicates) {
        NFq::TMessageStreamRecord record;
        record.Attributes = {{"key", std::nullopt}, {"key", TString()}, {"key", TString("value")}};
        UNIT_ASSERT(!record.Attributes[0].Value);
        UNIT_ASSERT(record.Attributes[1].Value && record.Attributes[1].Value->empty());
        UNIT_ASSERT_VALUES_EQUAL(*record.Attributes[2].Value, "value");
        for (const auto& attribute : record.Attributes) {
            UNIT_ASSERT_VALUES_EQUAL(attribute.Name, "key");
        }
    }

    Y_UNIT_TEST(OffsetResetPolicyIsExplicit) {
        NFq::TMessageStreamReadSessionSettings settings{.PartitionIds = {{7}}};
        UNIT_ASSERT(settings.OffsetResetPolicy == NFq::EMessageStreamOffsetResetPolicy::Earliest);
        ToSdkReadSettings("topic", settings);
        for (const auto policy : {NFq::EMessageStreamOffsetResetPolicy::Error, NFq::EMessageStreamOffsetResetPolicy::Latest}) {
            settings.OffsetResetPolicy = policy;
            try {
                ToSdkReadSettings("topic", settings);
                UNIT_FAIL("Unsupported policy must not fall back to Earliest");
            } catch (const NFq::TMessageStreamException& e) {
                UNIT_ASSERT(e.GetStatus() == NFq::EMessageStreamStatus::Unsupported);
            }
        }
    }

    Y_UNIT_TEST(ExhaustionConfirmationReachesSdkAfterProcessing) {
        TStreamFixture fixture;
        auto native = MakeIntrusive<TObservedPartition>();
        fixture.Mock->AddEvent(NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent(native, {}, {8, 9}));
        auto events = fixture.Session->GetEvents({});
        auto control = std::get<NFq::TMessageStreamPartitionExhaustedEvent>(events.at(0)).PartitionControl;
        UNIT_ASSERT_VALUES_EQUAL(native->Ends, 0);
        control->ConfirmExhausted();
        UNIT_ASSERT_VALUES_EQUAL(native->Ends, 1);
        UNIT_ASSERT_VALUES_EQUAL(native->Children.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(native->Children[0], 8);
        UNIT_ASSERT_VALUES_EQUAL(native->Children[1], 9);
        UNIT_ASSERT(control->AcknowledgeRange(0, 1));
        fixture.Session->Close().GetValueSync();
        control->ConfirmExhausted();
        UNIT_ASSERT_VALUES_EQUAL(native->Ends, 1);
        UNIT_ASSERT(!control->AcknowledgeRange(0, 1));
    }

    Y_UNIT_TEST(ExplicitStartBeyondObservedEndIsPreserved) {
        TStreamFixture fixture;
        auto native = MakeIntrusive<TObservedPartition>();
        fixture.Mock->AddEvent(NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent(native, 5, 10));
        auto events = fixture.Session->GetEvents({});
        auto control = std::get<NFq::TMessageStreamPartitionStartRequestedEvent>(events.at(0)).PartitionControl;
        control->ConfirmStart(11, {});
        UNIT_ASSERT_VALUES_EQUAL(native->Starts, 1);
        UNIT_ASSERT(native->StartOffset);
        UNIT_ASSERT_VALUES_EQUAL(*native->StartOffset, 11);
    }

    Y_UNIT_TEST(UnknownMetadataAndNullPayload) {
        NFq::TMessageStreamRecord record;
        UNIT_ASSERT(!record.Data);
        UNIT_ASSERT(!record.CreateTime);
        UNIT_ASSERT(!record.WriteTime);
        UNIT_ASSERT(!record.MessageGroupId);
        UNIT_ASSERT(!record.SeqNo);
        record.Data = TString();
        UNIT_ASSERT(record.Data);
        UNIT_ASSERT(record.Data->empty());

        NFq::TMessageStreamDescription topic;
        UNIT_ASSERT(!topic.Consumers);
        topic.Consumers.emplace();
        UNIT_ASSERT(topic.Consumers->empty());
        topic.Partitions = {{.PartitionId = {7}}, {.PartitionId = {42}, .Active = false}};
        UNIT_ASSERT_VALUES_EQUAL(topic.Partitions[1].PartitionId.Value, 42);

        NFq::TMessageStreamPartitionStartRequestedEvent started;
        NFq::TMessageStreamPartitionStatusEvent status;
        UNIT_ASSERT(!started.CommittedOffset);
        UNIT_ASSERT(!started.EndOffset);
        UNIT_ASSERT(!status.CommittedOffset);
        UNIT_ASSERT(!status.ReadOffset);
        UNIT_ASSERT(!status.EndOffset);
    }

    Y_UNIT_TEST(ZeroLimitsDoNotConsumeEvents) {
        TStreamFixture fixture;
        fixture.Mock->AddDataReceivedEvent(0, "payload");
        UNIT_ASSERT(fixture.Session->GetEvents({.MaxEventsCount = 0}).empty());
        UNIT_ASSERT(fixture.Session->GetEvents({.MaxByteSize = 0}).empty());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Mock->GetInflightEventsCount(), 1);
        const auto events = fixture.Session->GetEvents({.MaxEventsCount = 1, .MaxByteSize = 1});
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        const auto& data = std::get<NFq::TMessageStreamDataEvent>(events.front());
        UNIT_ASSERT_VALUES_EQUAL(data.Records.front().Data.value(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(data.Records.front().Id.PartitionId.Value, 7);
    }

    Y_UNIT_TEST(CloseWakesWaitersAndRevokesControl) {
        TStreamFixture fixture;
        fixture.Mock->AddDataReceivedEvent(0, "payload");
        const auto events = fixture.Session->GetEvents({});
        auto control = std::get<NFq::TMessageStreamDataEvent>(events.front()).PartitionControl;
        UNIT_ASSERT_EXCEPTION(control->AcknowledgeRange(2, 1), NFq::TMessageStreamException);
        auto ready = fixture.Session->WaitEvent();
        UNIT_ASSERT(!ready.IsReady());
        fixture.Session->Close().GetValueSync();
        UNIT_ASSERT(ready.IsReady());
        UNIT_ASSERT(!control->AcknowledgeRange(0, 1));
        control->ConfirmStart({}, {}); // Late confirmations are harmless.
        control->ConfirmStop();
        fixture.Session->Close().GetValueSync();
        UNIT_ASSERT(fixture.Session->GetEvents({}).empty());
    }

    Y_UNIT_TEST(TerminalEventIsDeliveredOnce) {
        TStreamFixture fixture;
        fixture.Mock->SetEventProvider([]() -> NYdb::NTopic::TReadSessionEvent::TEvent {
            return NYdb::NTopic::TSessionClosedEvent(NYdb::EStatus::CANCELLED, {});
        });
        const auto events = fixture.Session->GetEvents({});
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        UNIT_ASSERT(std::holds_alternative<NFq::TMessageStreamSessionClosedEvent>(events.front()));
        UNIT_ASSERT(fixture.Session->GetEvents({}).empty());
        UNIT_ASSERT(fixture.Session->WaitEvent().IsReady());
    }
}

} // namespace NYql
