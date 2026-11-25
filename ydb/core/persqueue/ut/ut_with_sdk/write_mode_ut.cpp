#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/library/grpc/server/grpc_server.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/services/deprecated/persqueue_v0/api/grpc/persqueue.grpc.pb.h>
#include <ydb/services/deprecated/persqueue_v0/persqueue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <chrono>
#include <map>

namespace NKikimr::NPersQueueTests {
namespace {

using namespace NYdb::NTopic;

const TString TOPIC = "/Root/write-mode";
const TString TOPIC_NAME = "write-mode";
const TString CONSUMER = "reader";
// NGRpcProxy::MAGIC_COOKIE_VALUE, skips ChooseProxy for pqv0.
constexpr ui64 PQV0_PROXY_COOKIE = 123456789;
const TString WRITE_MODE = "_write_mode";
const TString AVAILABLE = "available";
const TString UNAVAILABLE = "unavailable";
const TString PROHIBITED = "prohibited";
using TAttributes = std::map<TString, TString>;

class TWriteModeEnv {
public:
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    TTopicClient Client;
    NYdb::NPersQueue::TPersQueueClient PqClient;
    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> Stub;
    TPortManager PortManager;
    const ui16 PqV0Port;
    // The test server does not host the deprecated pqv0 service, so it gets its own gRPC server.
    NYdbGrpc::TGRpcServer PqV0Server;
    std::unique_ptr<NPersQueue::PersQueueService::Stub> PqV0Stub;

    TWriteModeEnv()
        : Setup("WriteMode", NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings(), false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
        , PqClient(Driver)
        , Stub(Ydb::PersQueue::V1::PersQueueService::NewStub(grpc::CreateChannel(
            Setup.GetEndpoint(), grpc::InsecureChannelCredentials())))
        , PqV0Port(PortManager.GetPort())
        , PqV0Server(NYdbGrpc::TServerOptions().SetHost("localhost").SetPort(PqV0Port))
        , PqV0Stub(NPersQueue::PersQueueService::NewStub(grpc::CreateChannel(
            "localhost:" + ToString(PqV0Port), grpc::InsecureChannelCredentials())))
    {
        PqV0Server.AddService(new NGRpcService::TGRpcPersQueueService(
            Setup.GetRuntime().GetActorSystem(0),
            MakeIntrusive<::NMonitoring::TDynamicCounters>(),
            NMsgBusProxy::CreatePersQueueMetaCacheV2Id()));
        PqV0Server.Start();
    }

    ~TWriteModeEnv() {
        PqV0Server.Stop();
    }

    void InitContext(grpc::ClientContext& context) {
        context.AddMetadata("x-ydb-database", Setup.GetDatabase());
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    }

    static void AssertStatus(const Ydb::Operations::Operation& operation, Ydb::StatusIds::StatusCode expected) {
        UNIT_ASSERT_C(operation.ready(), operation.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), expected, operation.ShortDebugString());
    }

    void Create(bool pqv1, const TAttributes& attributes = {},
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        if (pqv1) {
            Ydb::PersQueue::V1::CreateTopicRequest request;
            request.set_path(TOPIC);
            auto* settings = request.mutable_settings();
            settings->set_partitions_count(1);
            settings->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            settings->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            settings->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
            auto* rule = settings->add_read_rules();
            rule->set_consumer_name(CONSUMER);
            rule->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            rule->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
            for (const auto& [name, value] : attributes) {
                (*settings->mutable_attributes())[name] = value;
            }
            grpc::ClientContext context;
            InitContext(context);
            Ydb::PersQueue::V1::CreateTopicResponse response;
            UNIT_ASSERT(Stub->CreateTopic(&context, request, &response).ok());
            AssertStatus(response.operation(), expected);
        } else {
            auto settings = TCreateTopicSettings().PartitioningSettings(1, 1)
                .BeginAddConsumer(CONSUMER).EndAddConsumer();
            for (const auto& [name, value] : attributes) {
                settings.AddAttribute(name, value);
            }
            const auto result = Client.CreateTopic(TOPIC, settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), static_cast<NYdb::EStatus>(expected), result.GetIssues().ToString());
        }
    }

    Ydb::PersQueue::V1::TopicSettings DescribePq() {
        Ydb::PersQueue::V1::DescribeTopicRequest request;
        request.set_path(TOPIC);
        grpc::ClientContext context;
        InitContext(context);
        Ydb::PersQueue::V1::DescribeTopicResponse response;
        UNIT_ASSERT(Stub->DescribeTopic(&context, request, &response).ok());
        AssertStatus(response.operation(), Ydb::StatusIds::SUCCESS);
        Ydb::PersQueue::V1::DescribeTopicResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        return result.settings();
    }

    void Alter(bool pqv1, const TAttributes& attributes,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        if (pqv1) {
            Ydb::PersQueue::V1::AlterTopicRequest request;
            request.set_path(TOPIC);
            *request.mutable_settings() = DescribePq();
            auto& attrs = *request.mutable_settings()->mutable_attributes();
            attrs.erase(WRITE_MODE);
            for (const auto& [name, value] : attributes) {
                attrs[name] = value;
            }
            grpc::ClientContext context;
            InitContext(context);
            Ydb::PersQueue::V1::AlterTopicResponse response;
            UNIT_ASSERT(Stub->AlterTopic(&context, request, &response).ok());
            AssertStatus(response.operation(), expected);
        } else {
            auto settings = TAlterTopicSettings();
            for (const auto& [name, value] : attributes) {
                settings.BeginAlterAttributes().Alter(name, value).EndAlterAttributes();
            }
            const auto result = Client.AlterTopic(TOPIC, settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), static_cast<NYdb::EStatus>(expected), result.GetIssues().ToString());
        }
    }

    void AssertAttribute(const TString& mode = {}) {
        const auto pqSettings = DescribePq();
        const auto describe = Client.DescribeTopic(TOPIC).GetValueSync();
        AssertSuccess(describe);
        const auto& attributes = describe.GetTopicDescription().GetAttributes();
        const auto pqIt = pqSettings.attributes().find(WRITE_MODE);
        const auto it = attributes.find(WRITE_MODE);
        if (mode.empty()) {
            UNIT_ASSERT(pqIt == pqSettings.attributes().end());
            UNIT_ASSERT(it == attributes.end());
        } else {
            UNIT_ASSERT(pqIt != pqSettings.attributes().end());
            UNIT_ASSERT_VALUES_EQUAL(pqIt->second, mode);
            UNIT_ASSERT(it != attributes.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, mode);
        }
    }

    TWriteSessionSettings WriteSettings() const {
        return TWriteSessionSettings().Path(TOPIC).PartitionId(0)
            .ProducerId("producer").MessageGroupId("producer").Codec(ECodec::RAW)
            .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy());
    }

    void Write(ui64 seqNo) {
        auto writer = Client.CreateSimpleBlockingWriteSession(WriteSettings());
        UNIT_ASSERT(writer->Write("message", seqNo, std::nullopt, TDuration::Seconds(10)));
        UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
        AssertPqV0(true);
    }

    template <class TEvents, class TWriter>
    void AssertRejected(const std::shared_ptr<TWriter>& writer, const TString& data, const TString& mode) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        bool sent = false;
        while (TInstant::Now() < deadline) {
            UNIT_ASSERT_C(writer->WaitEvent().Wait(deadline - TInstant::Now()), "Write session did not respond");
            auto event = writer->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* ready = std::get_if<typename TEvents::TReadyToAcceptEvent>(&*event)) {
                if (!sent) {
                    writer->Write(std::move(ready->ContinuationToken), data, 100);
                    sent = true;
                }
            } else if (auto* acks = std::get_if<typename TEvents::TAcksEvent>(&*event)) {
                for (const auto& ack : acks->Acks) {
                    UNIT_ASSERT_VALUES_EQUAL(ack.State, TEvents::TWriteAck::EES_DISCARDED);
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                const auto expected = mode == UNAVAILABLE ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::BAD_REQUEST;
                const auto reason = mode == UNAVAILABLE ? "Write to topic is temporarily disabled" : "Write to topic is prohibited";
                UNIT_ASSERT_VALUES_EQUAL_C(closed->GetStatus(), expected, closed->DebugString());
                UNIT_ASSERT_STRING_CONTAINS(closed->GetIssues().ToString(), reason);
                return;
            }
        }
        UNIT_FAIL("Write session did not close after a rejected write");
    }

    // First pqv0 response is the error, so init never reaches the partition chooser.
    void AssertPqV0(bool expectOk, const TString& mode = {}) {
        grpc::ClientContext context;
        InitContext(context);
        auto stream = PqV0Stub->WriteSession(&context);
        NPersQueue::WriteRequest request;
        auto* init = request.mutable_init();
        init->set_topic(TOPIC_NAME);
        init->set_source_id("pqv0-producer");
        init->set_proxy_cookie(PQV0_PROXY_COOKIE);
        UNIT_ASSERT(stream->Write(request));
        NPersQueue::WriteResponse response;
        UNIT_ASSERT_C(stream->Read(&response), "pqv0 write session returned no message");
        if (expectOk) {
            UNIT_ASSERT_C(response.has_init(), response.ShortDebugString());
            return;
        }
        UNIT_ASSERT_C(response.has_error(), response.ShortDebugString());
        const auto expectedCode = mode == UNAVAILABLE
            ? NPersQueue::NErrorCode::ERROR
            : NPersQueue::NErrorCode::BAD_REQUEST;
        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(response.error().code()), static_cast<int>(expectedCode));
        const auto reason = mode == UNAVAILABLE
            ? "Write to topic is temporarily disabled"
            : "Write to topic is prohibited";
        UNIT_ASSERT_STRING_CONTAINS(response.error().description(), reason);
    }

    void RejectWrites(bool pqv1, const TString& mode) {
        AssertPqV0(false, mode);
        for (const auto size : {16u, 600u * 1024}) {
            const TString data(size, 'x');
            if (pqv1) {
                auto writer = PqClient.CreateWriteSession(NYdb::NPersQueue::TWriteSessionSettings()
                    .Path(TOPIC).MessageGroupId("producer").Codec(ECodec::RAW)
                    .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off)
                    .RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
                AssertRejected<NYdb::NPersQueue::TWriteSessionEvent>(writer, data, mode);
            } else {
                auto writer = Client.CreateWriteSession(WriteSettings());
                AssertRejected<TWriteSessionEvent>(writer, data, mode);
            }
        }
    }

    // The session passed its init check before the alter, so only the tablet can reject the write.
    void RejectWriteInOpenSession(bool pqv1, const TString& mode, ui64 seqNo) {
        auto writer = Client.CreateWriteSession(WriteSettings());
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        auto nextEvent = [&] {
            UNIT_ASSERT_C(writer->WaitEvent().Wait(deadline - TInstant::Now()), "Write session did not respond");
            auto event = writer->GetEvent(false);
            UNIT_ASSERT(event);
            return std::move(*event);
        };

        std::optional<TContinuationToken> token;
        bool written = false;
        bool sent = false;
        while (!written || !token) {
            auto event = nextEvent();
            if (auto* ready = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                if (sent) {
                    token = std::move(ready->ContinuationToken);
                } else {
                    writer->Write(std::move(ready->ContinuationToken), "before", seqNo);
                    sent = true;
                }
            } else if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                for (const auto& ack : acks->Acks) {
                    UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_WRITTEN);
                }
                written = true;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }

        Alter(pqv1, {{WRITE_MODE, mode}});
        writer->Write(std::move(*token), "after", seqNo + 1);
        while (true) {
            auto event = nextEvent();
            if (auto* acks = std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                for (const auto& ack : acks->Acks) {
                    UNIT_ASSERT_VALUES_EQUAL(ack.State, TWriteSessionEvent::TWriteAck::EES_DISCARDED);
                }
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&event)) {
                const auto expected = mode == UNAVAILABLE ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::BAD_REQUEST;
                const auto reason = mode == UNAVAILABLE ? "Write to topic is temporarily disabled" : "Write to topic is prohibited";
                UNIT_ASSERT_VALUES_EQUAL_C(closed->GetStatus(), expected, closed->DebugString());
                UNIT_ASSERT_STRING_CONTAINS(closed->GetIssues().ToString(), reason);
                return;
            }
        }
    }

    void AssertOffsets(ui64 committed, ui64 end) {
        auto reader = Client.CreateReadSession(TReadSessionSettings().ConsumerName(CONSUMER)
            .AppendTopics(TTopicReadSettings(TOPIC)).RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
        const auto deadline = TInstant::Now() + TDuration::Seconds(30);
        while (TInstant::Now() < deadline) {
            UNIT_ASSERT(reader->WaitEvent().Wait(deadline - TInstant::Now()));
            auto event = reader->GetEvent(false);
            UNIT_ASSERT(event);
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                UNIT_ASSERT_VALUES_EQUAL(start->GetCommittedOffset(), committed);
                UNIT_ASSERT_VALUES_EQUAL(start->GetEndOffset(), end);
                start->Confirm();
                UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
                return;
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
        }
        UNIT_FAIL("Read session did not start");
    }

    static void AssertSuccess(const NYdb::TStatus& status) {
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTopicWriteMode) {
    Y_UNIT_TEST(ReadRestrictionsCloseSession) {
        for (const auto size : {16u, 600u * 1024}) {
            TWriteModeEnv env;
            env.Create(false);
            const TString data(size, 'x');
            auto writer = env.Client.CreateSimpleBlockingWriteSession(env.WriteSettings());
            UNIT_ASSERT(writer->Write(data, 1, std::nullopt, TDuration::Seconds(10)));
            UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));

            auto checkRead = [&](const TString& mode) {
                auto reader = env.Client.CreateReadSession(TReadSessionSettings().ConsumerName(CONSUMER)
                    .AppendTopics(TTopicReadSettings(TOPIC)).RetryPolicy(NYdb::NTopic::IRetryPolicy::GetNoRetryPolicy()));
                const auto deadline = TInstant::Now() + TDuration::Seconds(30);
                while (TInstant::Now() < deadline) {
                    UNIT_ASSERT_C(reader->WaitEvent().Wait(deadline - TInstant::Now()), "Read session did not respond");
                    auto event = reader->GetEvent(false);
                    UNIT_ASSERT(event);
                    if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                        start->Confirm();
                    } else if (auto* stop = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                        stop->Confirm();
                    } else if (auto* received = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                        UNIT_ASSERT_VALUES_EQUAL(mode, AVAILABLE);
                        UNIT_ASSERT_VALUES_EQUAL(received->GetMessages().size(), 1);
                        UNIT_ASSERT_VALUES_EQUAL(received->GetMessages().front().GetOffset(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(received->GetMessages().front().GetData(), data);
                        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
                        return;
                    } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                        UNIT_ASSERT_C(mode != AVAILABLE, closed->DebugString());
                        const auto expected = mode == UNAVAILABLE ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::BAD_REQUEST;
                        const auto reason = mode == UNAVAILABLE
                            ? "Write to topic is temporarily disabled" : "Write to topic is prohibited";
                        UNIT_ASSERT_VALUES_EQUAL_C(closed->GetStatus(), expected, closed->DebugString());
                        UNIT_ASSERT_STRING_CONTAINS(closed->GetIssues().ToString(), reason);
                        return;
                    }
                }
                UNIT_FAIL("Read session neither returned data nor closed");
            };

            checkRead(AVAILABLE);
            for (const auto& mode : {UNAVAILABLE, PROHIBITED}) {
                env.Alter(false, {{WRITE_MODE, mode}});
                checkRead(mode);
                env.Alter(false, {{WRITE_MODE, AVAILABLE}});
                checkRead(AVAILABLE);
            }
        }
    }

    Y_UNIT_TEST(AbortTransactionAfterChangingWriteMode) {
        for (const auto size : {16u, 600u * 1024}) {
            TWriteModeEnv env;
            env.Create(false);
            NYdb::NQuery::TQueryClient queryClient(env.Driver);
            const auto sessionResult = queryClient.GetSession().GetValueSync();
            env.AssertSuccess(sessionResult);
            auto session = sessionResult.GetSession();
            const TString data(size, 'x');
            ui64 seqNo = 0;
            ui64 endOffset = 0;
            auto writeInTx = [&] {
                const auto result = session.BeginTransaction(NYdb::NQuery::TTxSettings()).GetValueSync();
                env.AssertSuccess(result);
                auto tx = result.GetTransaction();
                auto writer = env.Client.CreateSimpleBlockingWriteSession(env.WriteSettings());
                TWriteMessage message(data);
                message.SeqNo(++seqNo);
                UNIT_ASSERT(writer->Write(std::move(message), &tx, TDuration::Seconds(10)));
                UNIT_ASSERT(writer->Close(TDuration::Seconds(30)));
                return tx;
            };

            auto tx = writeInTx();
            env.AssertSuccess(tx.Commit().GetValueSync());
            env.AssertOffsets(0, ++endOffset);

            for (const auto& mode : {UNAVAILABLE, PROHIBITED}) {
                auto abortedTx = writeInTx();
                env.Alter(false, {{WRITE_MODE, mode}});
                const auto aborted = abortedTx.Commit().GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(aborted.GetStatus(), NYdb::EStatus::ABORTED, aborted.GetIssues().ToString());
                const auto reason = mode == UNAVAILABLE
                    ? "Write to topic is temporarily disabled" : "Write to topic is prohibited";
                UNIT_ASSERT_STRING_CONTAINS(aborted.GetIssues().ToString(), reason);
                env.AssertOffsets(0, endOffset);

                env.Alter(false, {{WRITE_MODE, AVAILABLE}});
                auto committedTx = writeInTx();
                env.AssertSuccess(committedTx.Commit().GetValueSync());
                env.AssertOffsets(0, ++endOffset);
            }
        }
    }

    Y_UNIT_TEST_TWIN(CreateWithWriteMode, pqv1) {
        for (const auto& mode : {AVAILABLE, UNAVAILABLE, PROHIBITED}) {
            TWriteModeEnv env;
            env.Create(pqv1, {{WRITE_MODE, mode}});
            env.AssertAttribute(mode);
            if (mode == AVAILABLE) {
                env.Write(1);
                env.AssertOffsets(0, 1);
            } else {
                env.RejectWrites(pqv1, mode);
                env.AssertOffsets(0, 0);
            }
        }
    }

    Y_UNIT_TEST_TWIN(ChangeWriteMode, pqv1) {
        TWriteModeEnv env;
        env.Create(pqv1);
        env.AssertAttribute();
        ui64 seqNo = 1;
        env.Write(seqNo);
        for (const auto& mode : {UNAVAILABLE, PROHIBITED}) {
            env.Alter(pqv1, {{WRITE_MODE, mode}});
            env.AssertAttribute(mode);

            // SDK settings do not expose the mode; ordinary alters must preserve it.
            const auto describe = env.PqClient.DescribeTopic(TOPIC).GetValueSync();
            env.AssertSuccess(describe);
            env.AssertSuccess(env.PqClient.AlterTopic(TOPIC,
                NYdb::NPersQueue::TAlterTopicSettings().SetSettings(describe.TopicSettings())).GetValueSync());
            env.AssertSuccess(env.Client.AlterTopic(TOPIC,
                TAlterTopicSettings().SetRetentionPeriod(TDuration::Hours(12))).GetValueSync());
            env.AssertAttribute(mode);
            env.RejectWrites(pqv1, mode);
            const auto commit = env.Client.CommitOffset(TOPIC, 0, CONSUMER, seqNo).GetValueSync();
            const auto expected = mode == UNAVAILABLE ? NYdb::EStatus::UNAVAILABLE : NYdb::EStatus::BAD_REQUEST;
            UNIT_ASSERT_VALUES_EQUAL_C(commit.GetStatus(), expected, commit.GetIssues().ToString());
            env.AssertOffsets(seqNo - 1, seqNo);

            env.Alter(pqv1, {{WRITE_MODE, AVAILABLE}});
            env.AssertAttribute(AVAILABLE);
            env.AssertSuccess(env.Client.CommitOffset(TOPIC, 0, CONSUMER, seqNo).GetValueSync());
            env.Write(++seqNo);
        }
        env.AssertOffsets(seqNo - 1, seqNo);
    }

    Y_UNIT_TEST_TWIN(SwitchRestrictedWriteMode, pqv1) {
        TWriteModeEnv env;
        env.Create(pqv1, {{WRITE_MODE, PROHIBITED}});
        for (const auto& mode : {UNAVAILABLE, PROHIBITED}) {
            env.Alter(pqv1, {{WRITE_MODE, mode}});
            env.AssertAttribute(mode);
            env.RejectWrites(pqv1, mode);
        }
        env.AssertOffsets(0, 0);
        env.Alter(pqv1, {{WRITE_MODE, AVAILABLE}});
        env.AssertAttribute(AVAILABLE);
        env.Write(1);
        env.AssertOffsets(0, 1);
    }

    Y_UNIT_TEST_TWIN(RejectWriteInOpenSession, pqv1) {
        for (const auto& mode : {UNAVAILABLE, PROHIBITED}) {
            TWriteModeEnv env;
            env.Create(pqv1);
            env.RejectWriteInOpenSession(pqv1, mode, 1);
            env.AssertOffsets(0, 1);
        }
    }

    Y_UNIT_TEST_TWIN(RejectInvalidWriteModeAttributes, pqv1) {
        TWriteModeEnv env;
        const TVector<TAttributes> invalid = {
            {{WRITE_MODE, ""}},
            {{WRITE_MODE, "true"}},
            {{WRITE_MODE, "false"}},
            {{WRITE_MODE, "unknown"}},
            {{WRITE_MODE, "AVAILABLE"}},
        };
        for (const auto& attrs : invalid) {
            env.Create(pqv1, attrs, Ydb::StatusIds::BAD_REQUEST);
        }
        env.Create(pqv1, {{WRITE_MODE, PROHIBITED}});
        for (const auto& attrs : invalid) {
            env.Alter(pqv1, attrs, Ydb::StatusIds::BAD_REQUEST);
            env.AssertAttribute(PROHIBITED);
        }
        env.RejectWrites(pqv1, PROHIBITED);
        env.AssertOffsets(0, 0);
    }
}

} // namespace NKikimr::NPersQueueTests
