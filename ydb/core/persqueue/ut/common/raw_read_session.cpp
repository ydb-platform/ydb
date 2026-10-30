#include "raw_read_session.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/resources/ydb_resources.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/mapfindptr.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>

#include <google/protobuf/util/time_util.h>

#include <grpcpp/create_channel.h>

#include <chrono>

namespace NKikimr::NPQ::NTest {

    namespace {

        // The server reports the full path of the topic.
        bool SameTopic(TStringBuf path, TStringBuf topic) {
            return path == topic || (path.EndsWith(topic) && path[path.size() - topic.size() - 1] == '/');
        }

    } // namespace

    TRawReadSession::TRawReadSession(NYdb::NTopic::NTests::TTopicSdkTestSetup& setup, const TRawReadSessionSettings& settings)
        : Settings(settings)
        , Stub(Ydb::Topic::V1::TopicService::NewStub(grpc::CreateChannel(TStringBuilder() << "localhost:" << setup.GetServer().GrpcPort, grpc::InsecureChannelCredentials())))
    {
        Context.AddMetadata(NYdb::YDB_DATABASE_HEADER, TString{setup.GetDatabase()});
        Context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(60));
        Stream = Stub->StreamRead(&Context);

        TFromClient init;
        init.mutable_init_request()->set_consumer(NYdb::NTopic::NTests::TEST_CONSUMER);
        init.mutable_init_request()->set_auto_partitioning_support(Settings.AutoPartitioningSupport);
        for (const TString& path : Settings.Topics) {
            Ydb::Topic::StreamReadMessage::InitRequest::TopicReadSettings* topic = init.mutable_init_request()->add_topics_read_settings();
            topic->set_path(path);
            if (Settings.ReadFrom) {
                *topic->mutable_read_from() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(Settings.ReadFrom->MilliSeconds());
            }
        }
        Send(init);
        TFromServer response;
        UNIT_ASSERT_C(Stream->Read(&response), "read stream closed before the init response");
        UNIT_ASSERT_C(response.has_init_response(), response);

        TFromClient read;
        read.mutable_read_request()->set_bytes_size(1_MB);
        Send(read);
    }

    TRawReadSession::~TRawReadSession() {
        Context.TryCancel();
    }

    const TVector<TRawReadSession::TMessage>& TRawReadSession::GetMessages() const {
        return Messages;
    }

    const TVector<i64>& TRawReadSession::GetEndedPartitionSessions() const {
        return EndedPartitionSessions;
    }

    TMaybe<i64> TRawReadSession::FindPartitionSession(TStringBuf topic, ui64 partitionId) const {
        TMaybe<i64> result;
        for (const auto& [id, session] : PartitionSessions) {
            if (session.PartitionId == partitionId && SameTopic(session.Topic, topic) && (!result || *result < id)) {
                result = id;
            }
        }
        return result;
    }

    void TRawReadSession::WaitFor(const std::function<bool()>& condition, TStringBuf what) {
        while (!condition()) {
            TFromServer message;
            UNIT_ASSERT_C(Stream->Read(&message), "read stream closed while waiting for " << what);
            UNIT_ASSERT_VALUES_EQUAL_C(message.status(), Ydb::StatusIds::SUCCESS, message);
            Process(message);
        }
    }

    i64 TRawReadSession::WaitPartitionSession(TStringBuf topic, ui64 partitionId) {
        WaitFor([&]() { return FindPartitionSession(topic, partitionId).Defined(); }, TStringBuilder() << "StartPartitionSession of " << topic << "/" << partitionId);
        return *FindPartitionSession(topic, partitionId);
    }

    void TRawReadSession::ConfirmStart(TStringBuf topic, ui64 partitionId) {
        SendStartResponse(WaitPartitionSession(topic, partitionId));
    }

    TRawReadSession::TMessage TRawReadSession::ReadMessage(TStringBuf topic, ui64 partitionId) {
        const auto find = [&]() { return FindIfPtr(Messages, [&](const TMessage& message) { return message.PartitionId == partitionId && SameTopic(message.Topic, topic); }); };
        WaitFor([&]() { return find() != nullptr; }, TStringBuilder() << "a message from " << topic << "/" << partitionId);
        return *find();
    }

    void TRawReadSession::Commit(const TMessage& message) {
        const TMaybe<i64> id = FindPartitionSession(message.Topic, message.PartitionId);
        UNIT_ASSERT_C(id, "no partition session for " << message.Topic << "/" << message.PartitionId);
        const TPartitionSession* session = MapFindPtr(PartitionSessions, *id);
        TFromClient commit;
        Ydb::Topic::StreamReadMessage::CommitOffsetRequest::PartitionCommitOffset* offsets = commit.mutable_commit_offset_request()->add_commit_offsets();
        offsets->set_partition_session_id(*id);
        Ydb::Topic::OffsetsRange* range = offsets->add_offsets();
        range->set_start(session->CommittedOffset);
        range->set_end(message.Offset + 1);
        Send(commit);
    }

    void TRawReadSession::WaitCommitted(TStringBuf topic, ui64 partitionId, ui64 offset) {
        const i64 id = WaitPartitionSession(topic, partitionId);
        WaitFor([&]() { return Committed.Value(id, 0) >= offset; }, TStringBuilder() << "CommitOffsetResponse for " << topic << "/" << partitionId << " up to " << offset);
    }

    void TRawReadSession::Send(const TFromClient& message) {
        UNIT_ASSERT_C(Stream->Write(message), "write to the read stream failed: " << message);
    }

    void TRawReadSession::SendStartResponse(i64 partitionSessionId) {
        TFromClient response;
        response.mutable_start_partition_session_response()->set_partition_session_id(partitionSessionId);
        Send(response);
    }

    bool TRawReadSession::IsDeferred(const TPartitionSession& session) const {
        return AnyOf(Settings.DeferredStarts, [&](const auto& deferred) { return deferred.second == session.PartitionId && SameTopic(session.Topic, deferred.first); });
    }

    void TRawReadSession::Process(const TFromServer& message) {
        switch (message.server_message_case()) {
            case TFromServer::kStartPartitionSessionRequest: {
                const Ydb::Topic::StreamReadMessage::StartPartitionSessionRequest& request = message.start_partition_session_request();
                const i64 id = request.partition_session().partition_session_id();
                const TPartitionSession& session = PartitionSessions[id] = TPartitionSession{
                    .Topic = request.partition_session().path(),
                    .PartitionId = static_cast<ui64>(request.partition_session().partition_id()),
                    .CommittedOffset = static_cast<ui64>(request.committed_offset()),
                };
                if (!IsDeferred(session)) {
                    SendStartResponse(id);
                }
                break;
            }
            case TFromServer::kStopPartitionSessionRequest: {
                const i64 id = message.stop_partition_session_request().partition_session_id();
                if (message.stop_partition_session_request().graceful()) {
                    TFromClient response;
                    response.mutable_stop_partition_session_response()->set_partition_session_id(id);
                    Send(response);
                } else {
                    PartitionSessions.erase(id);
                }
                break;
            }
            case TFromServer::kEndPartitionSession:
                EndedPartitionSessions.push_back(message.end_partition_session().partition_session_id());
                break;
            case TFromServer::kReadResponse: {
                for (const Ydb::Topic::StreamReadMessage::ReadResponse::PartitionData& data : message.read_response().partition_data()) {
                    const TPartitionSession* session = MapFindPtr(PartitionSessions, data.partition_session_id());
                    UNIT_ASSERT_C(session, "data for unknown partition session " << data.partition_session_id());
                    for (const Ydb::Topic::StreamReadMessage::ReadResponse::Batch& batch : data.batches()) {
                        for (const Ydb::Topic::StreamReadMessage::ReadResponse::MessageData& messageData : batch.message_data()) {
                            Messages.push_back(TMessage{
                                .Topic = session->Topic,
                                .PartitionId = session->PartitionId,
                                .Offset = static_cast<ui64>(messageData.offset()),
                                .Data = messageData.data(),
                            });
                        }
                    }
                }
                TFromClient read;
                read.mutable_read_request()->set_bytes_size(message.read_response().bytes_size());
                Send(read);
                break;
            }
            case TFromServer::kCommitOffsetResponse:
                for (const Ydb::Topic::StreamReadMessage::CommitOffsetResponse::PartitionCommittedOffset& committed : message.commit_offset_response().partitions_committed_offsets()) {
                    Committed[committed.partition_session_id()] = committed.committed_offset();
                }
                break;
            default:
                break;
        }
    }

} // namespace NKikimr::NPQ::NTest
