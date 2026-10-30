#pragma once

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NKikimr::NPQ::NTest {

    struct TRawReadSessionSettings {
        TVector<TString> Topics;
        bool AutoPartitioningSupport = true;
        TMaybe<TInstant> ReadFrom;
        // Explicit partitions of every topic.
        TVector<i64> PartitionIds;
        // StartPartitionSession of these partitions waits for ConfirmStart.
        TVector<std::pair<TString, ui64>> DeferredStarts;
    };

    // Topic API read stream without the SDK: it confirms partition sessions as the SDK does, but commits only on request.
    // Every wait reads server messages until the condition holds; the stream deadline bounds it.
    class TRawReadSession {
        using TFromClient = Ydb::Topic::StreamReadMessage::FromClient;
        using TFromServer = Ydb::Topic::StreamReadMessage::FromServer;

    public:
        struct TMessage {
            TString Topic;
            ui64 PartitionId;
            ui64 Offset;
            TString Data;
        };

        TRawReadSession(NYdb::NTopic::NTests::TTopicSdkTestSetup& setup, const TRawReadSessionSettings& settings);
        ~TRawReadSession();

        const TVector<TMessage>& GetMessages() const;
        // Partition sessions in the order of their EndPartitionSession.
        const TVector<i64>& GetEndedPartitionSessions() const;
        TMaybe<i64> FindPartitionSession(TStringBuf topic, ui64 partitionId) const;

        void WaitFor(const std::function<bool()>& condition, TStringBuf what);
        i64 WaitPartitionSession(TStringBuf topic, ui64 partitionId);
        void ConfirmStart(TStringBuf topic, ui64 partitionId);
        TMessage ReadMessage(TStringBuf topic, ui64 partitionId);
        void Commit(const TMessage& message);
        void WaitCommitted(TStringBuf topic, ui64 partitionId, ui64 offset);

    private:
        struct TPartitionSession {
            TString Topic;
            ui64 PartitionId;
            ui64 CommittedOffset;
        };

        void Send(const TFromClient& message);
        void Process(const TFromServer& message);
        void SendStartResponse(i64 partitionSessionId);
        bool IsDeferred(const TPartitionSession& session) const;

        TRawReadSessionSettings Settings;
        std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> Stub;
        grpc::ClientContext Context;
        std::unique_ptr<grpc::ClientReaderWriter<TFromClient, TFromServer>> Stream;
        THashMap<i64, TPartitionSession> PartitionSessions;
        THashMap<i64, ui64> Committed;
        TVector<TMessage> Messages;
        TVector<i64> EndedPartitionSessions;
    };

} // namespace NKikimr::NPQ::NTest
