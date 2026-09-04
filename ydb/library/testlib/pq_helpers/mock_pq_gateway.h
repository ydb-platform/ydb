#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_session.h>

#include <util/system/types.h>

#include <functional>
#include <memory>
#include <optional>
#include <vector>

namespace NActors {

class TTestActorRuntimeBase;

} //namespace NActors

namespace NTestUtils {

struct TEvMockPqEvents {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSession = EvBegin,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvCreateSession : public NActors::TEventLocal<TEvCreateSession, EvCreateSession> {};
};

class IMockPqReadSession {
public:
    using TPtr = std::shared_ptr<IMockPqReadSession>;
    using TEvGen = std::function<NYdb::NTopic::TReadSessionEvent::TEvent()>;

    struct TMessage {
        ui64 Offset;
        TString Data;
        std::optional<TInstant> MessageTime;  // CreateTime/WriteTime in event; if unset, TInstant::Now() is used
    };

    virtual ~IMockPqReadSession() = default;

    virtual NYdb::NTopic::TPartitionSession::TPtr GetPartitionSession() const = 0;

    virtual ui64 GetInflightEventsCount() const = 0;

    virtual void SetEventProvider(TEvGen evGen) = 0;

    virtual void AddEvent(NYdb::NTopic::TReadSessionEvent::TEvent&& ev) = 0;

    virtual void AddStartSessionEvent(ui64 endOffset = 0) = 0;

    virtual void AddDataReceivedEvent(ui64 offset, TString data) = 0;

    virtual void AddDataReceivedEvent(ui64 offset, TString data, TInstant messageTime) = 0;

    virtual void AddDataReceivedEvent(const std::vector<TMessage>& messages) = 0;

    virtual void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues = {}) = 0;

    virtual void ExpectSessionClosed(std::optional<TDuration> timeout = std::nullopt) = 0;
};

class IMockPqWriteSession {
public:
    using TPtr = std::shared_ptr<IMockPqWriteSession>;

    virtual ~IMockPqWriteSession() = default;

    virtual void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues = {}) = 0;

    virtual std::vector<TString> ExtractData() = 0;

    virtual void ExpectMessage(const TString& message) = 0;

    virtual void ExpectMessages(std::vector<TString> messages, bool sort = false) = 0;

    virtual void ExpectSessionClosed() = 0;

    virtual void EnsureEmpty() = 0;

    virtual void Lock() = 0;

    virtual void LockAcks() = 0;

    virtual void Unlock() = 0;

    virtual void UnlockAcks(NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState status = NYdb::NTopic::TWriteSessionEvent::TWriteAck::EES_ALREADY_WRITTEN) = 0;

    // Acks management. Must be called after LockAcks()

    virtual void WaitAcks(ui64 count) = 0;
};

class IMockPqDeferredPublishClient {
public:
    virtual ~IMockPqDeferredPublishClient() = default;

    virtual void EnsureOpenedPublications(ui64 count, const TString& nameSubstring) = 0;

    virtual void LockCommits() = 0;

    virtual void UnlockCommits() = 0;

    // Pending commits management. Must be called after LockCommits()

    virtual void WaitCommits(ui64 count) = 0;

    virtual void ClearCommits() = 0;

    virtual void AcceptCommits(NYdb::EStatus status, NYdb::NIssue::TIssues issues = {}) = 0;
};

// Limitations:
// - There should be at most one query in flight for each topic
class IMockPqGateway : public NYql::IPqGateway {
public:
    using TPtr = TIntrusivePtr<IMockPqGateway>;

    // Extract last created partition read session for the topic, returns nullptr if none
    virtual IMockPqReadSession::TPtr ExtractReadSession(const TString& topic) = 0;

    // Get read session for a specific partition (multi-partition topics). Returns nullptr if not created.
    // Topics with multiple partitions must be registered in TMockPqGatewaySettings.
    virtual IMockPqReadSession::TPtr GetReadSession(const TString& topic, ui64 partitionId) = 0;

    // Wait for read session creation
    virtual IMockPqReadSession::TPtr WaitReadSession(const TString& topic) = 0;

    // Extract last created partition write session, returns nullptr if there is no existing session
    virtual IMockPqWriteSession::TPtr ExtractWriteSession(const TString& topic) = 0;

    // Wait for write session creation
    virtual IMockPqWriteSession::TPtr WaitWriteSession(const TString& topic) = 0;

    virtual IMockPqDeferredPublishClient& GetDeferredPublishClientController() = 0;
};

struct TMockPqGatewaySettings {
    struct TTopicInfo {
        ui32  PartitionCount = 1;
    };

    bool LockWritingByDefault = false;
    TDuration OperationTimeout = TDuration::Seconds(10);
    NActors::TTestActorRuntimeBase* Runtime = nullptr;
    NActors::TActorId Notifier;
    std::unordered_map<TString, TTopicInfo> Topics;
};

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings = {});

}  // namespace NTestUtils
