#pragma once

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

#include <optional>

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

    virtual void SetEventProvider(TEvGen evGen) = 0;

    virtual void AddEvent(NYdb::NTopic::TReadSessionEvent::TEvent&& ev) = 0;

    virtual void AddStartSessionEvent() = 0;

    virtual void AddDataReceivedEvent(ui64 offset, const TString& data) = 0;

    virtual void AddDataReceivedEvent(ui64 offset, const TString& data, TInstant messageTime) = 0;

    virtual void AddDataReceivedEvent(const std::vector<TMessage>& messages) = 0;

    virtual void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues = {}) = 0;
};

class IMockPqWriteSession {
public:
    using TPtr = std::shared_ptr<IMockPqWriteSession>;

    virtual ~IMockPqWriteSession() = default;

    virtual std::vector<TString> ExtractData() = 0;

    virtual void ExpectMessage(const TString& message) = 0;

    virtual void ExpectMessages(std::vector<TString> messages, bool sort = false) = 0;

    virtual void Lock() = 0;

    virtual void Unlock() = 0;
};

// Limitations:
// - There should be at most one query in flight for each topic
class IMockPqGateway : public NYql::IPqGateway {
public:
    using TPtr = TIntrusivePtr<IMockPqGateway>;

    // Extract last created partition read session for the topic, returns nullptr if none
    virtual IMockPqReadSession::TPtr ExtractReadSession(const TString& topic) = 0;

    // Get read session for a specific partition (multi-partition topics). Returns nullptr if not created.
    virtual IMockPqReadSession::TPtr GetReadSession(const TString& topic, ui64 partitionId) = 0;

    // Wait for read session creation
    virtual IMockPqReadSession::TPtr WaitReadSession(const TString& topic) = 0;

    // Extract last created partition write session, returns nullptr if there is no existing session
    virtual IMockPqWriteSession::TPtr ExtractWriteSession(const TString& topic) = 0;

    // Wait for write session creation
    virtual IMockPqWriteSession::TPtr WaitWriteSession(const TString& topic) = 0;
};

struct TMockPqGatewaySettings {
    bool LockWritingByDefault = false;
    TDuration OperationTimeout = TDuration::Seconds(10);
    NActors::TTestActorRuntime* Runtime = nullptr;
    NActors::TActorId Notifier;
};

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings = {});

}  // namespace NTestUtils
