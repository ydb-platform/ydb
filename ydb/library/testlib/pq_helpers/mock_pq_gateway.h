#pragma once

#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

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
    };

    virtual ~IMockPqReadSession() = default;

    virtual NYdb::NTopic::TPartitionSession::TPtr GetPartitionSession() const = 0;

    virtual void SetEventProvider(TEvGen evGen) = 0;

    virtual void AddEvent(NYdb::NTopic::TReadSessionEvent::TEvent&& ev) = 0;

    virtual void AddStartSessionEvent() = 0;

    virtual void AddDataReceivedEvent(ui64 offset, const TString& data) = 0;

    virtual void AddDataReceivedEvent(const std::vector<TMessage>& messages) = 0;

    virtual void AddCloseSessionEvent(NYdb::EStatus status, NYdb::NIssue::TIssues issues = {}) = 0;
};

class IMockPqWriteSession {
public:
    using TPtr = std::shared_ptr<IMockPqWriteSession>;

    virtual ~IMockPqWriteSession() = default;

    virtual std::vector<TString> ExtractData() = 0;

    virtual void Lock() = 0;

    virtual void Unlock() = 0;
};

// Limitations:
// - There is should be at most one query in flight for each topic
// - Supported only single partition topics
class IMockPqGateway : public NYql::IPqGateway {
public:
    using TPtr = TIntrusivePtr<IMockPqGateway>;

    // Extract last created partition read session
    virtual IMockPqReadSession::TPtr ExtractReadSession(const TString& topic) = 0;

    // Extract last created partition write session
    virtual IMockPqWriteSession::TPtr ExtractWriteSession(const TString& topic) = 0;
};

struct TMockPqGatewaySettings {
    NActors::TTestActorRuntime* Runtime = nullptr;
    NActors::TActorId Notifier;
};

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings = {});

}  // namespace NTestUtils
