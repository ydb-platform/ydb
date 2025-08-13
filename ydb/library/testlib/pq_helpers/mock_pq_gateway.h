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

struct TMockPqSession {
    NYdb::NTopic::TPartitionSession::TPtr Session;
};

struct TMockPqGatewaySettings {
    NActors::TTestActorRuntime* Runtime = nullptr;
    NActors::TActorId Notifier;
};

class IMockPqGateway : public NYql::IPqGateway {
public:
    using TEvGen = std::function<NYdb::NTopic::TReadSessionEvent::TEvent(TMockPqSession)>;
    using TWriteResult = std::vector<TString>;

    virtual void AddEvent(const TString& topic, NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size = 0) = 0;

    virtual void AddEventProvider(const TString& topic, TEvGen evGen) = 0;

    virtual TWriteResult GetWriteSessionData(const TString& topic)  = 0;
};

NYdb::NTopic::TPartitionSession::TPtr CreatePartitionSession(const TString& path = "fake/path");

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(const TMockPqGatewaySettings& settings = {});

NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent MakePqMessage(ui64 offset, const TString& data, const TMockPqSession& meta);

}  // namespace NTestUtils
