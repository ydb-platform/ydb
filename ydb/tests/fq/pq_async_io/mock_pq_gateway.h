#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_rd_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_blocking_queue.h>

namespace NYql::NDq {

struct TEvMockPqEvents {
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvCreateSession = EvBegin,
        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");
    struct TEvCreateSession : public NActors::TEventLocal<TEvCreateSession, EvCreateSession> {};
};

class IMockPqGateway : public NYql::IPqGateway {
public:
    virtual void AddEvent(const TString& topic, NYdb::NTopic::TReadSessionEvent::TEvent&& e, size_t size) = 0;
};

NYdb::NTopic::TPartitionSession::TPtr CreatePartitionSession();

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway(
    NActors::TTestActorRuntime& runtime,
    NActors::TActorId notifier);

}
