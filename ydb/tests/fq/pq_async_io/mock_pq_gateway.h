#pragma once

#include <ydb/library/yql/providers/common/ut_helpers/dq_fake_ca.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_rd_read_actor.h>
#include <ydb/library/yql/providers/pq/async_io/dq_pq_write_actor.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <yql/essentials/minikql/mkql_alloc.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/core/testlib/basics/runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_blocking_queue.h>

namespace NYql::NDq {

class IMockPqGateway : public NYql::IPqGateway {
public:
    virtual std::shared_ptr<NYql::TBlockingEQueue> GetEventQueue(const TString& topic) = 0; 
};

NYdb::NTopic::TPartitionSession::TPtr CreatePartitionSession();

TIntrusivePtr<IMockPqGateway> CreateMockPqGateway();

}
