#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>


#include <ydb/library/yql/providers/dq/actors/events.h>

#include "result_receiver.h"
#include "result_actor_base.h"

using namespace NActors;
using namespace NYql;
using namespace NYql::NDqs;

Y_UNIT_TEST_SUITE(ResultReceiver) {

auto ResultReceiver(TActorId executerId = {}) {
    TDqConfiguration::TPtr settings = new TDqConfiguration();
    auto receiver = MakeResultReceiver(
        {}, // columns,
        executerId,
        "traceId",
        settings,
        {}, // secureParams
        "", // resultType ?
        {}, // graphExecutionEventsId ?
        false // discard
    );

    return receiver;
}

Y_UNIT_TEST(ReceiveStatus) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto sender = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver().Release());
    runtime.Send(new IEventHandle(receiverId, sender, new TEvReadyState(), 0, true));
}

Y_UNIT_TEST(ReceiveError) {
    TTestActorRuntimeBase runtime;
    runtime.Initialize();

    auto executerId = runtime.AllocateEdgeActor();
    auto receiverId = runtime.Register(ResultReceiver(executerId).Release());
    runtime.Send(new IEventHandle(receiverId, {}, new NActors::TEvents::TEvUndelivered(0,0), 0, true));

    auto response = runtime.GrabEdgeEvent<TEvDqFailure>();
    UNIT_ASSERT_EQUAL(response->Record.GetStatusCode(), NYql::NDqProto::StatusIds::UNAVAILABLE);
}

Y_UNIT_TEST(WriteQueue) {
    NYql::NDqs::NExecutionHelpers::TWriteQueue q;
    UNIT_ASSERT(q.empty());

    NYql::NDqs::NExecutionHelpers::TQueueItem item({}, ""); item.Size = 1000;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1000);

    item.Size = 11;
    q.emplace(item);
    UNIT_ASSERT_EQUAL(q.ByteSize, 1011);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 11);

    q.pop();
    UNIT_ASSERT_EQUAL(q.ByteSize, 0);
}

} // Y_UNIT_TEST_SUITE(ResultReceiver)
