#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>


#include <ydb/library/yql/providers/dq/actors/events.h>

#include "result_receiver.h"

using namespace NActors;
using namespace NYql;
using namespace NYql::NDqs;

Y_UNIT_TEST_SUITE(ResultReceiver) {

auto ResultReceiver() {
    TDqConfiguration::TPtr settings = new TDqConfiguration();
    auto receiver = MakeResultReceiver(
        {}, // columns,
        TActorId{}, // executerId
        "traceId",
        settings, // settings
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

} // Y_UNIT_TEST_SUITE(ResultReceiver) 

