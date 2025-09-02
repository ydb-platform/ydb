#include "actorsystem.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TActorSystemTest) {

    class TTestActor: public TActor<TTestActor> {
    public:
        TTestActor()
            : TActor{&TThis::Main}
        {
        }

        STATEFN(Main) {
            Y_UNUSED(ev);
        }
    };

    THolder<TTestActorRuntimeBase> CreateRuntime() {
        auto runtime = MakeHolder<TTestActorRuntimeBase>();
        runtime->SetScheduledEventFilter([](auto&&, auto&&, auto&&, auto&&) { return false; });
        runtime->Initialize();
        return runtime;
    }

    Y_UNIT_TEST(LocalService) {
        THolder<TTestActorRuntimeBase> runtime = CreateRuntime();
        auto actorA = runtime->Register(new TTestActor);
        auto actorB = runtime->Register(new TTestActor);

        TActorId myServiceId{0, TStringBuf{"my-service"}};

        auto prevActorId = runtime->RegisterService(myServiceId, actorA);
        UNIT_ASSERT(!prevActorId);
        UNIT_ASSERT_EQUAL(runtime->GetLocalServiceId(myServiceId), actorA);

        prevActorId = runtime->RegisterService(myServiceId, actorB);
        UNIT_ASSERT(prevActorId);
        UNIT_ASSERT_EQUAL(prevActorId, actorA);
        UNIT_ASSERT_EQUAL(runtime->GetLocalServiceId(myServiceId), actorB);
    }
}
