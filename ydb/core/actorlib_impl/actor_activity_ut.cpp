#include <ydb/core/actorlib_impl/defs.h>


#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>


#include <library/cpp/actors/core/actor.h>
#include <ydb/core/protos/services.pb.h>

namespace NActors {

class TTestActor : public TActor<TTestActor> {
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::ASYNC_DESTROYER;
    }

    TTestActor()
        : TActor(nullptr)
    { }
};

} // namespace NActors

Y_UNIT_TEST_SUITE(TActorActivity) {
    using namespace NActors;

    Y_UNIT_TEST(Basic) {
        TAutoPtr<IActor> actor = new TTestActor();
        UNIT_ASSERT_VALUES_EQUAL(actor->GetActivityType(), static_cast<ui32>(NKikimrServices::TActivity::ASYNC_DESTROYER));

        UNIT_ASSERT((TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetIndexByName("ASYNC_DESTROYER") == static_cast<ui32>(NKikimrServices::TActivity::ASYNC_DESTROYER)));

        UNIT_ASSERT((TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(static_cast<ui32>(NKikimrServices::TActivity::ASYNC_DESTROYER)) == "ASYNC_DESTROYER"));
    }
}
