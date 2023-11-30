#include <ydb/core/actorlib_impl/defs.h>


#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>


#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>

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
        const ui32 activityIndex = actor->GetActivityType();

        UNIT_ASSERT_VALUES_EQUAL(TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetIndexByName("ASYNC_DESTROYER"), activityIndex);

        Cerr << TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(activityIndex) << Endl;
        UNIT_ASSERT(TLocalProcessKeyState<TActorActivityTag>::GetInstance().GetNameByIndex(activityIndex) == "ASYNC_DESTROYER");
    }
}
