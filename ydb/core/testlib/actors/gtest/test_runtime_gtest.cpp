#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/library/actors/core/event_local.h>
#include <library/cpp/testing/gtest/gtest.h>

namespace NKikimr {
namespace {

    using namespace NActors;

    struct TEvResponse : TEventLocal<TEvResponse, EventSpaceBegin(TEvents::ES_PRIVATE)> {
    };

    class TRespondingActor : public TActorBootstrapped<TRespondingActor> {
    public:
        explicit TRespondingActor(TActorId recipient)
            : Recipient(recipient)
        {
        }

        void Bootstrap() {
            Send(Recipient, new TEvResponse);
            PassAway();
        }

    private:
        const TActorId Recipient;
    };

    TEST(TTestActorRuntimeGTest, DispatchesActorEvents) {
        TTestBasicRuntime runtime;
        TAppPrepare app;
        app.ClearDomainsAndHive();
        runtime.Initialize(app.Unwrap());

        const TActorId edge = runtime.AllocateEdgeActor();
        runtime.Register(new TRespondingActor(edge));

        auto response = runtime.GrabEdgeEvent<TEvResponse>(edge);
        ASSERT_TRUE(response);
    }

} // anonymous namespace
} // namespace NKikimr
