#include <library/cpp/testing/unittest/registar.h>
#include "http_router.h"

using namespace NKikimr::NPublicHttp;
using namespace NActors;

namespace {
THttpHandler AsTestHandler(IActor* a) {
    return [a](const THttpRequestContext&) {
        return a;
    };
}
}

Y_UNIT_TEST_SUITE(HttpRouter) {
    Y_UNIT_TEST(Basic) {
        IActor* const a1 = reinterpret_cast<IActor*>(1);
        IActor* const a2 = reinterpret_cast<IActor*>(2);
        THolder<TActorSystemSetup> setup(new TActorSystemSetup());
        TActorSystem actorSystem(setup);
        THttpRequestContext ctx(&actorSystem, new NHttp::THttpIncomingRequest(), {}, {}, {});

        THttpRequestRouter router;
        router.RegisterHandler(HTTP_METHOD_GET, "/apix/v1/fq/query", AsTestHandler(a1));
        router.RegisterHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/{query_id}", AsTestHandler(a2));
        UNIT_ASSERT_VALUES_EQUAL(router.GetSize(), 2);

        std::optional<THandlerWithParams> resolve1 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq"sv);
        UNIT_ASSERT(!resolve1);

        std::optional<THandlerWithParams> resolve2 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/1234/status"sv);
        UNIT_ASSERT(!resolve2);

        std::optional<THandlerWithParams> resolve3 = router.ResolveHandler(HTTP_METHOD_POST, "/apix/v1/fq/query"sv);
        UNIT_ASSERT(!resolve3);

        std::optional<THandlerWithParams> resolve4 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query"sv);
        UNIT_ASSERT(resolve4);
        UNIT_ASSERT_VALUES_EQUAL(resolve4->PathPattern, "/apix/v1/fq/query");
        UNIT_ASSERT(resolve4->Handler);
        UNIT_ASSERT_EQUAL(resolve4->Handler(ctx), a1);
        UNIT_ASSERT_VALUES_EQUAL(resolve4->PathParams.size(), 0u);

        std::optional<THandlerWithParams> resolve5 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/1234"sv);
        UNIT_ASSERT(resolve5);
        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathPattern, "/apix/v1/fq/query/{query_id}");
        UNIT_ASSERT(resolve5->Handler);
        UNIT_ASSERT_EQUAL(resolve5->Handler(ctx), a2);
        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.size(), 1u);

        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.begin()->first, "query_id");
        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.begin()->second, "1234");
    }
}
