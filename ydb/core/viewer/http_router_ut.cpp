#include <library/cpp/testing/unittest/registar.h>
#include "http_router.h"

using namespace NKikimr;
using namespace NViewer;

namespace {
class TTestHandler : public TJsonHandlerBase {
    int Id;
public:
    explicit TTestHandler(int id)
        : Id(id)
    {
    }

    int GetId() const { return Id; }
    virtual IActor* CreateRequestActor(IViewer* , const TRequest&) { return nullptr; }
    virtual TString GetResponseJsonSchema() { return TString(); }
};

std::shared_ptr<TTestHandler> AsTestHandler(TJsonHandlerBase::TPtr h) {
    return std::static_pointer_cast<TTestHandler>(h);
}

}
Y_UNIT_TEST_SUITE(HttpRouter) {
    Y_UNIT_TEST(Basic) {
        THttpRequestRouter router;
        router.RegisterHandler(HTTP_METHOD_GET, "/apix/v1/fq/query", std::make_shared<TTestHandler>(1));
        router.RegisterHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/{query_id}", std::make_shared<TTestHandler>(2));
        UNIT_ASSERT_VALUES_EQUAL(router.GetSize(), 2);

        std::optional<THandlerWithParams> resolve1 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq"sv);
        UNIT_ASSERT(!resolve1);

        std::optional<THandlerWithParams> resolve2 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/1234/status"sv);
        UNIT_ASSERT(!resolve2);

        std::optional<THandlerWithParams> resolve3 = router.ResolveHandler(HTTP_METHOD_POST, "/apix/v1/fq/query"sv);
        UNIT_ASSERT(!resolve3);

        std::optional<THandlerWithParams> resolve4 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query"sv);
        UNIT_ASSERT(resolve4);
        UNIT_ASSERT(resolve4->Handler);
        UNIT_ASSERT_VALUES_EQUAL(AsTestHandler(resolve4->Handler)->GetId(), 1);
        UNIT_ASSERT_VALUES_EQUAL(resolve4->PathParams.size(), 0u);

        std::optional<THandlerWithParams> resolve5 = router.ResolveHandler(HTTP_METHOD_GET, "/apix/v1/fq/query/1234"sv);
        UNIT_ASSERT(resolve5);
        UNIT_ASSERT(resolve5->Handler);
        UNIT_ASSERT_VALUES_EQUAL(AsTestHandler(resolve5->Handler)->GetId(), 2);
        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.size(), 1u);

        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.begin()->first, "query_id");
        UNIT_ASSERT_VALUES_EQUAL(resolve5->PathParams.begin()->second, "1234");
    }
}
