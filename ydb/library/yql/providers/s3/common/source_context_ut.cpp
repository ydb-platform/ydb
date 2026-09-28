#include "source_context.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDq {

namespace {

TSourceContext::TPtr MakeSourceContext() {
    return std::make_shared<TSourceContext>(NActors::TActorId{}, 1000000, nullptr,
        nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr);
}

}

Y_UNIT_TEST_SUITE(TestSourceContextRatio) {
    Y_UNIT_TEST(NothingDownloaded) {
        UNIT_ASSERT_DOUBLES_EQUAL(MakeSourceContext()->Ratio(), 1.0, 1e-9);
    }

    Y_UNIT_TEST(NothingDecodedYet) {
        auto context = MakeSourceContext();
        context->UpdateProgress(1000, 0, 0);
        UNIT_ASSERT_DOUBLES_EQUAL(context->Ratio(), 1.0, 1e-9);
    }

    Y_UNIT_TEST(DecodingExpandsData) {
        auto context = MakeSourceContext();
        context->UpdateProgress(1000, 3500, 10);
        UNIT_ASSERT_DOUBLES_EQUAL(context->Ratio(), 3.5, 1e-9);
    }

    Y_UNIT_TEST(RatioAccumulatesOverUpdates) {
        auto context = MakeSourceContext();
        context->UpdateProgress(1000, 4000, 10);
        context->UpdateProgress(3000, 4000, 10);
        UNIT_ASSERT_DOUBLES_EQUAL(context->Ratio(), 2.0, 1e-9);
    }

    Y_UNIT_TEST(DecodingShrinksData) {
        auto context = MakeSourceContext();
        context->UpdateProgress(1000, 250, 10);
        UNIT_ASSERT_DOUBLES_EQUAL(context->Ratio(), 1.0, 1e-9);
    }
}

} // namespace NYql::NDq
