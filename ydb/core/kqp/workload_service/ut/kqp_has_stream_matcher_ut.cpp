#include <ydb/core/kqp/workload_service/kqp_has_stream_matcher.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {


Y_UNIT_TEST_SUITE(TStreamMatcherPredicate) {


    Y_UNIT_TEST(NulloptAcceptsAny) {
        // No filter — every query passes regardless of streaming ops.
        {
            TUserRequestContext userRequestContext;
            userRequestContext.IsStreamingQuery = false;
            UNIT_ASSERT(NWorkload::MatchesStream(std::nullopt, userRequestContext));
        }
        {
            TUserRequestContext userRequestContext;
            userRequestContext.IsStreamingQuery = true;
            UNIT_ASSERT(NWorkload::MatchesStream(std::nullopt, userRequestContext));
        }
    }

    Y_UNIT_TEST(TrueMatchesStreamingQuery) {
        TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;

        UNIT_ASSERT(NWorkload::MatchesStream(true, userRequestContext));
    }

    Y_UNIT_TEST(TrueDoesNotMatchNonStreamingQuery) {
        TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = false;
        UNIT_ASSERT(!NWorkload::MatchesStream(true, userRequestContext));
    }

    Y_UNIT_TEST(FalseMatchesNonStreamingQuery) {
        TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = false;
        UNIT_ASSERT(NWorkload::MatchesStream(false, userRequestContext));
    }

    Y_UNIT_TEST(FalseDoesNotMatchStreamingQuery) {
        TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;
        UNIT_ASSERT(!NWorkload::MatchesStream(false, userRequestContext));
    }
}

}  // namespace NKikimr::NKqp
