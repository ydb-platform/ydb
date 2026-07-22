#include <ydb/services/workload_manager/has_stream_matcher.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {


Y_UNIT_TEST_SUITE(TStreamMatcherPredicate) {


    Y_UNIT_TEST(NulloptAcceptsAny) {
        // No filter — every query passes regardless of streaming ops.
        {
            NKqp::TUserRequestContext userRequestContext;
            userRequestContext.IsStreamingQuery = false;
            UNIT_ASSERT(MatchesStream(std::nullopt, userRequestContext));
        }
        {
            NKqp::TUserRequestContext userRequestContext;
            userRequestContext.IsStreamingQuery = true;
            UNIT_ASSERT(MatchesStream(std::nullopt, userRequestContext));
        }
    }

    Y_UNIT_TEST(TrueMatchesStreamingQuery) {
        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;

        UNIT_ASSERT(MatchesStream(true, userRequestContext));
    }

    Y_UNIT_TEST(TrueDoesNotMatchNonStreamingQuery) {
        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = false;
        UNIT_ASSERT(!MatchesStream(true, userRequestContext));
    }

    Y_UNIT_TEST(FalseMatchesNonStreamingQuery) {
        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = false;
        UNIT_ASSERT(MatchesStream(false, userRequestContext));
    }

    Y_UNIT_TEST(FalseDoesNotMatchStreamingQuery) {
        NKqp::TUserRequestContext userRequestContext;
        userRequestContext.IsStreamingQuery = true;
        UNIT_ASSERT(!MatchesStream(false, userRequestContext));
    }
}

}  // namespace NKikimr::NWorkloadManager
