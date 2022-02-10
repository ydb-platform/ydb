#include "wildcard.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
Y_UNIT_TEST_SUITE(TWildcardTest) {
    Y_UNIT_TEST(TestWildcard) {
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "*/*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "some/test/string"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "some*string"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "some*str?ng"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "some*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "*string"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("some/test/string", "*/test/*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("", "*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcard("", ""));
        UNIT_ASSERT(!NKikimr::IsMatchesWildcard("some/test/string", "ssome*"));
        UNIT_ASSERT(!NKikimr::IsMatchesWildcard("some/test/string", "*strring"));
        UNIT_ASSERT(!NKikimr::IsMatchesWildcard("", "*/test/*"));
    }

    Y_UNIT_TEST(TestWildcards) {
        UNIT_ASSERT(NKikimr::IsMatchesWildcards("some/test/string", "ssome*,*strring,*"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcards("some/test/string", "ssome*,*,*strring"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcards("some/test/string", "*,ssome*,*strring"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcards("some/test/string", "ssome*,*/*,*strring"));
        UNIT_ASSERT(NKikimr::IsMatchesWildcards("some/test/string", "ssome*,*/test/*,*strring"));
    }
}

} // NKikimr
