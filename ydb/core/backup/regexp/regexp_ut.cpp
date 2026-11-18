#include "regexp.h"
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup {

std::vector<TRegExMatch> CallCombineRegexps(const std::vector<TString>& regexps) {
    google::protobuf::RepeatedPtrField<TString> repeatedField(regexps.begin(), regexps.end()); // stores pointers to TString's
    return CombineRegexps(repeatedField);
}

Y_UNIT_TEST_SUITE(CallCombineRegexpsTest) {
    Y_UNIT_TEST(BaseCase) {
        auto regexps = CallCombineRegexps({"foo", "bar[0-9]+"});

        UNIT_ASSERT_VALUES_EQUAL(regexps.size(), 1);
        UNIT_ASSERT(regexps.front().Match("foo"));
        UNIT_ASSERT(regexps.front().Match("xxxbar123yyy"));
        UNIT_ASSERT(!regexps.front().Match("qux"));
    }

    Y_UNIT_TEST(EmptyInputReturnsEmpty) {
        auto regexps = CallCombineRegexps({});

        UNIT_ASSERT(regexps.empty());
    }

    Y_UNIT_TEST(InvalidRegexpThrows) {
        UNIT_ASSERT_EXCEPTION(CallCombineRegexps({"(", ".*"}), std::exception);
        UNIT_ASSERT_EXCEPTION(CallCombineRegexps({"foo", "("}), std::exception);
    }

    Y_UNIT_TEST(NamedGroupsCollisionFallback) {
        const std::vector<TString> patterns = {"(?<value>foo)", "(?<value>bar)"};

        auto regexps = CallCombineRegexps(patterns);

        UNIT_ASSERT_VALUES_EQUAL(regexps.size(), patterns.size());
        UNIT_ASSERT(regexps[0].Match("foo"));
        UNIT_ASSERT(!regexps[0].Match("bar"));
        UNIT_ASSERT(regexps[1].Match("bar"));
    }

    Y_UNIT_TEST(NamedGroupsNoCollisionCombines) {
        auto regexps = CallCombineRegexps({"(?<a>foo)", "(?<b>bar)"});

        UNIT_ASSERT_VALUES_EQUAL(regexps.size(), 1);
        UNIT_ASSERT(regexps.front().Match("foo"));
        UNIT_ASSERT(regexps.front().Match("bar"));
    }

    Y_UNIT_TEST(SpecialCharactersPreservedInCombinedRegexp) {
        auto regexps = CallCombineRegexps({"n|m", "(s)", "^abc", "xyz$"});

        UNIT_ASSERT_VALUES_EQUAL(regexps.size(), 1);
        UNIT_ASSERT(regexps.front().Match("n"));
        UNIT_ASSERT(regexps.front().Match("m"));
        UNIT_ASSERT(regexps.front().Match("s"));
        UNIT_ASSERT(!regexps.front().Match("d"));
        UNIT_ASSERT(regexps.front().Match("abcdef"));
        UNIT_ASSERT(!regexps.front().Match("defabc"));
        UNIT_ASSERT(regexps.front().Match("uvwxyz"));
        UNIT_ASSERT(!regexps.front().Match("xyzuvw"));
    }
}

} // namespace NKikimr::NBackup
