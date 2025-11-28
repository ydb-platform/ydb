#include "url_matcher.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NMonitoring::NAudit;

Y_UNIT_TEST_SUITE(TUrlMatcherTest) {
    Y_UNIT_TEST(MatchExactPathOnly) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/a/b/c", .Recursive = false});

        UNIT_ASSERT(matcher.Match("/a/b/c"));
        UNIT_ASSERT(matcher.Match("a/b/c"));
        UNIT_ASSERT(matcher.Match("/a/b/c?action=start"));

        UNIT_ASSERT(!matcher.Match(""));
        UNIT_ASSERT(!matcher.Match("/"));
        UNIT_ASSERT(!matcher.Match("/a/b"));
        UNIT_ASSERT(!matcher.Match("/c/a/b"));
        UNIT_ASSERT(!matcher.Match("/c/b/a"));
        UNIT_ASSERT(!matcher.Match("/a/b/z"));
        UNIT_ASSERT(!matcher.Match("/A/B/C"));
        UNIT_ASSERT(!matcher.Match("//a/b/c"));
        UNIT_ASSERT(!matcher.Match("/a/b///c"));
        UNIT_ASSERT(!matcher.Match("/a/b/c/d"));
        UNIT_ASSERT(!matcher.Match("/a/b/c/d/e/f"));
    }

    Y_UNIT_TEST(MatchRecursive) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/actors/blobstorageproxies", .Recursive = true});

        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies"));
        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies/blobstorageproxy2181038080"));
        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies/blobstorageproxy2181038080?PutSamplingRate=1"));
        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies/somethingelse?PutSamplingRate=123"));

        UNIT_ASSERT(!matcher.Match("/actors/blobstorageproxies123"));
        UNIT_ASSERT(!matcher.Match("/actors/otherproxy/blobstorageproxy2181038080?PutSamplingRate=1"));
    }
}
