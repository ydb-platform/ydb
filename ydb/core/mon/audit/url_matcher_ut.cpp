#include "url_matcher.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NMonitoring::NAudit;

Y_UNIT_TEST_SUITE(TUrlMatcherTest) {
    Y_UNIT_TEST(MatchExactPathOnly) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/a/b/c"});

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
    }

    Y_UNIT_TEST(MatchWithParamNameOnly) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/a/b", .ParamName = "mode"});

        UNIT_ASSERT(matcher.Match("/a/b?mode="));
        UNIT_ASSERT(matcher.Match("/a/b?mode="));
        UNIT_ASSERT(matcher.Match("/a/b?mode=1"));
        UNIT_ASSERT(matcher.Match("/a/b?other=1&mode=1"));

        UNIT_ASSERT(!matcher.Match("/a/b"));
        UNIT_ASSERT(!matcher.Match("/a?mode=1"));
        UNIT_ASSERT(!matcher.Match("/a/b?other=1"));
        UNIT_ASSERT(!matcher.Match("/a/b/c?mode=1"));
    }

    Y_UNIT_TEST(MatchWithParamNameAndValue) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/a/b", .ParamName = "action", .ParamValue = "start"});
        matcher.AddPattern({.Path = "/a/b", .ParamName = "action", .ParamValue = "stop"});

        UNIT_ASSERT(matcher.Match("/a/b?action=start"));
        UNIT_ASSERT(matcher.Match("/a/b?action=stop"));
        UNIT_ASSERT(matcher.Match("/a/b?k=stop&action=start"));

        UNIT_ASSERT(!matcher.Match("/a/b"));
        UNIT_ASSERT(!matcher.Match("/a/b?action=restart"));
        UNIT_ASSERT(!matcher.Match("/a/b?k=stop"));
        UNIT_ASSERT(!matcher.Match("/a/b/c?action=start"));
    }

    Y_UNIT_TEST(MatchWithWildcardPath) {
        NMonitoring::NAudit::TUrlMatcher matcher;
        matcher.AddPattern({.Path = "/actors/blobstorageproxies/*", .ParamName = "PutSamplingRate"});

        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies/blobstorageproxy2181038080?PutSamplingRate=1"));
        UNIT_ASSERT(matcher.Match("/actors/blobstorageproxies/somethingelse?PutSamplingRate=123"));

        UNIT_ASSERT(!matcher.Match("/actors/blobstorageproxies"));
        UNIT_ASSERT(!matcher.Match("/actors/blobstorageproxies/blobstorageproxy2181038080"));
        UNIT_ASSERT(!matcher.Match("/actors/blobstorageproxies/blobstorageproxy2181038080?OtherParam=1"));
        UNIT_ASSERT(!matcher.Match("/actors/otherproxy/blobstorageproxy2181038080?PutSamplingRate=1"));
    }
}
