
#include "url_tree.h"

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NActors;

Y_UNIT_TEST_SUITE(TAuditTest) {
    Y_UNIT_TEST(MatchExactPathOnly) {
        TUrlTree tree;
        tree.AddPattern({"/a/b/c"});

        UNIT_ASSERT(tree.Match("/a/b/c"));
        UNIT_ASSERT(tree.Match("/a/b/c", "action=start"));
        UNIT_ASSERT(tree.Match("/a/b/c/d"));

        UNIT_ASSERT(!tree.Match(""));
        UNIT_ASSERT(!tree.Match("/"));
        UNIT_ASSERT(!tree.Match("/a/b"));
        UNIT_ASSERT(!tree.Match("/c/a/b"));
        UNIT_ASSERT(!tree.Match("/c/b/a"));
        UNIT_ASSERT(!tree.Match("/a/b/z"));
        UNIT_ASSERT(!tree.Match("/A/B/C"));
    }

    Y_UNIT_TEST(MatchWithParamKeyOnly) {
        TUrlTree tree;
        tree.AddPattern({"/a/b", "mode"});

        UNIT_ASSERT(tree.Match("/a/b", "mode="));
        UNIT_ASSERT(tree.Match("/a/b", "mode="));
        UNIT_ASSERT(tree.Match("/a/b", "mode=1"));
        UNIT_ASSERT(tree.Match("/a/b", "other=1&mode=1"));

        UNIT_ASSERT(!tree.Match("/a/b"));
        UNIT_ASSERT(!tree.Match("/a", "mode=1"));
        UNIT_ASSERT(!tree.Match("/a/b", "other=1"));
    }

    Y_UNIT_TEST(MatchWithParamKeyAndValue) {
        TUrlTree tree;
        tree.AddPattern({"/a/b", "action", "start"});
        tree.AddPattern({"/a/b", "action", "stop"});

        UNIT_ASSERT(tree.Match("/a/b", "action=start"));
        UNIT_ASSERT(tree.Match("/a/b", "action=stop"));
        UNIT_ASSERT(tree.Match("/a/b", "k=stop&action=start"));
        UNIT_ASSERT(tree.Match("/a/b/c", "action=start"));

        UNIT_ASSERT(!tree.Match("/a/b"));
        UNIT_ASSERT(!tree.Match("/a/b", "action=restart"));
        UNIT_ASSERT(!tree.Match("/a/b", "k=stop"));
    }
}
