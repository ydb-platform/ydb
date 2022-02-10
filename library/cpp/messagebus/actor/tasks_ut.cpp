#include <library/cpp/testing/unittest/registar.h>

#include "tasks.h"

using namespace NActor;

Y_UNIT_TEST_SUITE(TTasks) {
    Y_UNIT_TEST(AddTask_FetchTask_Simple) {
        TTasks tasks;

        UNIT_ASSERT(tasks.AddTask());
        UNIT_ASSERT(!tasks.AddTask());
        UNIT_ASSERT(!tasks.AddTask());

        UNIT_ASSERT(tasks.FetchTask());
        UNIT_ASSERT(!tasks.FetchTask());

        UNIT_ASSERT(tasks.AddTask());
    }

    Y_UNIT_TEST(AddTask_FetchTask_AddTask) {
        TTasks tasks;

        UNIT_ASSERT(tasks.AddTask());
        UNIT_ASSERT(!tasks.AddTask());

        UNIT_ASSERT(tasks.FetchTask());
        UNIT_ASSERT(!tasks.AddTask());
        UNIT_ASSERT(tasks.FetchTask());
        UNIT_ASSERT(!tasks.AddTask());
        UNIT_ASSERT(!tasks.AddTask());
        UNIT_ASSERT(tasks.FetchTask());
        UNIT_ASSERT(!tasks.FetchTask());

        UNIT_ASSERT(tasks.AddTask());
    }
}
