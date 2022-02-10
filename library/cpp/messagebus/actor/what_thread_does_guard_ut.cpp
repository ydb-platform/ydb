#include <library/cpp/testing/unittest/registar.h>

#include "what_thread_does_guard.h"

#include <util/system/mutex.h>

Y_UNIT_TEST_SUITE(WhatThreadDoesGuard) {
    Y_UNIT_TEST(Simple) {
        TMutex mutex;

        TWhatThreadDoesAcquireGuard<TMutex> guard(mutex, "acquiring my mutex");
    }
}
