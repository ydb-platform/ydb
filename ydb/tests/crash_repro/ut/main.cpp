#include <csignal>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(CrashRepro) {
    Y_UNIT_TEST(Segfault) {
        std::raise(SIGSEGV);
    }

    Y_UNIT_TEST(Timeout) {
        for (;;) {
        }
    }
}
