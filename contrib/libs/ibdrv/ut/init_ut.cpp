#include <library/cpp/testing/unittest/registar.h>

#ifdef _linux_
#include <infiniband/verbs.h>
#endif

Y_UNIT_TEST_SUITE(TInitIbDrv) {

    Y_UNIT_TEST(Init) {
#ifdef _linux_
        try {
            ibv_fork_init();
        } catch (...) {
        }
#endif
    }
}
