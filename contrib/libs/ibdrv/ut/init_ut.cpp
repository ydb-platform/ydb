#include <library/cpp/testing/unittest/registar.h>

#ifdef _linux_
#include <verbs_loader.h>
#include <infiniband/verbs.h>

#include <cerrno>
#include <cstring>
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

    Y_UNIT_TEST(LoaderErrorsDoNotThrow) {
#ifdef _linux_
        char error[256] = {};
        UNIT_ASSERT_VALUES_EQUAL(
            ibdrv_try_load_ibverbs(nullptr, 1, error, sizeof(error)),
            -EINVAL);
        UNIT_ASSERT(std::strlen(error) > 0);

        const char* required[] = {"__ibdrv_test_missing_symbol__"};
        const int result = ibdrv_try_load_ibverbs(required, 1, error, sizeof(error));
        UNIT_ASSERT_C(result == -ENOENT || result == -ENOSYS, error);
        UNIT_ASSERT(std::strlen(error) > 0);
#endif
    }
}
