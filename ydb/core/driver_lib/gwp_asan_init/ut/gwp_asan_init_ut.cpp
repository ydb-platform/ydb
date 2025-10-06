#include <ydb/core/driver_lib/gwp_asan_init/gwp_asan_init.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(GwpAsanInit) {
    Y_UNIT_TEST(InitializeGwpAsanDoesNotCrash) {
        // Test that GwpAsan initialization doesn't crash
        // This should be safe to call multiple times
        NKikimr::InitializeGwpAsan();
        NKikimr::InitializeGwpAsan(); // Second call should be safe
        
        // If we reach here without crashing, the test passes
        UNIT_ASSERT(true);
    }
}