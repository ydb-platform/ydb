#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <thread>

namespace NKikimr::NYDBTest {

namespace {

class TTestController: public ICSController {};

}   // namespace

Y_UNIT_TEST_SUITE(TControllersTest) {
    Y_UNIT_TEST(GetColumnShardControllerWhileGuardsReplaceIt) {
        // Tablets read the controller from actor threads while tests register and release guards.
        std::atomic<bool> stop = false;
        std::atomic<ui64> reads = 0;
        std::thread reader([&] {
            while (!stop.load()) {
                Y_ABORT_UNLESS(TControllers::GetColumnShardController());
                reads.fetch_add(1);
            }
        });
        while (reads.load() == 0) {
            std::this_thread::yield();
        }
        const ui64 readsBefore = reads.load();
        for (ui32 i = 0; i < 100000; ++i) {
            auto guard = TControllers::RegisterCSControllerGuard<TTestController>();
        }
        const ui64 readsDuring = reads.load() - readsBefore;
        stop = true;
        reader.join();
        UNIT_ASSERT_C(readsDuring > 0, "the reader made no reads while the controller was replaced");
    }

    Y_UNIT_TEST(MoveAssignedGuardKeepsController) {
        auto first = TControllers::RegisterCSControllerGuard<TTestController>();
        auto second = TControllers::RegisterCSControllerGuard<TTestController>();
        TTestController* registered = TControllers::GetControllerAs<TTestController>();
        UNIT_ASSERT(registered);
        auto& assigned = (first = std::move(second));
        UNIT_ASSERT_EQUAL(&assigned, &first);
        UNIT_ASSERT_EQUAL(first.operator->(), registered);
    }
}

}   // namespace NKikimr::NYDBTest
