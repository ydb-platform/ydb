#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <latch>
#include <thread>

namespace NKikimr::NYDBTest {
namespace {

class TTestCSController: public ICSController {
public:
    bool IsForcedGenerateInternalPathId() const override {
        return true;
    }
};

}   // namespace

Y_UNIT_TEST_SUITE(TColumnShardControllers) {
    Y_UNIT_TEST(ReaderKeepsControllerAliveAfterGuardReset) {
        ICSController::TPtr retained;
        std::weak_ptr<ICSController> weak;
        {
            auto guard = TControllers::RegisterCSControllerGuard<TTestCSController>();
            retained = TControllers::GetColumnShardController();
            weak = retained;
            UNIT_ASSERT(retained.get() == guard.operator->());
            UNIT_ASSERT(retained->IsForcedGenerateInternalPathId());
        }

        auto current = TControllers::GetColumnShardController();
        UNIT_ASSERT(current);
        UNIT_ASSERT(current != retained);
        UNIT_ASSERT(!current->IsForcedGenerateInternalPathId());
        UNIT_ASSERT(!weak.expired());
        UNIT_ASSERT(retained->IsForcedGenerateInternalPathId());

        retained.reset();
        UNIT_ASSERT(weak.expired());
    }

    Y_UNIT_TEST(ConcurrentReadAndGuardReset) {
        constexpr size_t ReaderCount = 4;
        constexpr size_t Iterations = 10000;
        std::latch start(ReaderCount + 1);
        std::array<std::thread, ReaderCount> readers;
        std::array<size_t, ReaderCount> readCounts{};

        UNIT_ASSERT(TControllers::GetColumnShardController());
        for (size_t i = 0; i < ReaderCount; ++i) {
            readers[i] = std::thread([&, i] {
                start.arrive_and_wait();
                for (size_t iteration = 0; iteration < Iterations; ++iteration) {
                    auto controller = TControllers::GetColumnShardController();
                    if (controller) {
                        ++readCounts[i];
                    }
                }
            });
        }

        start.arrive_and_wait();
        for (size_t iteration = 0; iteration < Iterations; ++iteration) {
            auto guard = TControllers::RegisterCSControllerGuard<TTestCSController>();
        }

        for (auto& reader : readers) {
            reader.join();
        }

        for (const auto readCount : readCounts) {
            UNIT_ASSERT_VALUES_EQUAL(readCount, Iterations);
        }

        UNIT_ASSERT(!TControllers::GetColumnShardController()->IsForcedGenerateInternalPathId());
    }
}

}   // namespace NKikimr::NYDBTest
