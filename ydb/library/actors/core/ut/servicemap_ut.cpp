#include "../actorid.h"
#include "../servicemap.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <latch>
#include <thread>
#include <vector>

namespace NActors {

Y_UNIT_TEST_SUITE(ServiceMap) {

    Y_UNIT_TEST(ConcurrentUpdateAndFind) {
        constexpr size_t readerCount = 4;
        constexpr size_t updateCount = 1'000'000;

        TServiceMap<TActorId, TActorId, TActorId::THash, 1, 2, 2> map;
        const TActorId serviceId(1, TStringBuf("service"));
        const TActorId actorId1(1, 0, 1, 0);
        const TActorId actorId2(1, 0, 2, 0);
        map.Update(serviceId, actorId1);

        std::atomic<bool> stop = false;
        std::atomic<size_t> invalidReads = 0;
        std::latch start(readerCount + 1);
        std::vector<std::thread> readers;
        readers.reserve(readerCount);

        for (size_t i = 0; i != readerCount; ++i) {
            readers.emplace_back([&] {
                start.arrive_and_wait();
                while (!stop.load(std::memory_order_relaxed)) {
                    const TActorId actorId = map.Find(serviceId);
                    if (actorId && actorId != actorId1 && actorId != actorId2) {
                        invalidReads.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            });
        }

        start.arrive_and_wait();
        for (size_t i = 0; i != updateCount; ++i) {
            map.Update(serviceId, i % 2 ? actorId1 : actorId2);
        }
        stop.store(true, std::memory_order_relaxed);

        for (std::thread& reader : readers) {
            reader.join();
        }

        UNIT_ASSERT_VALUES_EQUAL(invalidReads.load(), 0);
    }
}

} // namespace NActors
