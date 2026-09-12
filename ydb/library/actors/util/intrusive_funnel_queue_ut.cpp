#include "intrusive_funnel_queue.h"

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace {

struct TItem
    : TIntrusiveFunnelQueueItem<TItem>
{
    size_t Producer = 0;
    size_t Sequence = 0;
};

} // namespace

Y_UNIT_TEST_SUITE(TIntrusiveFunnelQueueTest) {

    Y_UNIT_TEST(FifoAndReuse) {
        TIntrusiveFunnelQueue<TItem> queue;
        TItem first;
        TItem second;
        TItem third;

        UNIT_ASSERT(queue.IsEmpty());
        UNIT_ASSERT(queue.Push(&first));
        UNIT_ASSERT(!queue.Push(&second));
        UNIT_ASSERT(!queue.Push(&third));

        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), &first);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), &second);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), &third);
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), nullptr);
        UNIT_ASSERT(queue.IsEmpty());

        UNIT_ASSERT(queue.Push(&first));
        UNIT_ASSERT_VALUES_EQUAL(queue.Pop(), &first);
    }

    Y_UNIT_TEST(MultipleProducersSingleConsumer) {
        constexpr size_t ProducerCount = 4;
        constexpr size_t ItemsPerProducer = 10000;

        TIntrusiveFunnelQueue<TItem> queue;
        std::array<std::unique_ptr<TItem[]>, ProducerCount> items;
        std::atomic<bool> start = false;
        std::vector<std::thread> producers;
        producers.reserve(ProducerCount);

        for (size_t producer = 0; producer < ProducerCount; ++producer) {
            items[producer] = std::make_unique<TItem[]>(ItemsPerProducer);
            for (size_t sequence = 0; sequence < ItemsPerProducer; ++sequence) {
                items[producer][sequence].Producer = producer;
                items[producer][sequence].Sequence = sequence;
            }

            producers.emplace_back([&, producer] {
                while (!start.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                for (size_t sequence = 0; sequence < ItemsPerProducer; ++sequence) {
                    queue.Push(&items[producer][sequence]);
                }
            });
        }

        start.store(true, std::memory_order_release);

        std::array<size_t, ProducerCount> nextSequence{};
        bool fifo = true;
        size_t consumed = 0;
        while (consumed < ProducerCount * ItemsPerProducer) {
            if (TItem* item = queue.Pop()) {
                if (item->Producer < ProducerCount) {
                    fifo &= item->Sequence == nextSequence[item->Producer];
                    ++nextSequence[item->Producer];
                } else {
                    fifo = false;
                }
                ++consumed;
            } else {
                std::this_thread::yield();
            }
        }

        for (auto& producer : producers) {
            producer.join();
        }
        UNIT_ASSERT(fifo);
        for (size_t sequence : nextSequence) {
            UNIT_ASSERT_VALUES_EQUAL(sequence, ItemsPerProducer);
        }
        UNIT_ASSERT(queue.IsEmpty());
    }
}
