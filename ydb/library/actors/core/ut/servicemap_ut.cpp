#include "../actorid.h"
#include "../servicemap.h"

#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <latch>
#include <semaphore>
#include <thread>
#include <vector>

namespace NActors {

    namespace {

        struct TPause {
            std::binary_semaphore Reached{0};
            std::binary_semaphore Resume{0};

            void Wait() {
                Reached.release();
                Resume.acquire();
            }

            bool WaitUntilReached() {
                return Reached.try_acquire_for(std::chrono::seconds(10));
            }
        };

        thread_local TPause* KeyComparisonPause = nullptr;
        thread_local TPause* ValueLoadPause = nullptr;

        struct TKey {
            ui64 Id = 0;

            bool operator==(const TKey& other) const {
                // A colliding key separates the free slot from the service being moved.
                if (KeyComparisonPause && Id == 2 && other.Id == 1) {
                    auto* pause = KeyComparisonPause;
                    KeyComparisonPause = nullptr;
                    pause->Wait();
                }
                return Id == other.Id;
            }
        };

        struct TSameHash {
            ui64 operator()(const TKey&) const {
                return 0;
            }
        };

        void CheckReplacementDuringFind(size_t replacements, bool reuseOldSlot = false) {
            TServiceMap<TKey, ui64, TSameHash, 1, 4, 1> map;
            const TKey service{1};
            map.Update(service, 11); // slot 0
            map.Update(TKey{2}, 99); // slot 1
            map.Update(service, 22); // slot 2, leaving slot 0 free

            TPause pause;
            ui64 found = 0;
            std::thread reader([&] {
                KeyComparisonPause = &pause;
                found = map.Find(service);
                KeyComparisonPause = nullptr;
            });

            const bool reached = pause.WaitUntilReached();
            ui64 expected = 22;
            bool correctPreviousValues = true;
            if (reached) {
                for (size_t i = 0; i < replacements; ++i) {
                    const ui64 next = expected + 11;
                    correctPreviousValues &= map.Update(service, next) == expected;
                    expected = next;
                }
                if (reuseOldSlot) {
                    map.Update(TKey{3}, 77);
                }
            }
            pause.Resume.release();
            reader.join();

            UNIT_ASSERT_C(reached, "Reader did not reach the colliding key");
            UNIT_ASSERT(correctPreviousValues);
            UNIT_ASSERT_VALUES_EQUAL(map.Find(service), expected);
            // All updates finished before the reader visited the service's old slot.
            UNIT_ASSERT_VALUES_EQUAL(found, expected);
            UNIT_ASSERT_VALUES_EQUAL(map.Find(TKey{2}), 99);
            UNIT_ASSERT_VALUES_EQUAL(map.Find(TKey{3}), reuseOldSlot ? 77 : 0);
            UNIT_ASSERT_VALUES_EQUAL(map.Find(TKey{4}), 0);
            UNIT_ASSERT(map.Erase(service));
            UNIT_ASSERT_VALUES_EQUAL(map.Find(service), 0);
        }

        struct TValue {
            ui64 Id = 0;

            TValue() {
                // Update loads the old value after publishing the new entry, before
                // clearing the old one. Pause only the writer at that boundary.
                if (ValueLoadPause) {
                    auto* pause = ValueLoadPause;
                    ValueLoadPause = nullptr;
                    pause->Wait();
                }
            }

            explicit TValue(ui64 id)
                : Id(id)
            {
            }
        };

        static_assert(std::is_trivially_copyable_v<TKey>);
        static_assert(std::is_trivially_copyable_v<TValue>);

    } // namespace

    Y_UNIT_TEST_SUITE(ServiceMap) {

        Y_UNIT_TEST(FindWhileServiceMovesBehindReader) {
            CheckReplacementDuringFind(1);
        }

        Y_UNIT_TEST(FindWhileServiceMovesRepeatedly) {
            CheckReplacementDuringFind(9);
        }

        Y_UNIT_TEST(FindWhileOldSlotIsReused) {
            CheckReplacementDuringFind(1, true);
        }

        Y_UNIT_TEST(FindWithoutReplacement) {
            CheckReplacementDuringFind(0);
        }

        Y_UNIT_TEST(FindDoesNotWaitForPausedWriter) {
            TServiceMap<TKey, TValue, TSameHash, 1, 4, 1> map;
            const TKey service{1};
            map.Update(service, TValue{11});
            const TValue replacement{22};
            TPause writerPause;
            TValue previous;
            std::thread writer([&] {
                ValueLoadPause = &writerPause;
                previous = map.Update(service, replacement);
                ValueLoadPause = nullptr;
            });

            const bool writerReached = writerPause.WaitUntilReached();
            std::binary_semaphore readerFinished{0};
            ui64 found = 0;
            ui64 missing = 1;
            std::thread reader([&] {
                found = map.Find(service).Id;
                missing = map.Find(TKey{3}).Id;
                readerFinished.release();
            });
            const bool readWhilePaused = readerFinished.try_acquire_for(std::chrono::seconds(10));
            writerPause.Resume.release();
            writer.join();
            reader.join();

            UNIT_ASSERT_C(writerReached, "Writer did not reach the publication boundary");
            UNIT_ASSERT_C(readWhilePaused, "Lookups waited for the paused writer");
            UNIT_ASSERT(found == 11 || found == 22);
            UNIT_ASSERT_VALUES_EQUAL(missing, 0);
            UNIT_ASSERT_VALUES_EQUAL(previous.Id, 11);
            UNIT_ASSERT_VALUES_EQUAL(map.Find(service).Id, 22);
        }

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
            std::latch firstReads(readerCount);
            std::vector<std::thread> readers;
            readers.reserve(readerCount);

            for (size_t i = 0; i != readerCount; ++i) {
                readers.emplace_back([&] {
                    start.arrive_and_wait();
                    bool first = true;
                    while (!stop.load(std::memory_order_relaxed)) {
                        const TActorId actorId = map.Find(serviceId);
                        if (actorId != actorId1 && actorId != actorId2) {
                            invalidReads.fetch_add(1, std::memory_order_relaxed);
                        }
                        if (first) {
                            first = false;
                            firstReads.count_down();
                        }
                    }
                });
            }

            start.arrive_and_wait();
            firstReads.wait();
            for (size_t i = 0; i != updateCount; ++i) {
                map.Update(serviceId, i % 2 ? actorId1 : actorId2);
            }
            stop.store(true, std::memory_order_relaxed);

            for (std::thread& reader : readers) {
                reader.join();
            }

            UNIT_ASSERT_VALUES_EQUAL(invalidReads.load(), 0);
        }
    } // Y_UNIT_TEST_SUITE(ServiceMap)

} // namespace NActors
