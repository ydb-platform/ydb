#include "test_support.h"

using namespace NGenerationTest;
Y_UNIT_TEST_SUITE(TCMallocGeneration) {
    Y_UNIT_TEST(GeometryAndUsableCapacity) {
        Child([] {
            auto* a = CreateAllocator(64 * 1024 * 1024);
            for (size_t align : {8, 16, 64, 256, 4096}) {
                for (size_t size : {0, 1, 7, 8, 15, 16, 63, 64, 127, 128, 129, 4096, 32769}) {
                    auto r = a->Allocate(size, align, 0);
                    UNIT_ASSERT(r.ptr);
                    UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(r.ptr) % align, 0);
                    UNIT_ASSERT(r.capacity >= size);
                    memset(r.ptr, 42, r.capacity);
                    UNIT_ASSERT_VALUES_EQUAL(a->Size(r.ptr), r.capacity);
                    a->Free(r.ptr);
                }
            }
            UNIT_ASSERT(!a->Allocate(SIZE_MAX, 8, 0).ptr);
            UNIT_ASSERT(!a->Allocate(16, 3, 0).ptr);
            UNIT_ASSERT(!a->Allocate(16, size_t{1} << 63, 0).ptr);
            UNIT_ASSERT_VALUES_EQUAL(a->GetStats().live, 0);
        });
    }
    Y_UNIT_TEST(GenerationSequenceAndDeadline) {
        Child([] {
            auto* a = CreateAllocator();
            void* pointers[64];
            size_t count = 0;
            do {
                UNIT_ASSERT(count < 64);
                auto r = a->Allocate(count % 2 ? 120 : 32, 8, 0);
                pointers[count] = r.ptr;
                if (count) UNIT_ASSERT_VALUES_EQUAL(
                    reinterpret_cast<uintptr_t>(r.ptr), reinterpret_cast<uintptr_t>(pointers[count - 1]) + 8);
                a->Free(r.ptr);
                ++count;
            } while (!a->GetStats().quarantined);
            UNIT_ASSERT(count >= 2);
            FakeNow = 99;
            auto next = a->Allocate(32, 8, 0);
            for (size_t i = 0; i < count; ++i) UNIT_ASSERT(next.ptr != pointers[i]);
            FakeNow = 100;
            auto wrapped = a->Allocate(32, 8, 0);
            UNIT_ASSERT_VALUES_EQUAL(wrapped.ptr, pointers[0]);
            // The oldest pointer is numerically indistinguishable after wrap.
            a->Free(wrapped.ptr);
            a->Free(next.ptr);
        });
    }
    Y_UNIT_TEST(BudgetDoesNotShortenQuarantine) {
        Child([] {
            auto* a = CreateAllocator(512 * 1024); // One 256 KiB region plus registry metadata.
            size_t allocations = 0;
            for (;;) {
                auto r = a->Allocate(32, 8, 0);
                if (!r.ptr) break;
                a->Free(r.ptr);
                UNIT_ASSERT(++allocations < 100000);
            }
            UNIT_ASSERT(a->GetStats().quarantined > 0);
            UNIT_ASSERT(a->GetStats().reserved <= 512 * 1024);
            FakeNow = 99;
            UNIT_ASSERT(!a->Allocate(32, 8, 0).ptr);
            FakeNow = 100;
            UNIT_ASSERT(a->Allocate(32, 8, 0).ptr);
        });
    }
    Y_UNIT_TEST(DoubleFree) {
        Child([] {
            void* p = NonFinalGlobalSlot();
            TCMallocInternalFree(p);
            TCMallocInternalFree(p);
        }, "DOUBLE_FREE");
    }
    Y_UNIT_TEST(StaleGeneration) {
        Child([] {
            auto* a = CreateAllocator();
            void* old = a->Allocate(32, 8, 0).ptr;
            a->Free(old);
            UNIT_ASSERT(a->Allocate(120, 8, 0).ptr != old);
            a->Free(old);
        }, "STALE_GENERATION_FREE");
    }
    Y_UNIT_TEST(QuarantinedFree) {
        Child([] { auto* a = CreateAllocator(); a->Free(Exhaust(a)); }, "FREE_DURING_QUARANTINE");
    }
    Y_UNIT_TEST(SizedDoubleFree) {
        Child([] {
            void* p = NonFinalGlobalSlot();
            TCMallocInternalFreeSized(p, 32);
            TCMallocInternalFreeSized(p, 32);
        }, "DOUBLE_FREE");
    }
    Y_UNIT_TEST(InvalidPointer) {
        Child([] { int local; CreateAllocator()->Free(&local); }, "INVALID_SLOT_POINTER");
    }
    Y_UNIT_TEST(InteriorPointer) {
        Child([] { auto* a = CreateAllocator(); a->Free(static_cast<char*>(a->Allocate(32, 8, 0).ptr) + 1); },
              "STALE_GENERATION_FREE");
    }
    Y_UNIT_TEST(CorruptHeader) {
        Child([] {
            auto* a = CreateAllocator();
            void* p = a->Allocate(32, 8, 0).ptr;
            auto* h = reinterpret_cast<volatile unsigned char*>(reinterpret_cast<uintptr_t>(p) & ~uintptr_t{255});
            h[0] = h[0] ^ 1;
            a->Free(p);
        }, "CORRUPTED_HEADER");
    }
    Y_UNIT_TEST(WriteAfterFreeHead) {
        Child([] {
            auto* a = CreateAllocator(4 * 1024 * 1024, true);
            auto* p = static_cast<volatile unsigned char*>(Exhaust(a));
            p[0] = p[0] ^ 1;
            FakeNow = 100;
            a->Allocate(32, 8, 0);
        }, "WRITE_AFTER_FREE");
    }
    Y_UNIT_TEST(WriteAfterFreeTail) {
        Child([] {
            auto* a = CreateAllocator(4 * 1024 * 1024, true);
            auto* p = static_cast<volatile unsigned char*>(Exhaust(a));
            p[31] = p[31] ^ 1;
            FakeNow = 100;
            a->Allocate(32, 8, 0);
        }, "WRITE_AFTER_FREE");
    }
    Y_UNIT_TEST(ApiAndReallocOOM) {
        Child([] {
            void* p = TCMallocInternalCalloc(7, 13);
            UNIT_ASSERT(p);
            for (size_t i = 0; i < 91; ++i) UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(p)[i], 0);
            const size_t capacity = *tcmalloc::MallocExtension::GetAllocatedSize(p);
            UNIT_ASSERT(capacity >= 91);
            memset(p, 42, capacity);
            void* q = TCMallocInternalRealloc(p, 200);
            UNIT_ASSERT(q && q != p);
            for (size_t i = 0; i < 91; ++i) UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(q)[i], 42);
            errno = 0;
            UNIT_ASSERT(!TCMallocInternalRealloc(q, SIZE_MAX));
            UNIT_ASSERT_VALUES_EQUAL(errno, ENOMEM);
            UNIT_ASSERT_VALUES_EQUAL(static_cast<char*>(q)[0], 42);
            TCMallocInternalFree(q);
            UNIT_ASSERT(!TCMallocInternalCalloc(SIZE_MAX, 2));
            void* aligned = TCMallocInternalMemalign(4096, 32);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(aligned) % 4096, 0);
            TCMallocInternalFree(aligned);
            auto* cpp = new (std::align_val_t(256)) char[200];
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(cpp) % 256, 0);
            ::operator delete[](cpp, std::align_val_t(256));
            auto sized = __size_returning_new_aligned(64, std::align_val_t(256));
            UNIT_ASSERT(sized.n >= 64);
            UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(sized.p) % 256, 0);
            memset(sized.p, 42, sized.n);
            ::operator delete(sized.p, std::align_val_t(256));
            void* estimated = TCMallocInternalMalloc(32);
            UNIT_ASSERT_VALUES_EQUAL(nallocx(32, 0),
                *tcmalloc::MallocExtension::GetAllocatedSize(estimated));
            TCMallocInternalFree(estimated);
            TCMallocInternalFree(nullptr);
        });
    }
    Y_UNIT_TEST(LargeAllocationAndAlignment) {
        Child([] {
            for (size_t alignment : {size_t{16}, size_t{4} << 20}) {
                void* p = TCMallocInternalMemalign(alignment, 5 * 1024 * 1024);
                UNIT_ASSERT(p);
                UNIT_ASSERT_VALUES_EQUAL(reinterpret_cast<uintptr_t>(p) % alignment, 0);
                const size_t capacity = *tcmalloc::MallocExtension::GetAllocatedSize(p);
                UNIT_ASSERT(capacity >= 5 * 1024 * 1024);
                memset(p, 42, capacity);
                TCMallocInternalFree(p);
            }
        });
    }
    Y_UNIT_TEST(ManyGenerationCycles) {
        Child([] {
            auto* a = CreateAllocator();
            for (size_t cycle = 0; cycle < 100; ++cycle) {
                Exhaust(a);
                FakeNow += 100;
                void* p = a->Allocate(32, 8, 0).ptr;
                UNIT_ASSERT(p);
                a->Free(p);
            }
        });
    }
    Y_UNIT_TEST(CrossThreadFree) {
        Child([] {
            void* pointers[512];
            for (void*& p : pointers) {
                p = TCMallocInternalMalloc(32);
                UNIT_ASSERT(p);
                memset(p, 42, 32);
            }
            std::thread consumer([&] {
                for (void* p : pointers) {
                    for (size_t i = 0; i < 32; ++i)
                        UNIT_ASSERT_VALUES_EQUAL(static_cast<unsigned char*>(p)[i], 42);
                    TCMallocInternalFree(p);
                }
            });
            consumer.join();
        });
    }
    Y_UNIT_TEST(ConcurrentDoubleFree) {
        Child([] {
            auto* a = CreateAllocator();
            void* p = a->Allocate(32, 8, 0).ptr;
            std::atomic<int> ready{0};
            auto action = [&] {
                ready.fetch_add(1);
                while (ready.load() != 2) {}
                a->Free(p);
            };
            std::thread t1(action), t2(action);
            t1.join(); t2.join();
        }, "DOUBLE_FREE");
    }
    Y_UNIT_TEST(ConcurrentChurnAndFork) {
        Child([] {
            std::vector<std::thread> threads;
            for (int t = 0; t < 8; ++t) {
                threads.emplace_back([] {
                    for (size_t i = 0; i < 10000; ++i) {
                        const size_t size = 1 + i % 4096;
                        void* p = TCMallocInternalMalloc(size);
                        UNIT_ASSERT(p);
                        memset(p, 42, size);
                        TCMallocInternalFree(p);
                    }
                });
            }
            const pid_t pid = fork();
            if (pid == 0) {
                for (int i = 0; i < 1000; ++i) TCMallocInternalFree(TCMallocInternalMalloc(128));
                _exit(0);
            }
            for (auto& thread : threads) thread.join();
            UNIT_ASSERT(pid > 0);
            int status = 0;
            UNIT_ASSERT_VALUES_EQUAL(waitpid(pid, &status, 0), pid);
            UNIT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
        });
    }
}
