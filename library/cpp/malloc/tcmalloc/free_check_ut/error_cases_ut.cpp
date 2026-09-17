#include "test_support.h"

extern "C" void TCMallocInternalCfree(void*) noexcept;
extern "C" void TCMallocInternalFreeAlignedSized(void*, size_t, size_t);
extern "C" void TCMallocInternalSdallocx(void*, size_t, int) noexcept;
extern "C" void TCMallocInternalDelete(void*) noexcept;
extern "C" void TCMallocInternalDeleteSized(void*, size_t) noexcept;
extern "C" void TCMallocInternalDeleteAligned(void*, std::align_val_t) noexcept;
extern "C" void TCMallocInternalDeleteSizedAligned(void*, size_t, std::align_val_t) noexcept;
extern "C" void TCMallocInternalDeleteArray(void*) noexcept;
extern "C" void TCMallocInternalDeleteArraySized(void*, size_t) noexcept;
extern "C" void TCMallocInternalDeleteArrayAligned(void*, std::align_val_t) noexcept;
extern "C" void TCMallocInternalDeleteArraySizedAligned(void*, size_t, std::align_val_t) noexcept;
extern "C" void TCMallocInternalDeleteNothrow(void*, const std::nothrow_t&) noexcept;
extern "C" void TCMallocInternalDeleteAlignedNothrow(void*, std::align_val_t, const std::nothrow_t&) noexcept;
extern "C" void TCMallocInternalDeleteArrayNothrow(void*, const std::nothrow_t&) noexcept;
extern "C" void TCMallocInternalDeleteArrayAlignedNothrow(void*, std::align_val_t, const std::nothrow_t&) noexcept;

using namespace NGenerationTest;
namespace {
using Deallocator = void (*)(void*);
const Deallocator Deallocators[] = {
    TCMallocInternalFree, TCMallocInternalCfree,
    [](void* p) { TCMallocInternalFreeSized(p, 32); },
    [](void* p) { TCMallocInternalFreeAlignedSized(p, 16, 32); },
    [](void* p) { TCMallocInternalSdallocx(p, 32, 0); },
    [](void* p) { TCMallocInternalSdallocx(p, 32, 4); },
    TCMallocInternalDelete, TCMallocInternalDeleteArray,
    [](void* p) { TCMallocInternalDeleteSized(p, 32); },
    [](void* p) { TCMallocInternalDeleteArraySized(p, 32); },
    [](void* p) { TCMallocInternalDeleteAligned(p, std::align_val_t(16)); },
    [](void* p) { TCMallocInternalDeleteArrayAligned(p, std::align_val_t(16)); },
    [](void* p) { TCMallocInternalDeleteSizedAligned(p, 32, std::align_val_t(16)); },
    [](void* p) { TCMallocInternalDeleteArraySizedAligned(p, 32, std::align_val_t(16)); },
    [](void* p) { TCMallocInternalDeleteNothrow(p, std::nothrow); },
    [](void* p) { TCMallocInternalDeleteArrayNothrow(p, std::nothrow); },
    [](void* p) { TCMallocInternalDeleteAlignedNothrow(p, std::align_val_t(16), std::nothrow); },
    [](void* p) { TCMallocInternalDeleteArrayAlignedNothrow(p, std::align_val_t(16), std::nothrow); },
};
}

Y_UNIT_TEST_SUITE(TCMallocGenerationErrors) {
    Y_UNIT_TEST(DoubleFreeThroughEveryDeallocator) {
        for (auto release : Deallocators) Child([=] {
            void* p = NonFinalGlobalSlot();
            ExpectAllocated(p, static_cast<char*>(p) + 16, 32, 16, 0);
            release(p);
            release(p);
        }, "DOUBLE_FREE");
    }
    Y_UNIT_TEST(StalePointerThroughEveryDeallocator) {
        for (auto release : Deallocators) Child([=] {
            void* old = NonFinalGlobalSlot();
            release(old);
            void* current = TCMallocInternalMalloc(32);
            UNIT_ASSERT(Address(current) == Address(old) + 16);
            ExpectAllocated(old, current, 32, 16);
            release(old);
        }, "STALE_GENERATION_FREE");
    }
    Y_UNIT_TEST(EveryEarlierGenerationBeforeWrap) {
        for (size_t distance = 1; distance <= 10; ++distance) Child([=] {
            auto* a = CreateAllocator();
            void* old = a->Allocate(32, 8, 0).ptr;
            void* current = old;
            for (size_t i = 0; i < distance; ++i) {
                a->Free(current);
                current = a->Allocate(120, 8, 0).ptr;
            }
            UNIT_ASSERT_VALUES_EQUAL(Address(current), Address(old) + distance * 8);
            ExpectAllocated(old, current, 120, 8);
            a->Free(old);
        }, "STALE_GENERATION_FREE");
    }
    Y_UNIT_TEST(StalePointerAcrossSlotSizesAndAlignments) {
        for (size_t alignment : {8, 64, 4096}) {
            for (size_t size : {129, 4096, 2097153}) Child([=] {
                auto* a = CreateAllocator(32 * 1024 * 1024);
                void* old = a->Allocate(size, alignment, 0).ptr;
                UNIT_ASSERT(old);
                a->Free(old);
                void* current = a->Allocate(size, alignment, 0).ptr;
                UNIT_ASSERT_VALUES_EQUAL(Address(current), Address(old) + alignment);
                ExpectAllocated(old, current, size, alignment);
                a->Free(old);
            }, "STALE_GENERATION_FREE");
        }
    }
    Y_UNIT_TEST(QuarantinedPointersUntilDrainedIncludingZeroDelay) {
        for (uint64_t delay : {0, 100}) for (uint64_t now : {0, 99, 100, 101}) {
            for (bool oldest : {false, true}) Child([=] {
                auto* a = CreateAllocator(4 * 1024 * 1024, false, delay);
                void* first = a->Allocate(32, 8, 0).ptr;
                a->Free(first);
                void* last = Exhaust(a);
                void* invalid = oldest ? first : last;
                FakeNow = now;
                ExpectAllocated(invalid, last, 32, 8, 2);
                a->Free(invalid);
            }, "FREE_DURING_QUARANTINE");
        }
    }
    Y_UNIT_TEST(ForeignPointersFreeAndSize) {
        for (bool sizeQuery : {false, true}) for (int kind = 0; kind < 4; ++kind) Child([=] {
            auto* a = CreateAllocator();
            int stackValue;
            static int staticValue;
            void* p = kind == 0 ? &stackValue : kind == 1 ? &staticValue : kind == 2 ?
                reinterpret_cast<void*>(uintptr_t{1}) :
                mmap(nullptr, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            UNIT_ASSERT(p != MAP_FAILED);
            ExpectFields(p, 0, 0, 0, 0, 0, 0);
            if (sizeQuery) a->Size(p); else a->Free(p);
        }, "INVALID_SLOT_POINTER");
    }
    Y_UNIT_TEST(HeaderInteriorAndPaddingPointers) {
        for (bool sizeQuery : {false, true}) for (size_t offset : {0, 47, 49, 80, 176, 255}) Child([=] {
            auto* a = CreateAllocator();
            void* p = a->Allocate(32, 8, 0).ptr;
            const uintptr_t base = SlotBase(p, 32, 8);
            UNIT_ASSERT_VALUES_EQUAL(Address(p) - base, 48);
            void* invalid = reinterpret_cast<void*>(base + offset);
            ExpectAllocated(invalid, p, 32, 8);
            if (sizeQuery) a->Size(invalid); else a->Free(invalid);
        }, "STALE_GENERATION_FREE");
    }
    Y_UNIT_TEST(EveryHeaderByteInAllocatedFreeAndQuarantinedSlots) {
        // This allocator targets 64-bit platforms; the first 48 bytes are metadata.
        for (int operation = 0; operation < 4; ++operation) {
            for (size_t byte = 0; byte < 48; ++byte) Child([=] {
                auto* a = CreateAllocator();
                void* p = a->Allocate(32, 8, 0).ptr;
                const uintptr_t base = SlotBase(p, 32, 8);
                UNIT_ASSERT_VALUES_EQUAL(Address(p) - base, 48);
                if (operation == 2) a->Free(p);
                if (operation == 3) { a->Free(p); Exhaust(a); FakeNow = 100; }
                CorruptByte(reinterpret_cast<void*>(base), byte);
                ExpectFields(reinterpret_cast<void*>(base), 0, 0, 0, 0, 0, 0);
                if (operation == 0) a->Free(p);
                else if (operation == 1) a->Size(p);
                else a->Allocate(32, 8, 0);
            }, "CORRUPTED_HEADER");
        }
    }
    Y_UNIT_TEST(CorruptedQuarantineTailOnAppend) {
        Child([] {
            auto* a = CreateAllocator();
            Exhaust(a);
            void* tail = Exhaust(a);
            const uintptr_t base = SlotBase(tail, 32, 8);
            CorruptByte(reinterpret_cast<void*>(base), 0);
            ExpectFields(reinterpret_cast<void*>(base), 0, 0, 0, 0, 0, 0);
            Exhaust(a);
        }, "CORRUPTED_HEADER");
    }
    Y_UNIT_TEST(PoisonHeadTailAndOverlapBoundaries) {
        for (uint64_t delay : {0, 100}) for (size_t size : {1, 64, 65, 128, 129, 256}) {
            const size_t edge = size < 64 ? size - 1 : 63;
            for (size_t offset : {size_t{0}, edge, size - 1 - edge, size - 1}) Child([=] {
                auto* a = CreateAllocator(4 * 1024 * 1024, true, delay);
                void* p = Exhaust(a, size);
                CorruptByte(p, offset);
                FakeNow = delay;
                ExpectAllocated(static_cast<char*>(p) + offset, p, size, 8, 2);
                a->Allocate(size, 8, 0);
            }, "WRITE_AFTER_FREE");
        }
    }
    Y_UNIT_TEST(SizeValidatesFreedStaleAndQuarantinedPointers) {
        for (int state = 0; state < 3; ++state) Child([=] {
            auto* a = CreateAllocator();
            void* p = a->Allocate(32, 8, 0).ptr;
            a->Free(p);
            if (state == 0) ExpectAllocated(p, static_cast<char*>(p) + 8, 32, 8, 0);
            if (state == 1) ExpectAllocated(p, a->Allocate(120, 8, 0).ptr, 120, 8);
            if (state == 2) ExpectAllocated(p, Exhaust(a), 32, 8, 2);
            a->Size(p);
        }, state == 0 ? "DOUBLE_FREE" : state == 1 ? "STALE_GENERATION_FREE" : "FREE_DURING_QUARANTINE");
    }
    Y_UNIT_TEST(ReallocValidatesFreedAndStalePointers) {
        for (bool stale : {false, true}) Child([=] {
            void* p = NonFinalGlobalSlot();
            TCMallocInternalFree(p);
            void* current = stale ? TCMallocInternalMalloc(32) : static_cast<char*>(p) + 16;
            // Numeric equality macros stringify operands and can reuse this free slot.
            UNIT_ASSERT(Address(current) == Address(p) + 16);
            ExpectAllocated(p, current, 32, 16, stale ? 1 : 0);
            TCMallocInternalRealloc(p, 256);
        }, stale ? "STALE_GENERATION_FREE" : "DOUBLE_FREE");
    }
    Y_UNIT_TEST(ConcurrentDoubleFreeDiagnostics) {
        for (bool finalGeneration : {false, true}) Child([=] {
            auto* a = CreateAllocator();
            void* p = a->Allocate(32, 8, 0).ptr;
            if (finalGeneration) {
                while (Address(p) - SlotBase(p, 32, 8) < 128) {
                    a->Free(p);
                    p = a->Allocate(32, 8, 0).ptr;
                }
            }
            ExpectAllocated(p, finalGeneration ? p : static_cast<char*>(p) + 8,
                            32, 8, finalGeneration ? 2 : 0);
            std::atomic<int> ready{0};
            auto release = [&] {
                ready.fetch_add(1);
                while (ready.load() != 2) {}
                a->Free(p);
            };
            std::thread first(release), second(release);
            first.join(); second.join();
        }, finalGeneration ? "FREE_DURING_QUARANTINE" : "DOUBLE_FREE");
    }
    Y_UNIT_TEST(PoisonDetectionLimitsAndIntactPayload) {
        for (uint64_t delay : {0, 100}) for (int mode = 0; mode < 5; ++mode) Child([=] {
            auto* a = CreateAllocator(4 * 1024 * 1024, mode != 4, delay);
            void* p = Exhaust(a, 256);
            if (mode == 1) { volatile auto value = *static_cast<volatile unsigned char*>(p); (void)value; }
            if (mode == 2) CorruptByte(p, 128); // Unchecked middle.
            if (mode == 3) { CorruptByte(p, 0); CorruptByte(p, 0); } // Restored poison.
            if (mode == 4) CorruptByte(p, 0); // Poison disabled.
            FakeNow = delay;
            void* next = a->Allocate(256, 8, 0).ptr;
            UNIT_ASSERT(next);
            a->Free(next);
        });
    }
    Y_UNIT_TEST(ZeroDelayCannotDistinguishAddressAfterFullWrap) {
        Child([] {
            auto* a = CreateAllocator(4 * 1024 * 1024, true, 0);
            void* first = a->Allocate(32, 8, 0).ptr;
            a->Free(first);
            Exhaust(a);
            void* wrapped = a->Allocate(32, 8, 0).ptr;
            UNIT_ASSERT_VALUES_EQUAL(first, wrapped);
            a->Free(first);
        });
    }
}
