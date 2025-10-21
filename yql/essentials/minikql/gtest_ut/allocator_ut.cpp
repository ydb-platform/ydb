#include <yql/essentials/minikql/mkql_alloc.cpp>

#include <library/cpp/testing/gtest/gtest.h>

#include <cstdlib>

namespace NKikimr::NMiniKQL {

enum class EAllocatorType {
    DefaultAllocator,
    ArrowAllocator,
    HugeAllocator,
};

class MemoryTest: public ::testing::TestWithParam<std::tuple<int, EAllocatorType>> {
protected:
    MemoryTest()
        : ScopedAlloc_(__LOCATION__) {
    }

    size_t AllocSize() const {
        return static_cast<size_t>(std::get<0>(GetParam()));
    }

    EAllocatorType GetAllocatorType() const {
        return std::get<1>(GetParam());
    }

    void* AllocateMemory(size_t size) const {
        EAllocatorType allocatorType = GetAllocatorType();
        switch (allocatorType) {
            case EAllocatorType::DefaultAllocator:
                return TWithDefaultMiniKQLAlloc::AllocWithSize(size);
            case EAllocatorType::ArrowAllocator:
                return MKQLArrowAllocate(size);
            case EAllocatorType::HugeAllocator:
                return TMKQLHugeAllocator<char>::allocate(size);
            default:
                return nullptr; // Should never reach here
        }
    }

    void Free(const void* mem, size_t size) const {
        EAllocatorType allocatorType = GetAllocatorType();
        switch (allocatorType) {
            case EAllocatorType::DefaultAllocator:
                TWithDefaultMiniKQLAlloc::FreeWithSize(mem, size);
                break;
            case EAllocatorType::ArrowAllocator:
                MKQLArrowFree(mem, size);
                break;
            case EAllocatorType::HugeAllocator:
                TMKQLHugeAllocator<char>::deallocate(const_cast<char*>(static_cast<const char*>(mem)), size);
                break;
            default:
                break; // Should never reach here
        }
    }

    void AccessMemory(volatile void* memory, ssize_t offset) const {
        volatile char* ptr = static_cast<volatile char*>(memory) + offset;
        *ptr = 'A'; // Perform a basic write operation
    }

private:
    TScopedAlloc ScopedAlloc_;
};

// Test naming function
std::string TestNameGenerator(const ::testing::TestParamInfo<MemoryTest::ParamType>& info) {
    int sizeNumber = std::get<0>(info.param);
    EAllocatorType allocatorType = std::get<1>(info.param);


    std::string allocatorName = [&] () {
        switch (allocatorType) {
            case EAllocatorType::DefaultAllocator:
                return "DefaultAllocator";
            case EAllocatorType::ArrowAllocator:
                return "ArrowAllocator";
            case EAllocatorType::HugeAllocator:
                return "HugeAllocator";
        }
    }();

    return "Size" + std::to_string(sizeNumber) + "With" + allocatorName + "Allocator";
}

// Out of bounds access + use after free can be tested only with
// --sanitize=address.
#if defined(_asan_enabled_)
TEST_P(MemoryTest, AccessOutOfBounds) {
    size_t allocationSize = AllocSize();

    void* memory = AllocateMemory(allocationSize);
    ASSERT_NE(memory, nullptr) << "Memory allocation failed.";
    // Accessing valid memory.
    ASSERT_NO_THROW({
        AccessMemory(memory, 0);
        AccessMemory(memory, allocationSize - 1);
    });

    // Accessing invalid left memory.
    EXPECT_DEATH({ AccessMemory(memory, -1); }, "");
    EXPECT_DEATH({ AccessMemory(memory, -8); }, "");
    EXPECT_DEATH({ AccessMemory(memory, -16); }, "");

    // Accessing invalid right memory.
    EXPECT_DEATH({ AccessMemory(memory, allocationSize); }, "");
    EXPECT_DEATH({ AccessMemory(memory, allocationSize + 6); }, "");
    EXPECT_DEATH({ AccessMemory(memory, allocationSize + 12); }, "");
    EXPECT_DEATH({ AccessMemory(memory, allocationSize + 15); }, "");

    Free(memory, allocationSize);
}

TEST_P(MemoryTest, AccessAfterFree) {
    size_t allocationSize = AllocSize();
    void* memory = AllocateMemory(allocationSize);
    void* memory2 = AllocateMemory(allocationSize);
    ASSERT_NE(memory, nullptr) << "Memory allocation failed.";
    Free(memory, allocationSize);

    // Access after free — should crash
    EXPECT_DEATH({ AccessMemory(memory, 0); }, "");
    EXPECT_DEATH({ AccessMemory(memory, allocationSize / 2); }, "");
    EXPECT_DEATH({ AccessMemory(memory, allocationSize - 1); }, "");

    Free(memory2, allocationSize);
    // Access after free — should crash
    EXPECT_DEATH({ AccessMemory(memory2, 0); }, "");
    EXPECT_DEATH({ AccessMemory(memory2, allocationSize / 2); }, "");
    EXPECT_DEATH({ AccessMemory(memory2, allocationSize - 1); }, "");
}

#endif // defined(_asan_enabled_)

// Double free tracked only in DEBUG mode.
#ifndef NDEBUG
TEST_P(MemoryTest, DoubleFree) {
    if (GetAllocatorType() == EAllocatorType::ArrowAllocator || GetAllocatorType() == EAllocatorType::HugeAllocator) {
        GTEST_SKIP() << "Arrow and Huge allocators arae not instrumented yet to track double free.";
    }
    size_t allocationSize = AllocSize();

    void* memory = AllocateMemory(allocationSize);
    ASSERT_NE(memory, nullptr) << "Memory allocation failed.";

    Free(memory, allocationSize);

    // Attempting double free — should crash
    EXPECT_DEATH({ Free(memory, allocationSize); }, "");
}
#endif // NDEBUG

// Allow empty tests for MSAN and other sanitizers.
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MemoryTest);

INSTANTIATE_TEST_SUITE_P(MemoryTests, MemoryTest,
                         ::testing::Combine(
                             ::testing::Values(8, 64, 32 * 1024, 64 * 1024, 128 * 1024, 64 * 1024 * 1024),
                             ::testing::Values(
                                 EAllocatorType::DefaultAllocator,
                                 EAllocatorType::ArrowAllocator,
                                 EAllocatorType::HugeAllocator)),
                         TestNameGenerator);

} // namespace NKikimr::NMiniKQL
