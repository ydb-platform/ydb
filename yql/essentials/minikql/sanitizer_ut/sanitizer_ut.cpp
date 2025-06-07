#include <yql/essentials/minikql/mkql_alloc.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <cstdlib>
#include <cstdio>

namespace NKikimr::NMiniKQL {

enum class EAllocatorType {
    DefaultAllocator,
    ArrowAllocator,
    HugeAllocator,
};

class MemoryTest: public testing::Test {
protected:
    MemoryTest()
        : ScopedAlloc_(__LOCATION__) {
    }

    void AccessMemory(volatile void* memory, ssize_t offset) const {
        volatile char* ptr = static_cast<volatile char*>(memory) + offset;
        *ptr = 'A'; // Perform a basic write operation
    }

    void BranchMemory(const volatile void* memory, ssize_t offset) const {
        const volatile char* ptr = static_cast<const volatile char*>(memory) + offset;
        if (*ptr == 'A') {
            Cerr << "Branch access" << Endl;
        }
    }

    TAlignedPagePool& GetPagePool() {
        return ScopedAlloc_.Ref();
    }

private:
    TScopedAlloc ScopedAlloc_;
};

class MemoryTestWithSizeAndAllocator: public MemoryTest, public ::testing::WithParamInterface<std::tuple<int, EAllocatorType>> {
protected:
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
};

// Test naming function
std::string TestNameGenerator(const ::testing::TestParamInfo<MemoryTestWithSizeAndAllocator::ParamType>& info) {
    int sizeNumber = std::get<0>(info.param);
    EAllocatorType allocatorType = std::get<1>(info.param);

    std::string allocatorName = [&]() {
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
TEST_P(MemoryTestWithSizeAndAllocator, AccessOutOfBounds) {
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

TEST_P(MemoryTestWithSizeAndAllocator, AccessAfterFree) {
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

TEST_F(MemoryTest, TestPageFromPagePool) {
    auto* page = GetPagePool().GetPage();
    // No access allowed.
    EXPECT_DEATH({ AccessMemory(page, 0); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE / 2); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE - 1); }, "");

    // Allow access.
    NYql::NUdf::SanitizerMakeRegionAccessible(page, TAlignedPagePool::POOL_PAGE_SIZE);

    // Access allowed.
    AccessMemory(page, 0);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE / 2);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE - 1);

    // Return page should disable access.
    GetPagePool().ReturnPage(page);

    // Page should be unaddressable after being returned.
    EXPECT_DEATH({ AccessMemory(page, 0); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE / 2); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE - 1); }, "");
}

TEST_F(MemoryTest, TestBlockFromPagePool) {
    auto* page = GetPagePool().GetBlock(TAlignedPagePool::POOL_PAGE_SIZE * 2);
    // No access allowed.
    EXPECT_DEATH({ AccessMemory(page, 0); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1); }, "");

    // Allow access.
    NYql::NUdf::SanitizerMakeRegionAccessible(page, TAlignedPagePool::POOL_PAGE_SIZE * 2);

    // Access allowed.
    AccessMemory(page, 0);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1);
    // Return page.
    GetPagePool().ReturnBlock(page, TAlignedPagePool::POOL_PAGE_SIZE * 2);

    // Page should be unaddressable after being returned.
    EXPECT_DEATH({ AccessMemory(page, 0); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE); }, "");
    EXPECT_DEATH({ AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1); }, "");
}

#endif // defined(_asan_enabled_)

#if defined(_msan_enabled_)

TEST_P(MemoryTestWithSizeAndAllocator, UninitializedAccess) {
    size_t allocationSize = AllocSize();
    void* memory = AllocateMemory(allocationSize);

    // Check unitialized access.
    EXPECT_DEATH({ BranchMemory(memory, 0); }, "");
    EXPECT_DEATH({ BranchMemory(memory, AllocSize() - 1); }, "");

    // Initialize memory.
    memset(memory, 0, allocationSize);

    // Check initialized access.
    BranchMemory(memory, 0);
    BranchMemory(memory, AllocSize() - 1);

    Free(memory, allocationSize);
}

TEST_F(MemoryTest, TestPageFromPagePool) {
    auto* page = GetPagePool().GetPage();

    // No access allowed.
    EXPECT_DEATH({ BranchMemory(page, 0); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE / 2); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE - 1); }, "");

    // Open region.
    NYql::NUdf::SanitizerMakeRegionAccessible(page, TAlignedPagePool::POOL_PAGE_SIZE);

    // Still cannot access memory. Opening region does not make memory initialized.
    EXPECT_DEATH({ BranchMemory(page, 0); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1); }, "");

    // Allow access.
    memset(page, 0, TAlignedPagePool::POOL_PAGE_SIZE * 2);

    // Access should be valid.
    BranchMemory(page, 0);
    BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE);
    BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1);

    // Return page should disable access.
    GetPagePool().ReturnPage(page);
}

TEST_F(MemoryTest, TestScopedInterceptorDisable) {
    // Allocate memory but don't initialize it
    size_t allocationSize = 64;
    char* uninitializedSrc = static_cast<char*>(TWithDefaultMiniKQLAlloc::AllocWithSize(allocationSize));
    ASSERT_NE(uninitializedSrc, nullptr) << "Source memory allocation failed.";
    Y_DEFER {
        TWithDefaultMiniKQLAlloc::FreeWithSize(uninitializedSrc, allocationSize);
    };

    // Without disabling access check, using uninitialized memory should trigger MSAN.
    EXPECT_DEATH({
        BranchMemory(uninitializedSrc, 0);
    }, "");

    // With YQL_MSAN_FREEZE_AND_SCOPED_UNPOISON, the access check should be disabled.
    {
        YQL_MSAN_FREEZE_AND_SCOPED_UNPOISON(uninitializedSrc, allocationSize);

        // This should not trigger MSAN.
        BranchMemory(uninitializedSrc, 0);
    }

    // After the scope ends, access check should be re-enabled.
    EXPECT_DEATH({
        BranchMemory(uninitializedSrc, 0);
    }, "");
}

TEST_F(MemoryTest, TestBlockMsan) {
    auto* page = GetPagePool().GetBlock(TAlignedPagePool::POOL_PAGE_SIZE * 2);

    // No access allowed.
    EXPECT_DEATH({ BranchMemory(page, 0); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1); }, "");

    // Open region.
    NYql::NUdf::SanitizerMakeRegionAccessible(page, TAlignedPagePool::POOL_PAGE_SIZE);

    // Still cannot access memory.
    EXPECT_DEATH({ BranchMemory(page, 0); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE); }, "");
    EXPECT_DEATH({ BranchMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1); }, "");

    // Allow access via memory initialization.
    memset(page, 0, TAlignedPagePool::POOL_PAGE_SIZE * 2);

    // Access allowed.
    AccessMemory(page, 0);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2);
    AccessMemory(page, TAlignedPagePool::POOL_PAGE_SIZE * 2 - 1);
    GetPagePool().ReturnBlock(page, TAlignedPagePool::POOL_PAGE_SIZE * 2);
}

#endif // defined(_msan_enabled_)

// Double free tracked only in DEBUG mode.
#ifndef NDEBUG
TEST_P(MemoryTestWithSizeAndAllocator, DoubleFree) {
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
GTEST_ALLOW_UNINSTANTIATED_PARAMETERIZED_TEST(MemoryTestWithSizeAndAllocator);

INSTANTIATE_TEST_SUITE_P(MemoryTestWithSizeAndAllocators, MemoryTestWithSizeAndAllocator,
                         ::testing::Combine(::testing::Values(8, 64, 32 * 1024, 64 * 1024, 128 * 1024, 64 * 1024 * 1024),
                                            ::testing::Values(EAllocatorType::DefaultAllocator, EAllocatorType::ArrowAllocator,
                                                              EAllocatorType::HugeAllocator)),
                         TestNameGenerator);

} // namespace NKikimr::NMiniKQL
