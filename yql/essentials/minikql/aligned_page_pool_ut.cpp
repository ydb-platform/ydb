#include "aligned_page_pool.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/info.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

namespace NKikimr::NMiniKQL {

namespace {
class TScopedMemoryMapper {
public:
    static constexpr size_t EXTRA_SPACE_FOR_UNALIGNMENT = 1;

    struct TUnmapEntry {
        void* Addr;
        size_t Size;
        bool operator==(const TUnmapEntry& rhs) {
            return std::tie(Addr, Size) == std::tie(rhs.Addr, rhs.Size);
        }
    };

    explicit TScopedMemoryMapper(bool aligned) {
        Aligned_ = aligned;
        TFakeMmap::OnMunmap = [this](void* addr, size_t s) {
            Munmaps_.push_back({addr, s});
        };

        TFakeMmap::OnMmap = [this](size_t size) -> void* {
            // Allocate more memory to ensure we have enough space for alignment
            Storage_ = THolder<char, TDeleteArray>(new char[AlignUp(size + EXTRA_SPACE_FOR_UNALIGNMENT, TAlignedPagePool::POOL_PAGE_SIZE)]);
            UNIT_ASSERT(Storage_.Get());

            // Force TFakeMmap::Munmap to be called by returning a pointer that will always need adjustment
            if (Aligned_) {
                return PointerToAlignedMemory();
            } else {
                // Ensure the pointer is always unaligned by a fixed amount
                void* ptr = PointerToAlignedMemory();
                // Add EXTRA_SPACE_FOR_UNALIGNMENT to ensure it's unaligned
                return static_cast<void*>(static_cast<char*>(ptr) + EXTRA_SPACE_FOR_UNALIGNMENT);
            }
        };
    }

    ~TScopedMemoryMapper() {
        TFakeMmap::OnMunmap = {};
        TFakeMmap::OnMmap = {};
        Storage_.Reset();
    }

    void* PointerToAlignedMemory() {
        return AlignUp(Storage_.Get(), TAlignedPagePool::POOL_PAGE_SIZE);
    }

    size_t MunmapsSize() {
        return Munmaps_.size();
    }

    TUnmapEntry Munmaps(size_t i) {
        return Munmaps_[i];
    }

private:
    THolder<char, TDeleteArray> Storage_;
    std::vector<TUnmapEntry> Munmaps_;
    bool Aligned_;
};

}; // namespace

Y_UNIT_TEST_SUITE(TAlignedPagePoolTest) {

Y_UNIT_TEST(AlignedMmapPageSize) {
    TAlignedPagePoolImpl<TFakeMmap>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeMmap> alloc(__LOCATION__);
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    UNIT_ASSERT_EQUAL(0u, mmapper.MunmapsSize());

    UNIT_ASSERT_VALUES_EQUAL(block.get(), mmapper.PointerToAlignedMemory());

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), TAlignedPagePool::POOL_PAGE_SIZE + TAlignedPagePool::ALLOC_AHEAD_PAGES * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(UnalignedMmapPageSize) {
    TAlignedPagePoolImpl<TFakeMmap>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeMmap> alloc(__LOCATION__);
    TScopedMemoryMapper mmapper(/*aligned=*/false);

    auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    UNIT_ASSERT_EQUAL(2, mmapper.MunmapsSize());
    UNIT_ASSERT_EQUAL(TAlignedPagePool::POOL_PAGE_SIZE - TScopedMemoryMapper::EXTRA_SPACE_FOR_UNALIGNMENT, mmapper.Munmaps(0).Size);
    UNIT_ASSERT_EQUAL(TScopedMemoryMapper::EXTRA_SPACE_FOR_UNALIGNMENT, mmapper.Munmaps(1).Size);

    UNIT_ASSERT_VALUES_EQUAL(block.get(), (char*)mmapper.PointerToAlignedMemory() + TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES - 1);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), TAlignedPagePool::POOL_PAGE_SIZE + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 1) * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(AlignedMmapUnalignedSize) {
    TAlignedPagePoolImpl<TFakeMmap>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeMmap> alloc(__LOCATION__);
    auto smallSize = NSystemInfo::GetPageSize();
    auto size = smallSize + 1024 * TAlignedPagePool::POOL_PAGE_SIZE;
    TScopedMemoryMapper mmapper(/*aligned=*/true);

    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });

    UNIT_ASSERT_EQUAL(2, mmapper.MunmapsSize());
    auto expected0 = (TScopedMemoryMapper::TUnmapEntry{(char*)mmapper.PointerToAlignedMemory() + size, TAlignedPagePool::POOL_PAGE_SIZE - smallSize});
    UNIT_ASSERT_EQUAL(expected0, mmapper.Munmaps(0));
    auto expected1 = TScopedMemoryMapper::TUnmapEntry{
        (char*)mmapper.PointerToAlignedMemory() + TAlignedPagePool::ALLOC_AHEAD_PAGES * TAlignedPagePool::POOL_PAGE_SIZE + size - smallSize,
        smallSize};
    UNIT_ASSERT_EQUAL(expected1, mmapper.Munmaps(1));

    UNIT_ASSERT_VALUES_EQUAL(block.get(), mmapper.PointerToAlignedMemory());

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES - 1);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), size + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 1) * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(UnalignedMmapUnalignedSize) {
    TAlignedPagePoolImpl<TFakeMmap>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TFakeMmap> alloc(__LOCATION__);
    auto smallSize = NSystemInfo::GetPageSize();
    auto size = smallSize + 1024 * TAlignedPagePool::POOL_PAGE_SIZE;
    TScopedMemoryMapper mmapper(/*aligned=*/false);
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    UNIT_ASSERT_EQUAL(3, mmapper.MunmapsSize());
    UNIT_ASSERT_EQUAL(TAlignedPagePool::POOL_PAGE_SIZE - TScopedMemoryMapper::EXTRA_SPACE_FOR_UNALIGNMENT, mmapper.Munmaps(0).Size);
    UNIT_ASSERT_EQUAL(TAlignedPagePool::POOL_PAGE_SIZE - smallSize, mmapper.Munmaps(1).Size);
    UNIT_ASSERT_EQUAL(smallSize + 1, mmapper.Munmaps(2).Size);

    UNIT_ASSERT_VALUES_EQUAL(block.get(), (char*)mmapper.PointerToAlignedMemory() + TAlignedPagePool::POOL_PAGE_SIZE);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES - 2);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), size + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 2) * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(YellowZoneSwitchesCorrectlyBlock) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl alloc(__LOCATION__);

    // choose relatively big chunk so ALLOC_AHEAD_PAGES don't affect the correctness of the test
    auto size = 1024 * TAlignedPagePool::POOL_PAGE_SIZE;

    alloc.SetLimit(size * 10);

    // 50% allocated -> no yellow zone
    auto block1 = alloc.GetBlock(size * 5);
    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());

    // 70% allocated -> no yellow zone
    auto block2 = alloc.GetBlock(size * 2);
    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());

    // 90% allocated -> yellow zone is enabled (> 80%)
    auto block3 = alloc.GetBlock(size * 2);
    UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsMemoryYellowZoneEnabled());

    // 70% allocated -> yellow zone is still enabled (> 50%)
    alloc.ReturnBlock(block3, size * 2);
    UNIT_ASSERT_VALUES_EQUAL(true, alloc.IsMemoryYellowZoneEnabled());

    // 50% allocated -> yellow zone is disabled
    alloc.ReturnBlock(block2, size * 2);
    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());

    // 0% allocated -> yellow zone is disabled
    alloc.ReturnBlock(block1, size * 5);
    UNIT_ASSERT_VALUES_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());
}

Y_UNIT_TEST(YellowZoneZeroDivision) {
    TAlignedPagePool::ResetGlobalsUT();
    TAlignedPagePoolImpl alloc(__LOCATION__);

    alloc.SetLimit(0);

    UNIT_ASSERT_EQUAL(false, alloc.IsMemoryYellowZoneEnabled());
}

} // Y_UNIT_TEST_SUITE(TAlignedPagePoolTest)

} // namespace NKikimr::NMiniKQL
