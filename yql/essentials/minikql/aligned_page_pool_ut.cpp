#include "aligned_page_pool.h"
#include "fake_mmap.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/system/error.h>
#include <util/system/info.h>
#include <yql/essentials/utils/backtrace/backtrace.h>

#include <cerrno>
#include <expected>
#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

constexpr TStringBuf MapperErrorMessage = "Injected mapper error";

std::unexpected<TSystemError> MakeMapperError() {
    TSystemError error(EIO);
    error << MapperErrorMessage;
    ClearLastSystemError();
    return std::unexpected(std::move(error));
}

bool IsMapperError(const TSystemError& error) {
    return error.Status() == EIO && error.AsStrBuf().Contains(MapperErrorMessage);
}

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
        TFakeMmap::GetInstance().OnMunmap = [this](void* addr, size_t s) -> std::expected<void, TSystemError> {
            Munmaps_.push_back({addr, s});
            return {};
        };

        TFakeMmap::GetInstance().OnMmap = [this](size_t size) -> std::expected<void*, TSystemError> {
            Storage_ = THolder<char, TDeleteArray>(new char[AlignUp(size + EXTRA_SPACE_FOR_UNALIGNMENT, TAlignedPagePool::POOL_PAGE_SIZE)]);
            UNIT_ASSERT(Storage_.Get());

            if (Aligned_) {
                return PointerToAlignedMemory();
            } else {
                void* ptr = PointerToAlignedMemory();
                return static_cast<void*>(static_cast<char*>(ptr) + EXTRA_SPACE_FOR_UNALIGNMENT);
            }
        };
    }

    ~TScopedMemoryMapper() {
        TFakeMmap::GetInstance().OnMunmap = {};
        TFakeMmap::GetInstance().OnMmap = {};
        TFakeMmap::GetInstance().OnFreeze = {};
        TFakeMmap::GetInstance().OnUnfreeze = {};
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

Y_UNIT_TEST(AlignedMmapKeepsExtraPage) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mapper(/*aligned=*/true);
    Y_DEFER {
        TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::DoCleanupGlobalFreeList(0);
    };

    const auto releasePage = [](void* address) {
        ReleaseAlignedPage<TTrackedMmap<TFakeMmap>>(address);
    };
    const auto pageSize = TAlignedPagePool::POOL_PAGE_SIZE;
    auto firstPage = std::shared_ptr<void>(GetAlignedPage<TTrackedMmap<TFakeMmap>>(), releasePage);
    UNIT_ASSERT_VALUES_EQUAL(firstPage.get(), mapper.PointerToAlignedMemory());
    UNIT_ASSERT_VALUES_EQUAL(TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::GetGlobalPagePoolSize(), pageSize);

    auto secondPage = std::shared_ptr<void>(GetAlignedPage<TTrackedMmap<TFakeMmap>>(), releasePage);
    UNIT_ASSERT_VALUES_EQUAL(secondPage.get(), static_cast<char*>(firstPage.get()) + pageSize);
    UNIT_ASSERT_VALUES_EQUAL(firstPage.get(), mapper.PointerToAlignedMemory());
    UNIT_ASSERT_VALUES_EQUAL(TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::GetGlobalPagePoolSize(), 0U);
    UNIT_ASSERT_VALUES_EQUAL(mapper.MunmapsSize(), 0U);
}

Y_UNIT_TEST(AlignedMmapPageSize) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    auto size = TAlignedPagePool::POOL_PAGE_SIZE;
    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });
    UNIT_ASSERT_EQUAL(0U, mmapper.MunmapsSize());

    UNIT_ASSERT_VALUES_EQUAL(block.get(), mmapper.PointerToAlignedMemory());

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), TAlignedPagePool::POOL_PAGE_SIZE + TAlignedPagePool::ALLOC_AHEAD_PAGES * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(UnalignedMmapPageSize) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
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
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    auto smallSize = NSystemInfo::GetPageSize();
    auto size = smallSize + 1024 * TAlignedPagePool::POOL_PAGE_SIZE;
    TScopedMemoryMapper mmapper(/*aligned=*/true);

    auto block = std::shared_ptr<void>(alloc.GetBlock(size), [&](void* addr) { alloc.ReturnBlock(addr, size); });

    UNIT_ASSERT_EQUAL(2, mmapper.MunmapsSize());
    auto expected0 = (TScopedMemoryMapper::TUnmapEntry{.Addr = (char*)mmapper.PointerToAlignedMemory() + size, .Size = TAlignedPagePool::POOL_PAGE_SIZE - smallSize});
    UNIT_ASSERT_EQUAL(expected0, mmapper.Munmaps(0));
    auto expected1 = TScopedMemoryMapper::TUnmapEntry{
        .Addr = (char*)mmapper.PointerToAlignedMemory() + TAlignedPagePool::ALLOC_AHEAD_PAGES * TAlignedPagePool::POOL_PAGE_SIZE + size - smallSize,
        .Size = smallSize};
    UNIT_ASSERT_EQUAL(expected1, mmapper.Munmaps(1));

    UNIT_ASSERT_VALUES_EQUAL(block.get(), mmapper.PointerToAlignedMemory());

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetFreePageCount(), TAlignedPagePool::ALLOC_AHEAD_PAGES - 1);

    UNIT_ASSERT_VALUES_EQUAL(alloc.GetAllocated(), size + (TAlignedPagePool::ALLOC_AHEAD_PAGES - 1) * TAlignedPagePool::POOL_PAGE_SIZE);
}

Y_UNIT_TEST(UnalignedMmapUnalignedSize) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
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

Y_UNIT_TEST(ReusesDiscardedBlocks) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    constexpr size_t size = MaxMidSize;
    void* original = nullptr;
    {
        TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
        original = alloc.GetBlock(size);
        alloc.ReturnBlock(original, size);
    }
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::DoCleanupGlobalFreeList(0);

    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    void* reused = alloc.GetBlock(size);
    UNIT_ASSERT_VALUES_EQUAL(reused, original);
    UNIT_ASSERT_VALUES_EQUAL(mmapper.MunmapsSize(), 0);
    alloc.ReturnBlock(reused, size);
}

Y_UNIT_TEST(PropagatesFreezeErrors) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    constexpr size_t size = MaxMidSize;
    void* block = alloc.GetBlock(size);
    alloc.ReturnBlock(block, size);
    const i64 committedBytes = GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>();

    auto& provider = TFakeMmap::GetInstance();
    const auto failOperation = [](void*, size_t) { return MakeMapperError(); };
    provider.OnFreeze = failOperation;
    UNIT_ASSERT_EXCEPTION_SATISFIES(TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::DoCleanupGlobalFreeList(0), TSystemError, IsMapperError);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), committedBytes);

    provider.OnFreeze = {};
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::DoCleanupGlobalFreeList(0);
    provider.OnUnfreeze = failOperation;
    UNIT_ASSERT_EXCEPTION_SATISFIES(alloc.GetBlock(size), TSystemError, IsMapperError);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), committedBytes - size);

    provider.OnUnfreeze = {};
    void* recovered = alloc.GetBlock(size);
    UNIT_ASSERT_VALUES_EQUAL(recovered, block);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), committedBytes);
    alloc.ReturnBlock(recovered, size);
}

Y_UNIT_TEST(TracksCommittedBytes) {
    TAlignedPagePool::DoCleanupGlobalFreeList(0);
    TAlignedPagePool::ResetGlobalsUT();
    const i64 initialBytes = GetTotalMmapedBytes();
    constexpr size_t size = MaxMidSize;
    for (bool discardBeforeReset : {false, true}) {
        {
            TAlignedPagePool alloc(__LOCATION__);
            void* block = alloc.GetBlock(size);
            const i64 allocatedBytes = GetTotalMmapedBytes();
            UNIT_ASSERT_VALUES_EQUAL(allocatedBytes, initialBytes + alloc.GetAllocated());
            alloc.ReturnBlock(block, size);
            UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes(), allocatedBytes);
            TAlignedPagePool::DoCleanupGlobalFreeList(0);
            UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes(), allocatedBytes - size);
            block = alloc.GetBlock(size);
            UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes(), allocatedBytes);
            alloc.ReturnBlock(block, size);
        }
        if (discardBeforeReset) {
            TAlignedPagePool::DoCleanupGlobalFreeList(0);
            UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes(), initialBytes);
        }
        TAlignedPagePool::ResetGlobalsUT();
        UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes(), initialBytes);
    }
}

Y_UNIT_TEST(ReleasesUnclaimedFrozenPage) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    constexpr size_t size = MaxMidSize;
    void* block = alloc.GetBlock(size);
    alloc.ReturnBlock(block, size);
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::DoCleanupGlobalFreeList(0);
    UNIT_ASSERT_VALUES_EQUAL(mmapper.MunmapsSize(), 0);

    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    UNIT_ASSERT_VALUES_EQUAL(mmapper.MunmapsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(mmapper.Munmaps(0).Addr, block);
    UNIT_ASSERT_VALUES_EQUAL(mmapper.Munmaps(0).Size, size);
}

Y_UNIT_TEST(DoesNotCountFailedMmap) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    const i64 initialBytes = GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>();
    TFakeMmap::GetInstance().OnMmap = [](size_t) { return MakeMapperError(); };

    UNIT_ASSERT_EXCEPTION_SATISFIES(alloc.GetBlock(MaxMidSize), TSystemError, IsMapperError);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), initialBytes);
}

Y_UNIT_TEST(PropagatesMunmapErrors) {
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>>::ResetGlobalsUT();
    TScopedMemoryMapper mmapper(/*aligned=*/true);
    TAlignedPagePoolImpl<TTrackedMmap<TFakeMmap>> alloc(__LOCATION__);
    constexpr size_t size = MaxMidSize + TAlignedPagePool::POOL_PAGE_SIZE;
    void* block = alloc.GetBlock(size);
    const i64 committedBytes = GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>();
    auto& provider = TFakeMmap::GetInstance();
    provider.OnMunmap = [](void*, size_t) { return MakeMapperError(); };

    UNIT_ASSERT_EXCEPTION_SATISFIES(alloc.ReturnBlock(block, size), TSystemError, IsMapperError);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), committedBytes);

    provider.OnMunmap = {};
    alloc.ReturnBlock(block, size);
    UNIT_ASSERT_VALUES_EQUAL(GetTotalMmapedBytes<TTrackedMmap<TFakeMmap>>(), committedBytes - size);
}

} // Y_UNIT_TEST_SUITE(TAlignedPagePoolTest)

} // namespace NKikimr::NMiniKQL
