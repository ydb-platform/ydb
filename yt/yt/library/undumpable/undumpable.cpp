#include "undumpable.h"

#if defined(_linux_)
#include <sys/mman.h>
#endif

#include <util/generic/hash.h>

#include <library/cpp/yt/threading/spin_lock.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/yt/memory/ref_counted.h>
#include <library/cpp/yt/memory/new.h>

#include <yt/yt/library/profiling/sensor.h>

#include <atomic>
#include <optional>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TUndumpableMark
{
    // TUndumpableMark-s are never freed. All objects are linked through NextMark.
    TUndumpableMark* NextMark = nullptr;
    TUndumpableMark* NextFree = nullptr;

    void* Ptr = nullptr;
    size_t Size = 0;
};

class TUndumpableMemoryManager
{
public:
    constexpr TUndumpableMemoryManager() = default;

    TUndumpableMark* MarkUndumpable(void* ptr, size_t size)
    {
        UndumpableSize_.fetch_add(size, std::memory_order::relaxed);

        auto guard = Guard(MarkListsLock_);
        auto* mark = GetFreeMark();
        mark->Ptr = ptr;
        mark->Size = size;
        return mark;
    }

    void UnmarkUndumpable(TUndumpableMark* mark)
    {
        UndumpableSize_.fetch_sub(mark->Size, std::memory_order::relaxed);

        mark->Size = 0;
        mark->Ptr = nullptr;

        auto guard = Guard(MarkListsLock_);
        FreeMark(mark);
    }

    void MarkUndumpableOob(void* ptr, size_t size)
    {
        auto* mark = MarkUndumpable(ptr, size);
        auto guard = Guard(MarkTableLock_);

        if (!MarkTable_) {
            MarkTable_.emplace();
        }
        YT_VERIFY(MarkTable_->emplace(ptr, mark).second);
    }

    void UnmarkUndumpableOob(void* ptr)
    {
        auto guard = Guard(MarkTableLock_);

        if (!MarkTable_) {
            MarkTable_.emplace();
        }

        auto it = MarkTable_->find(ptr);
        YT_VERIFY(it != MarkTable_->end());
        auto* mark = it->second;
        MarkTable_->erase(it);
        guard.Release();

        UnmarkUndumpable(mark);
    }

    size_t GetUndumpableMemorySize() const
    {
        return UndumpableSize_.load();
    }

    size_t GetUndumpableMemoryFootprint() const
    {
        return UndumpableFootprint_.load();
    }

    TCutBlocksInfo CutUndumpableRegionsFromCoredump()
    {
        TCutBlocksInfo result;
#if defined(_linux_)
        auto* mark = AllMarksHead_;
        while (mark) {
            int ret = ::madvise(mark->Ptr, mark->Size, MADV_DONTDUMP);
            if (ret == 0) {
                ++result.MarkedSize += mark->Size;
            } else {
                auto errorCode = LastSystemError();
                for (auto& record : result.FailedToMarkMemory) {
                    if (record.ErrorCode == 0 || record.ErrorCode == errorCode) {
                        record.ErrorCode = errorCode;
                        record.Size += mark->Size;
                        break;
                    }
                }
            }
            mark = mark->NextMark;
        }
#endif
        return result;
    }

private:
    std::atomic<size_t> UndumpableSize_ = 0;
    std::atomic<size_t> UndumpableFootprint_ = 0;

    NThreading::TSpinLock MarkListsLock_;
    TUndumpableMark* AllMarksHead_ = nullptr;
    TUndumpableMark* FreeMarksHead_ = nullptr;

    NThreading::TSpinLock MarkTableLock_;
    std::optional<THashMap<void*, TUndumpableMark*>> MarkTable_;

    TUndumpableMark* GetFreeMark()
    {
        if (FreeMarksHead_) {
            auto mark = FreeMarksHead_;
            FreeMarksHead_ = mark->NextFree;
            return mark;
        }

        auto* mark = new TUndumpableMark();
        UndumpableFootprint_.fetch_add(sizeof(*mark), std::memory_order::relaxed);

        mark->NextMark = AllMarksHead_;
        AllMarksHead_ = mark;
        return mark;
    }

    void FreeMark(TUndumpableMark* mark)
    {
        mark->NextFree = FreeMarksHead_;
        FreeMarksHead_ = mark;
    }
};

static constinit TUndumpableMemoryManager UndumpableMemoryManager;

////////////////////////////////////////////////////////////////////////////////

TUndumpableMark* MarkUndumpable(void* ptr, size_t size)
{
    return UndumpableMemoryManager.MarkUndumpable(ptr, size);
}

void UnmarkUndumpable(TUndumpableMark* mark)
{
    UndumpableMemoryManager.UnmarkUndumpable(mark);
}

void MarkUndumpableOob(void* ptr, size_t size)
{
    UndumpableMemoryManager.MarkUndumpableOob(ptr, size);
}

void UnmarkUndumpableOob(void* ptr)
{
    UndumpableMemoryManager.UnmarkUndumpableOob(ptr);
}

size_t GetUndumpableMemorySize()
{
    return UndumpableMemoryManager.GetUndumpableMemorySize();
}

size_t GetUndumpableMemoryFootprint()
{
    return UndumpableMemoryManager.GetUndumpableMemoryFootprint();
}

TCutBlocksInfo CutUndumpableRegionsFromCoredump()
{
    return UndumpableMemoryManager.CutUndumpableRegionsFromCoredump();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TUndumpableSensors)

struct TUndumpableSensors
    : public TRefCounted
{
    TUndumpableSensors()
    {
        NProfiling::TProfiler profiler{"/memory"};

        profiler.AddFuncGauge("/undumpable_memory_size", MakeStrong(this), [] {
            return GetUndumpableMemorySize();
        });

        profiler.AddFuncGauge("/undumpable_memory_footprint", MakeStrong(this), [] {
            return GetUndumpableMemoryFootprint();
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TUndumpableSensors)

static const TUndumpableSensorsPtr Sensors = New<TUndumpableSensors>();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
