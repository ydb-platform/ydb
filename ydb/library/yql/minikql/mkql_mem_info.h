#pragma once

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <unordered_map>

#ifndef NDEBUG

namespace NKikimr {
namespace NMiniKQL {

using TMkqlLocation = TSourceLocation;

}
}

#define __MKQL_LOCATION__ __LOCATION__
#   define MKQL_MEM_TAKE3(MemInfo, Mem, Size) \
        ::NKikimr::NMiniKQL::Take(MemInfo, (Mem), (Size), __MKQL_LOCATION__)
#   define MKQL_MEM_TAKE4(MemInfo, Mem, Size, Location) \
        ::NKikimr::NMiniKQL::Take(MemInfo, (Mem), (Size), Location)
#   define MKQL_MEM_RETURN(MemInfo, Mem, Size) \
        ::NKikimr::NMiniKQL::Return(MemInfo, (Mem), (Size))
#   define MKQL_MEM_RETURN_PTR(MemInfo, Mem) \
        ::NKikimr::NMiniKQL::Return(MemInfo, (Mem))
#else

namespace NKikimr {
namespace NMiniKQL {

using TMkqlLocation = int;

}
}

#define __MKQL_LOCATION__ 0
#   define MKQL_MEM_TAKE3(MemInfo, Mem, Size) \
        Y_UNUSED(MemInfo); Y_UNUSED(Mem); Y_UNUSED(Size);
#   define MKQL_MEM_TAKE4(MemInfo, Mem, Size, Location) \
    Y_UNUSED(MemInfo); Y_UNUSED(Mem); Y_UNUSED(Size); Y_UNUSED(Location);
#   define MKQL_MEM_RETURN(MemInfo, Mem, Size) \
        Y_UNUSED(MemInfo); Y_UNUSED(Mem); Y_UNUSED(Size);
#   define MKQL_MEM_RETURN_PTR(MemInfo, Mem) \
        Y_UNUSED(MemInfo); Y_UNUSED(Mem);
#endif

#define GET_MKQL_MEM_TAKE(_1, _2, _3, _4, IMPL, ...) IMPL
#define MKQL_MEM_TAKE(...) Y_PASS_VA_ARGS(GET_MKQL_MEM_TAKE(__VA_ARGS__, MKQL_MEM_TAKE4, MKQL_MEM_TAKE3)(__VA_ARGS__))


namespace NKikimr {
namespace NMiniKQL {

class TMemoryUsageInfo : public TThrRefBase
{
    struct TAllocationInfo {
        ui64 Size;
        TMkqlLocation Location;
        bool IsDeleted;
    };
public:
    inline explicit TMemoryUsageInfo(const TStringBuf& title)
        : Title_(title)
        , Allocated_(0)
        , Freed_(0)
        , Peak_(0)
        , AllowMissing_(false)
        , CheckOnExit_(true)
    {
    }

    inline ~TMemoryUsageInfo() {
        if (CheckOnExit_ && !UncaughtException()) {
            VerifyDebug();
        }
    }

    void AllowMissing() {
        AllowMissing_ = true;
    }

    void CheckOnExit(bool check) {
        CheckOnExit_ = check;
    }

#ifndef NDEBUG
    inline void Take(const void* mem, ui64 size, TMkqlLocation location) {
        Allocated_ += size;
        Peak_ = Max(Peak_, Allocated_ - Freed_);
        if (size == 0) {
            return;
        }
        if (AllowMissing_) {
            auto it = AllocationsMap_.find(mem);
            if (it != AllocationsMap_.end() && it->second.IsDeleted) {
                AllocationsMap_.erase(it);
            }
        }
        auto res = AllocationsMap_.insert({mem, { size, std::move(location), false }});
        Y_VERIFY_DEBUG(res.second, "Duplicate allocation at: %p, "
                                   "already allocated at: %s", mem, (TStringBuilder() << res.first->second.Location).c_str());
        //Clog << Title_ << " take: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
    }
#endif

#ifndef NDEBUG
    inline void Return(const void* mem, ui64 size) {
        Freed_ += size;
        if (size == 0) {
            return;
        }
        //Clog << Title_ << " free: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
        auto it = AllocationsMap_.find(mem);
        if (AllowMissing_ && it == AllocationsMap_.end()) {
            return;
        }

        if (AllowMissing_) {
            Y_VERIFY_DEBUG(!it->second.IsDeleted, "Double free at: %p", mem);
        } else {
            Y_VERIFY_DEBUG(it != AllocationsMap_.end(), "Double free at: %p", mem);
        }

        Y_VERIFY_DEBUG(size == it->second.Size,
                    "Deallocating wrong size at: %p, "
                    "allocated at: %s", mem, (TStringBuilder() << it->second.Location).c_str());
        if (AllowMissing_) {
            it->second.IsDeleted = true;
        } else {
            AllocationsMap_.erase(it);
        }
    }
#endif

#ifndef NDEBUG
    inline void Return(const void* mem) {
        //Clog << Title_ << " free: " << size << " -> " << mem << " " << AllocationsMap_.size() << Endl;
        auto it = AllocationsMap_.find(mem);
        if (AllowMissing_ && it == AllocationsMap_.end()) {
            return;
        }

        if (AllowMissing_) {
            Y_VERIFY_DEBUG(!it->second.IsDeleted, "Double free at: %p", mem);
        } else {
            Y_VERIFY_DEBUG(it != AllocationsMap_.end(), "Double free at: %p", mem);
        }

        Freed_ += it->second.Size;
        if (AllowMissing_) {
            it->second.IsDeleted = true;
        } else {
            AllocationsMap_.erase(it);
        }
    }
#endif

    inline i64 GetUsage() const {
        return static_cast<i64>(Allocated_) - static_cast<i64>(Freed_);
    }

    inline ui64 GetAllocated() const { return Allocated_; }
    inline ui64 GetFreed() const { return Freed_; }
    inline ui64 GetPeak() const { return Peak_; }

    inline void PrintTo(IOutputStream& out) const {
        out << Title_ << TStringBuf(": usage=") << GetUsage()
            << TStringBuf(" (allocated=") << GetAllocated()
            << TStringBuf(", freed=") << GetFreed()
            << TStringBuf(", peak=") << GetPeak()
            << ')';
    }

    inline void VerifyDebug() const {
#ifndef NDEBUG
        size_t leakCount = 0;
        for (const auto& it: AllocationsMap_) {
            if (it.second.IsDeleted) {
                continue;
            }
            ++leakCount;
            Cerr << TStringBuf("Not freed ")
                << it.first << TStringBuf(" size: ") << it.second.Size
                << TStringBuf(", location: ") << it.second.Location
                << Endl;
        }

        if (!AllowMissing_) {
            Y_VERIFY_DEBUG(GetUsage() == 0,
                    "Allocated: %ld, Freed: %ld, Peak: %ld",
                    GetAllocated(), GetFreed(), GetPeak());
        }
        Y_VERIFY_DEBUG(!leakCount, "Has no freed memory");
#endif
    }

private:
    const TString Title_;
    ui64 Allocated_;
    ui64 Freed_;
    ui64 Peak_;
    bool AllowMissing_;
    bool CheckOnExit_;

#ifndef NDEBUG
    std::unordered_map<const void*, TAllocationInfo> AllocationsMap_;
#endif
};

#ifndef NDEBUG
inline void Take(TMemoryUsageInfo& memInfo, const void* mem, ui64 size, TMkqlLocation location)
{
    memInfo.Take(mem, size, std::move(location));
}

inline void Take(TMemoryUsageInfo* memInfo, const void* mem, ui64 size, TMkqlLocation location)
{
    memInfo->Take(mem, size, std::move(location));
}

inline void Return(TMemoryUsageInfo& memInfo, const void* mem, ui64 size)
{
    memInfo.Return(mem, size);
}

inline void Return(TMemoryUsageInfo* memInfo, const void* mem, ui64 size)
{
    memInfo->Return(mem, size);
}

inline void Return(TMemoryUsageInfo& memInfo, const void* mem)
{
    memInfo.Return(mem);
}

inline void Return(TMemoryUsageInfo* memInfo, const void* mem)
{
    memInfo->Return(mem);
}
#endif

} // namespace NMiniKQL
} // namespace NKikimr

template <>
inline void Out<NKikimr::NMiniKQL::TMemoryUsageInfo>(
        IOutputStream& out,
        const NKikimr::NMiniKQL::TMemoryUsageInfo& memInfo)
{
    memInfo.PrintTo(out);
}
