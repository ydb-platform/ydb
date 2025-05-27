#pragma once

#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/src_location.h>

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
    explicit TMemoryUsageInfo(const TStringBuf& title);
    ~TMemoryUsageInfo();

    void AllowMissing();
    void CheckOnExit(bool check);

#ifndef NDEBUG
    void Take(const void* mem, ui64 size, TMkqlLocation location);
#endif

#ifndef NDEBUG
    void Return(const void* mem, ui64 size);
#endif

#ifndef NDEBUG
    void Return(const void* mem);
#endif

    i64 GetUsage() const;

    ui64 GetAllocated() const;
    ui64 GetFreed() const;
    ui64 GetPeak() const;

    void PrintTo(IOutputStream& out) const;

    void VerifyDebug() const;

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
