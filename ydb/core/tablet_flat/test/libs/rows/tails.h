#pragma once

namespace NKikimr {
namespace NTable {
namespace NTest{

    constexpr ui32 operator"" _u32(unsigned long long val) noexcept
    {
        return static_cast<ui32>(val);
    }

    constexpr i32 operator"" _s32(unsigned long long val) noexcept
    {
        return static_cast<i32>(val);
    }

    constexpr ui64 operator"" _u64(unsigned long long val) noexcept
    {
        return static_cast<ui64>(val);
    }

    constexpr i64 operator"" _s64(unsigned long long val) noexcept
    {
        return static_cast<i64>(val);
    }

}
}
}
