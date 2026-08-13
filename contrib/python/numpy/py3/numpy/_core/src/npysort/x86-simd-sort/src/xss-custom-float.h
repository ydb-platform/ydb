#ifndef XSS_CUSTOM_FLOAT
#define XSS_CUSTOM_FLOAT
#include <cstdint>
namespace xss {
namespace fp {
    template <typename T>
    inline constexpr bool is_floating_point_v = std::is_floating_point_v<T>;

    template <typename T>
    static bool isnan(T elem)
    {
        return std::isnan(elem);
    }
    template <typename T>
    static bool isunordered(T a, T b)
    {
        return std::isunordered(a, b);
    }
    template <typename T>
    static T max()
    {
        return std::numeric_limits<T>::max();
    }
    template <typename T>
    static T min()
    {
        return std::numeric_limits<T>::min();
    }
    template <typename T>
    static T infinity()
    {
        return std::numeric_limits<T>::infinity();
    }
    template <typename T>
    static T quiet_NaN()
    {
        return std::numeric_limits<T>::quiet_NaN();
    }

#ifdef __FLT16_MAX__
    typedef union {
        _Float16 f_;
        uint16_t i_;
    } Fp16Bits;

    static _Float16 convert_bits(uint16_t val)
    {
        Fp16Bits temp;
        temp.i_ = val;
        return temp.f_;
    }

    template <>
    [[maybe_unused]] inline constexpr bool is_floating_point_v<_Float16> = true;

    template <>
    [[maybe_unused]] bool isnan<_Float16>(_Float16 elem)
    {
        return elem != elem;
    }
    template <>
    [[maybe_unused]] bool isunordered<_Float16>(_Float16 a, _Float16 b)
    {
        return isnan(a) || isnan(b);
    }
    template <>
    [[maybe_unused]] _Float16 max<_Float16>()
    {
        return convert_bits(0x7bff);
    }
    template <>
    [[maybe_unused]] _Float16 min<_Float16>()
    {
        return convert_bits(0x0400);
    }
    template <>
    [[maybe_unused]] _Float16 infinity<_Float16>()
    {
        return convert_bits(0x7c00);
    }
    template <>
    [[maybe_unused]] _Float16 quiet_NaN<_Float16>()
    {
        return convert_bits(0x7c01);
    }
#endif

} // namespace fp
} // namespace xss
#endif // XSS_CUSTOM_FLOAT
