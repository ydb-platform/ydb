#ifndef YTREE_SIZE_INL_H_
#error "Direct inclusion of this file is not allowed, include size.h"
// For the sake of sane code completion.
#include "size.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

constexpr TSize::TSize()
    : Underlying_(0)
{ }

constexpr TSize::TSize(TUnderlying underlying)
    : Underlying_(underlying)
{ }

constexpr TSize::operator const TUnderlying&() const
{
    return Underlying_;
}

constexpr TSize::operator TUnderlying&()
{
    return Underlying_;
}

constexpr TSize::TUnderlying& TSize::Underlying() &
{
    return Underlying_;
}

constexpr const TSize::TUnderlying& TSize::Underlying() const &
{
    return Underlying_;
}

constexpr TSize::TUnderlying TSize::Underlying() &&
{
    return Underlying_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

////////////////////////////////////////////////////////////////////////////////

namespace std {

////////////////////////////////////////////////////////////////////////////////

template <>
struct hash<NYT::NYTree::TSize>
{
    size_t operator()(const NYT::NYTree::TSize& value) const
    {
        return std::hash<NYT::NYTree::TSize::TUnderlying>()(value.Underlying());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class numeric_limits<NYT::NYTree::TSize>
{
public:
    #define XX(name) \
        static constexpr decltype(numeric_limits<NYT::NYTree::TSize::TUnderlying>::name) name = numeric_limits<NYT::NYTree::TSize::TUnderlying>::name;

    XX(is_specialized)
    XX(is_signed)
    XX(digits)
    XX(digits10)
    XX(max_digits10)
    XX(is_integer)
    XX(is_exact)
    XX(radix)
    XX(min_exponent)
    XX(min_exponent10)
    XX(max_exponent)
    XX(max_exponent10)
    XX(has_infinity)
    XX(has_quiet_NaN)
    XX(has_signaling_NaN)
    XX(has_denorm)
    XX(has_denorm_loss)
    XX(is_iec559)
    XX(is_bounded)
    XX(is_modulo)
    XX(traps)
    XX(tinyness_before)
    XX(round_style)

    #undef XX

    #define XX(name) \
        static constexpr NYT::NYTree::TSize name() noexcept \
        { \
            return NYT::NYTree::TSize(numeric_limits<NYT::NYTree::TSize::TUnderlying>::name()); \
        }

    XX(min)
    XX(max)
    XX(lowest)
    XX(epsilon)
    XX(round_error)
    XX(infinity)
    XX(quiet_NaN)
    XX(signaling_NaN)
    XX(denorm_min)

    #undef XX
};

////////////////////////////////////////////////////////////////////////////////

} // namespace std

////////////////////////////////////////////////////////////////////////////////
