#ifndef FIXED_POINT_NUMBER_INL_H_
#error "Direct inclusion of this file is not allowed, include fixed_point_number.h"
// For the sake of sane code completion.
#include "fixed_point_number.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr T ComputePower(T base, int exponent)
{
    return exponent == 0 ? 1 : base * ComputePower(base, exponent - 1);
}

////////////////////////////////////////////////////////////////////////////////

template <typename U, int P>
TFixedPointNumber<U, P>::TFixedPointNumber()
    : Value_()
{ }

template <typename U, int P>
TFixedPointNumber<U, P>::TFixedPointNumber(i64 value)
    : Value_(value * ScalingFactor)
{ }

template <typename U, int P>
TFixedPointNumber<U, P>::TFixedPointNumber(double value)
    : Value_(std::round(value * ScalingFactor))
{ }

template <typename U, int P>
TFixedPointNumber<U, P>::operator i64 () const
{
    return Value_ / ScalingFactor;
}

template <typename U, int P>
TFixedPointNumber<U, P>::operator double () const
{
    return static_cast<double>(Value_) / ScalingFactor;
}

template <typename U, int P>
U TFixedPointNumber<U, P>::GetUnderlyingValue() const
{
    return Value_;
}

template <typename U, int P>
void TFixedPointNumber<U, P>::SetUnderlyingValue(U value)
{
    Value_ = value;
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator += (const TFixedPointNumber& rhs)
{
    Value_ += rhs.Value_;
    return *this;
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator -= (const TFixedPointNumber<U, P>& rhs)
{
    Value_ -= rhs.Value_;
    return *this;
}

template <typename U, int P>
template <typename T>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator *= (const T& value)
{
    Value_ *= value;
    return *this;
}

template <typename U, int P>
template <typename T>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator /= (const T& value)
{
    Value_ /= value;
    return *this;
}

template <typename U, int P>
TFixedPointNumber<U, P>& TFixedPointNumber<U, P>::operator *= (const double& value)
{
    Value_ = std::round(Value_ * value);
    return *this;
}

template <typename U, int P>
NYT::TFixedPointNumber<U, P> round(const NYT::TFixedPointNumber<U, P>& number)
{
    return number;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
