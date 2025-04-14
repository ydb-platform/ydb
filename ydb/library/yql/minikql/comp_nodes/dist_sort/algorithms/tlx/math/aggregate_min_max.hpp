/*******************************************************************************
 * tlx/math/aggregate_min_max.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2019 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_AGGREGATE_MIN_MAX_HEADER
#define TLX_MATH_AGGREGATE_MIN_MAX_HEADER

#include <algorithm>
#include <limits>

namespace tlx {

//! \addtogroup tlx_math
//! \{

/*!
 * Calculate running aggregate statistics: feed it with values, and it will keep
 * the minimum and the maximum values.
 */
template <typename Type_>
class AggregateMinMax
{
public:
    using Type = Type_;

    //! default constructor
    AggregateMinMax() = default;

    //! initializing constructor
    AggregateMinMax(const Type& min, const Type& max) noexcept
        : min_(min), max_(max) { }

    //! add a value to the running aggregation
    AggregateMinMax& add(const Type& value) noexcept {
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        return *this;
    }

    //! return minimum over all values aggregated
    const Type& min() const noexcept { return min_; }

    //! return maximum over all values aggregated
    const Type& max() const noexcept { return max_; }

    //! return maximum - minimum over all values aggregated
    Type span() const noexcept { return max_ - min_; }

    //! change currently aggregated minimum
    void set_min(const Type& v) noexcept { min_ = v; }

    //! change currently aggregated minimum
    void set_max(const Type& v) noexcept { max_ = v; }

    //! operator + to combine two AggregateMinMax<>
    AggregateMinMax operator + (const AggregateMinMax& a) const noexcept {
        return AggregateMinMax(
            // min, max
            std::min(min_, a.min_), std::max(max_, a.max_));
    }

    //! operator += to combine two AggregateMinMax<>
    AggregateMinMax& operator += (const AggregateMinMax& a) noexcept {
        min_ = std::min(min_, a.min_);
        max_ = std::max(max_, a.max_);
        return *this;
    }

    //! serialization method for cereal.
    template <typename Archive>
    void serialize(Archive& archive) {
        archive(min_, max_);
    }

private:
    //! minimum value
    Type min_ = std::numeric_limits<Type>::max();

    //! maximum value
    Type max_ = std::numeric_limits<Type>::lowest();
};

//! \}

} // namespace tlx

#endif // !TLX_MATH_AGGREGATE_MIN_MAX_HEADER

/******************************************************************************/
