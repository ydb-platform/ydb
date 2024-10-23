/*******************************************************************************
 * tlx/math/aggregate.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2015-2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_AGGREGATE_HEADER
#define TLX_MATH_AGGREGATE_HEADER

#include <tlx/define/likely.hpp>

#include <algorithm>
#include <cmath>
#include <limits>

namespace tlx {

//! \addtogroup tlx_math
//! \{

/*!
 * Calculate running aggregate statistics: feed it with values, and it will keep
 * the minimum, the maximum, the average, the value number, and the standard
 * deviation is values.
 */
template <typename Type_>
class Aggregate
{
public:
    using Type = Type_;

    //! default constructor
    Aggregate() = default;

    //! initializing constructor
    Aggregate(size_t count, const double& mean, const double& nvar,
              const Type& min, const Type& max) noexcept
        : count_(count), mean_(mean), nvar_(nvar),
          min_(min), max_(max) { }

    //! add a value to the running aggregation
    Aggregate& add(const Type& value) noexcept {
        count_++;
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        // Single-pass numerically stable mean and standard deviation
        // calculation as described in Donald Knuth: The Art of Computer
        // Programming, Volume 2, Chapter 4.2.2, Equations 15 & 16
        double delta = value - mean_;
        mean_ += delta / count_;
        nvar_ += delta * (value - mean_);
        return *this;
    }

    //! return number of values aggregated
    size_t count() const noexcept { return count_; }

    //! return sum over all values aggregated
    // can't make noexcept since Type_'s conversion is allowed to throw
    const Type sum() const { return static_cast<Type>(count_ * mean_); }

    //! return sum over all values aggregated
    const Type total() const { return sum(); }

    //! return the average over all values aggregated
    double average() const noexcept { return mean_; }

    //! return the average over all values aggregated
    double avg() const noexcept { return average(); }

    //! return the average over all values aggregated
    double mean() const noexcept { return average(); }

    //! return minimum over all values aggregated
    const Type& min() const noexcept { return min_; }

    //! return maximum over all values aggregated
    const Type& max() const noexcept { return max_; }

    //! return maximum - minimum over all values aggregated
    Type span() const noexcept { return max_ - min_; }

    //! return the variance of all values aggregated.
    //! ddof = delta degrees of freedom
    //! Set to 0 if you have the entire distribution
    //! Set to 1 if you have a sample (to correct for bias)
    double variance(size_t ddof = 1) const {
        if (count_ <= 1) return 0.0;
        return nvar_ / static_cast<double>(count_ - ddof);
    }

    //! return the variance of all values aggregated.
    //! ddof = delta degrees of freedom
    //! Set to 0 if you have the entire distribution
    //! Set to 1 if you have a sample (to correct for bias)
    double var(size_t ddof = 1) const {
        return variance(ddof);
    }

    //! return the standard deviation of all values aggregated.
    //! ddof = delta degrees of freedom
    //! Set to 0 if you have the entire distribution
    //! Set to 1 if you have a sample (to correct for bias)
    double standard_deviation(size_t ddof = 1) const {
        return std::sqrt(variance(ddof));
    }

    //! return the standard deviation of all values aggregated.
    //! ddof = delta degrees of freedom
    //! Set to 0 if you have the entire distribution
    //! Set to 1 if you have a sample (to correct for bias)
    double stdev(size_t ddof = 1) const { return standard_deviation(ddof); }

    //! operator + to combine two Aggregate<>
    Aggregate operator + (const Aggregate& a) const noexcept {
        return Aggregate(
            // count
            count_ + a.count_,
            // mean
            combine_means(a),
            // merging variance is a bit complicated
            combine_variance(a),
            // min, max
            std::min(min_, a.min_), std::max(max_, a.max_));
    }

    //! operator += to combine two Aggregate<>
    Aggregate& operator += (const Aggregate& a) noexcept {
        mean_ = combine_means(a);
        min_ = std::min(min_, a.min_);
        max_ = std::max(max_, a.max_);
        nvar_ = combine_variance(a);
        count_ += a.count_;
        return *this;
    }

    //! serialization method for cereal.
    template <typename Archive>
    void serialize(Archive& archive) {
        archive(count_, mean_, nvar_, min_, max_);
    }

private:
    //! combine means, check if either count is zero. fix problems with NaN
    double combine_means(const Aggregate& a) const noexcept {
        if (count_ == 0)
            return a.mean_;
        if (a.count_ == 0)
            return mean_;
        return (mean_ * count_ + a.mean_ * a.count_) / (count_ + a.count_);
    }

    //! T. Chan et al 1979, "Updating Formulae and a Pairwise Algorithm for
    //! Computing Sample Variances"
    double combine_variance(const Aggregate& other) const noexcept {
        double delta = mean_ - other.mean_;
        return nvar_ + other.nvar_ + (delta * delta) *
               (count_ * other.count_) / (count_ + other.count_);
    }

    //! number of values aggregated
    size_t count_ = 0;

    //! mean of values
    double mean_ = 0.0;

    //! approximate count * variance; stddev = sqrt(nvar / (count-1))
    double nvar_ = 0.0;

    //! minimum value
    Type min_ = std::numeric_limits<Type>::max();

    //! maximum value
    Type max_ = std::numeric_limits<Type>::lowest();
};

//! \}

} // namespace tlx

#endif // !TLX_MATH_AGGREGATE_HEADER

/******************************************************************************/
