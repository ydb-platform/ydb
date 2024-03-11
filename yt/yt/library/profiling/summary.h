#pragma once

#include <util/system/types.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TSummarySnapshot
{
public:
    void Record(T value);
    void Add(const TSummarySnapshot& other);

    TSummarySnapshot<T>& operator += (const TSummarySnapshot& other);
    TSummarySnapshot() = default;
    TSummarySnapshot(T sum, T min, T max, T last, i64 count);

    bool operator == (const TSummarySnapshot& other) const = default;

    T Sum() const;
    T Min() const;
    T Max() const;
    T Last() const;
    i64 Count() const;

private:
    T Sum_{}, Min_{}, Max_{}, Last_{};
    i64 Count_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

#define SUMMARY_INL_H_
#include "summary-inl.h"
#undef SUMMARY_INL_H_
