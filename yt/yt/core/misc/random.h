#pragma once

#include "common.h"

#include <util/generic/noncopyable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A fully deterministic pseudo-random number generator.
class TRandomGenerator final
    : public TNonCopyable
{
public:
    TRandomGenerator();
    explicit TRandomGenerator(ui64 seed);

    template <class T>
    T Generate();

private:
    ui64 Current_;

    ui64 GenerateInteger();
    double GenerateDouble();
};

////////////////////////////////////////////////////////////////////////////////

// TGenerator is supposed to implement operator(), which returns
// uniformly-distributed integers in the range [0, #max) given argument #max.
template <class TForwardIterator, class TOutputIterator, class TGenerator>
TOutputIterator RandomSampleN(
    TForwardIterator begin,
    TForwardIterator end,
    TOutputIterator output,
    size_t n,
    TGenerator&& generator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANDOM_INL_H_
#include "random-inl.h"
#undef RANDOM_INL_H_
