#pragma once

#include <util/generic/string.h>

#include <util/stream/zerocopy_output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// This class grows its buffer by chunks of fixed length, thus appending one char is O(n) amortized.
//
// NB: Very slow class! Use only for tests.
class TFixedGrowthStringOutput
    : public IZeroCopyOutput
{
public:
    TFixedGrowthStringOutput(TString* s, size_t growthSize) noexcept;

private:
    size_t DoNext(void** ptr) override;
    void DoUndo(size_t len) override;

private:
    TString* const String_;
    const size_t GrowthSize_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT