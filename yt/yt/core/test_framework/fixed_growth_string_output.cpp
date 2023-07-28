#include "fixed_growth_string_output.h"

#include "framework.h"

namespace NYT {

TFixedGrowthStringOutput::TFixedGrowthStringOutput(TString* s, size_t growthSize) noexcept
    : String_(s)
    , GrowthSize_(growthSize)
{ }

size_t TFixedGrowthStringOutput::DoNext(void** ptr)
{
    auto previousSize = String_->size();
    String_->resize(String_->size() + GrowthSize_);
    *ptr = String_->begin() + previousSize;
    return String_->size() - previousSize;
}

void TFixedGrowthStringOutput::DoUndo(size_t len)
{
    ASSERT_LE(len, String_->size());
    String_->resize(String_->size() - len);
}

} // namespace NYT