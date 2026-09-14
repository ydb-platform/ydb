#include "mkql_custom_list.h"

namespace NKikimr::NMiniKQL {

TForwardListValue::TForwardListValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream)
    : TCustomListValue(memInfo)
    , Stream_(std::move(stream))
{
    MKQL_ENSURE(Stream_, "Empty stream.");
}

TForwardListValue::TIterator::TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream)
    : TComputationValue(memInfo)
    , Stream_(std::move(stream))
{
}

bool TForwardListValue::TIterator::Next(NUdf::TUnboxedValue& value) {
    const auto status = Stream_.Fetch(value);
    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status.");
    return status == NUdf::EFetchStatus::Ok;
}

NUdf::TUnboxedValue TForwardListValue::GetListIterator() const {
    MKQL_ENSURE(Stream_, "Second pass for ForwardList");
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), std::move(Stream_)));
}

TExtendListValue::TExtendListValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists)
    : TCustomListValue(memInfo)
    , Lists_(std::move(lists))
{
    MKQL_MEM_TAKE(memInfo, Lists_.data(), Lists_.capacity() * sizeof(NUdf::TUnboxedValue));
    Y_ASSERT(!Lists_.empty());
}

TExtendListValue::TIterator::TIterator(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& iters)
    : TComputationValue(memInfo)
    , Iters_(std::move(iters))
    , Index_(0)
{
    MKQL_MEM_TAKE(memInfo, Iters_.data(), Iters_.capacity() * sizeof(NUdf::TUnboxedValue));
}

TExtendListValue::TIterator::~TIterator()
{
    MKQL_MEM_RETURN(GetMemInfo(), Iters_.data(), Iters_.capacity() * sizeof(NUdf::TUnboxedValue));
}

bool TExtendListValue::TIterator::Next(NUdf::TUnboxedValue& value) {
    for (; Index_ < Iters_.size(); ++Index_) {
        if (Iters_[Index_].Next(value)) {
            return true;
        }
    }
    return false;
}

bool TExtendListValue::TIterator::Skip() {
    for (; Index_ < Iters_.size(); ++Index_) {
        if (Iters_[Index_].Skip()) {
            return true;
        }
    }
    return false;
}

NUdf::TUnboxedValue TExtendListValue::GetListIterator() const {
    TUnboxedValueVector iters;
    iters.reserve(Lists_.size());
    for (const auto& list : Lists_) {
        iters.emplace_back(list.GetListIterator());
    }

    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), std::move(iters)));
}

TExtendListValue::~TExtendListValue() {
    MKQL_MEM_RETURN(GetMemInfo(), Lists_.data(), Lists_.capacity() * sizeof(NUdf::TUnboxedValue));
}

ui64 TExtendListValue::GetListLength() const {
    if (!Length_) {
        ui64 length = 0ULL;
        for (const auto& list : Lists_) {
            ui64 partialLength = list.GetListLength();
            length += partialLength;
        }

        Length_ = length;
    }

    return *Length_;
}

bool TExtendListValue::HasListItems() const {
    if (!HasItems_) {
        for (const auto& list : Lists_) {
            if (list.HasListItems()) {
                HasItems_ = true;
                break;
            }
        }

        if (!HasItems_) {
            HasItems_ = false;
        }
    }

    return *HasItems_;
}

TExtendStreamValue::TExtendStreamValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists)
    : TBase(memInfo)
    , Lists_(std::move(lists))
{
    MKQL_MEM_TAKE(memInfo, Lists_.data(), Lists_.capacity() * sizeof(NUdf::TUnboxedValue));
    Y_ASSERT(!Lists_.empty());
}

TExtendStreamValue::~TExtendStreamValue() {
    MKQL_MEM_RETURN(GetMemInfo(), Lists_.data(), Lists_.capacity() * sizeof(NUdf::TUnboxedValue));
}

NUdf::EFetchStatus TExtendStreamValue::Fetch(NUdf::TUnboxedValue& value) {
    for (; Index_ < Lists_.size(); ++Index_) {
        const auto status = Lists_[Index_].Fetch(value);
        if (status != NUdf::EFetchStatus::Finish) {
            return status;
        }
    }
    return NUdf::EFetchStatus::Finish;
}

} // namespace NKikimr::NMiniKQL
