#include "mkql_custom_list.h"

namespace NKikimr {
namespace NMiniKQL {

TForwardListValue::TForwardListValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream)
    : TCustomListValue(memInfo)
    , Stream(std::move(stream))
{
    MKQL_ENSURE(Stream, "Empty stream.");
}

TForwardListValue::TIterator::TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream)
    : TComputationValue(memInfo), Stream(std::move(stream))
{}

bool TForwardListValue::TIterator::Next(NUdf::TUnboxedValue& value) {
    const auto status = Stream.Fetch(value);
    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status.");
    return status == NUdf::EFetchStatus::Ok;
}

NUdf::TUnboxedValue TForwardListValue::GetListIterator() const {
    MKQL_ENSURE(Stream, "Second pass for ForwardList");
    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), std::move(Stream)));
}

TExtendListValue::TExtendListValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists)
    : TCustomListValue(memInfo)
    , Lists(std::move(lists))
{
    MKQL_MEM_TAKE(memInfo, Lists.data(), Lists.capacity() * sizeof(NUdf::TUnboxedValue));
    Y_ASSERT(!Lists.empty());
}

TExtendListValue::TIterator::TIterator(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& iters)
    : TComputationValue(memInfo)
    , Iters(std::move(iters))
    , Index(0)
{
    MKQL_MEM_TAKE(memInfo, Iters.data(), Iters.capacity() * sizeof(NUdf::TUnboxedValue));
}

TExtendListValue::TIterator::~TIterator()
{
    MKQL_MEM_RETURN(GetMemInfo(), Iters.data(), Iters.capacity() * sizeof(NUdf::TUnboxedValue));
}

bool TExtendListValue::TIterator::Next(NUdf::TUnboxedValue& value) {
    for (; Index < Iters.size(); ++Index) {
        if (Iters[Index].Next(value)) {
            return true;
        }
    }
    return false;
}

bool TExtendListValue::TIterator::Skip() {
    for (; Index < Iters.size(); ++Index) {
        if (Iters[Index].Skip()) {
            return true;
        }
    }
    return false;
}

NUdf::TUnboxedValue TExtendListValue::GetListIterator() const {
    TUnboxedValueVector iters;
    iters.reserve(Lists.size());
    for (const auto& list : Lists) {
        iters.emplace_back(list.GetListIterator());
    }

    return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), std::move(iters)));
}

TExtendListValue::~TExtendListValue() {
    MKQL_MEM_RETURN(GetMemInfo(), Lists.data(), Lists.capacity() * sizeof(NUdf::TUnboxedValue));
}

ui64 TExtendListValue::GetListLength() const {
    if (!Length) {
        ui64 length = 0ULL;
        for (const auto& list : Lists) {
            ui64 partialLength = list.GetListLength();
            length += partialLength;
        }

        Length = length;
    }

    return *Length;
}

bool TExtendListValue::HasListItems() const  {
    if (!HasItems) {
        for (const auto& list : Lists) {
            if (list.HasListItems()) {
                HasItems = true;
                break;
            }
        }

        if (!HasItems) {
            HasItems = false;
        }
    }

    return *HasItems;
}

TExtendStreamValue::TExtendStreamValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& lists)
    : TBase(memInfo)
    , Lists(std::move(lists))
{
    MKQL_MEM_TAKE(memInfo, Lists.data(), Lists.capacity() * sizeof(NUdf::TUnboxedValue));
    Y_ASSERT(!Lists.empty());
}

TExtendStreamValue::~TExtendStreamValue() {
    MKQL_MEM_RETURN(GetMemInfo(), Lists.data(), Lists.capacity() * sizeof(NUdf::TUnboxedValue));
}

NUdf::EFetchStatus TExtendStreamValue::Fetch(NUdf::TUnboxedValue& value)  {
    for (; Index < Lists.size(); ++Index) {
        const auto status = Lists[Index].Fetch(value);
        if (status != NUdf::EFetchStatus::Finish) {
            return status;
        }
    }
    return NUdf::EFetchStatus::Finish;
}

}
}
