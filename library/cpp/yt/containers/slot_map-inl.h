#ifndef SLOT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include slot_map.h"
// For the sake of sane code completion.
#include "slot_map.h"
#endif

#include <library/cpp/yt/assert/assert.h>

#include <iterator>
#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TDefaultSlotMapTraits::IsEmpty(const T& value) const
{
    return !static_cast<bool>(value);
}

template <class T>
T TDefaultSlotMapTraits::MakeEmpty() const
{
    return T();
}

////////////////////////////////////////////////////////////////////////////////

template <class T, template <class> class TVector, class TTraits>
constexpr TSlotMap<T, TVector, TTraits>::TSlotMap(TTraits traits)
    : Traits_(std::move(traits))
{ }

template <class T, template <class> class TVector, class TTraits>
TSlotMapIndex TSlotMap<T, TVector, TTraits>::Insert(T value)
{
    YT_VERIFY(!Traits_.IsEmpty(value));

    if (!FreeList_.empty()) {
        auto index = FreeList_.back();
        FreeList_.pop_back();
        auto& slot = Slots_[index.Underlying()];
        YT_ASSERT(Traits_.IsEmpty(slot));
        slot = std::move(value);
        return index;
    }

    auto index = TSlotMapIndex(std::ssize(Slots_));
    Slots_.push_back(std::move(value));
    return index;
}

template <class T, template <class> class TVector, class TTraits>
T TSlotMap<T, TVector, TTraits>::Extract(TSlotMapIndex index)
{
    auto rawIndex = index.Underlying();
    YT_ASSERT(rawIndex < std::ssize(Slots_));

    auto& slot = Slots_[rawIndex];
    YT_ASSERT(!Traits_.IsEmpty(slot));

    auto value = std::move(slot);
    slot = Traits_.template MakeEmpty<T>();
    FreeList_.push_back(index);
    return value;
}

template <class T, template <class> class TVector, class TTraits>
template <class F>
void TSlotMap<T, TVector, TTraits>::ExtractAll(F&& func)
{
    for (auto& slot : Slots_) {
        if (!Traits_.IsEmpty(slot)) {
            func(std::move(slot));
        }
    }
    Slots_.clear();
    FreeList_.clear();
}

template <class T, template <class> class TVector, class TTraits>
bool TSlotMap<T, TVector, TTraits>::IsEmpty() const
{
    return Slots_.size() == FreeList_.size();
}

template <class T, template <class> class TVector, class TTraits>
int TSlotMap<T, TVector, TTraits>::GetSize() const
{
    return std::ssize(Slots_) - std::ssize(FreeList_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
