#pragma once

#include <library/cpp/yt/misc/strong_typedef.h>

#include <vector>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! A stable handle into a #TSlotMap; remains valid until the value is extracted.
YT_DEFINE_STRONG_TYPEDEF(TSlotMapIndex, ui32);

//! A sentinel that never refers to a live slot.
constexpr auto InvalidSlotMapIndex = TSlotMapIndex(-1);

////////////////////////////////////////////////////////////////////////////////

//! Defines the tombstone marking free slots: a value-initialized |T| convertible
//! to |false| (empty callback, null pointer). Provide a custom traits object for
//! types lacking a null state.
struct TDefaultSlotMapTraits
{
    template <class T>
    bool IsEmpty(const T& value) const;

    template <class T>
    T MakeEmpty() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Maps stable indices to values, recycling freed slots via a free list.
/*!
 *  Values live in a dense vector; each #Insert returns an index valid until the
 *  value is extracted. Freed slots hold a tombstone (see #TDefaultSlotMapTraits).
 *  Inserted values must be non-empty. Not thread-safe.
 */
template <
    class T,
    template <class> class TVector = std::vector,
    class TTraits = TDefaultSlotMapTraits>
class TSlotMap
{
public:
    explicit constexpr TSlotMap(TTraits traits = {});

    //! Inserts a non-empty #value; returns its stable index.
    TSlotMapIndex Insert(T value);

    //! Extracts and returns the value at #index, freeing the slot.
    /*!
     *  #index must refer to a live slot (as returned by a prior #Insert and not
     *  yet extracted).
     */
    T Extract(TSlotMapIndex index);

    //! Moves each live value into #func, then clears the map.
    template <class F>
    void ExtractAll(F&& func);

    bool IsEmpty() const;
    int GetSize() const;

private:
    Y_NO_UNIQUE_ADDRESS TTraits Traits_;
    TVector<T> Slots_;
    TVector<TSlotMapIndex> FreeList_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SLOT_MAP_INL_H_
#include "slot_map-inl.h"
#undef SLOT_MAP_INL_H_
