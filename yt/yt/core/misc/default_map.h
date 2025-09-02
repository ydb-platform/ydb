#pragma once

#include "property.h"

#include <library/cpp/yt/string/string_builder.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TDefaultMap
    : public T
{
public:
    using TUnderlying = T;
    using mapped_type = typename T::mapped_type;

    DEFINE_BYREF_RO_PROPERTY(mapped_type, DefaultValue);

    TDefaultMap() = default;

    explicit TDefaultMap(mapped_type defaultValue);

    template <class K>
    mapped_type& operator[](const K& key);

    template <class K>
    const mapped_type& GetOrDefault(const K& key) const;

    T& AsUnderlying() noexcept;
    const T& AsUnderlying() const noexcept;

    // NB: There are no Save()/Load() since it can be easiliy (de)serialized
    // manually. Moreover, it's easy to forget write compats when replacing
    // THashTable to this one since Save()/Load() works out of the box.
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, const TDefaultMap<T>& map, TStringBuf format);

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TIsDefaultMap
{
    static constexpr bool Value = false;
};

template <class T>
struct TIsDefaultMap<TDefaultMap<T>>
{
    static constexpr bool Value = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define DEFAULT_MAP_INL_H_
#include "default_map-inl.h"
#undef DEFAULT_MAP_INL_H_
