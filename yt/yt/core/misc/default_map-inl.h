#ifndef DEFAULT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include default_map.h"
// For the sake of sane code completion.
#include "default_map.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TDefaultMap<T>::TDefaultMap(mapped_type defaultValue)
    : DefaultValue_(std::move(defaultValue))
{ }

template <class T>
template <class K>
typename TDefaultMap<T>::mapped_type& TDefaultMap<T>::operator[](const K& key)
{
    if (auto it = T::find(key); it != T::end()) {
        return it->second;
    }
    return T::insert({key, DefaultValue_}).first->second;
}

template <class T>
template <class K>
const typename TDefaultMap<T>::mapped_type& TDefaultMap<T>::GetOrDefault(const K& key) const
{
    auto it = T::find(key);
    return it == T::end() ? DefaultValue_ : it->second;
}

template <class T>
T& TDefaultMap<T>::AsUnderlying() noexcept
{
    return static_cast<T&>(*this);
}

template <class T>
const T& TDefaultMap<T>::AsUnderlying() const noexcept
{
    return static_cast<const T&>(*this);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, const TDefaultMap<T>& map, TStringBuf format)
{
    FormatValue(builder, static_cast<const T&>(map), format);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
