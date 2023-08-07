#ifndef DEFAULT_MAP_INL_H_
#error "Direct inclusion of this file is not allowed, include default_map.h"
// For the sake of sane code completion.
#include "default_map.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
TDefaultMap<TUnderlying>::TDefaultMap(mapped_type defaultValue)
    : DefaultValue_(std::move(defaultValue))
{ }

template <class TUnderlying>
template <class K>
typename TDefaultMap<TUnderlying>::mapped_type& TDefaultMap<TUnderlying>::operator[](const K& key)
{
    if (auto it = TUnderlying::find(key); it != TUnderlying::end()) {
        return it->second;
    }
    return TUnderlying::insert({key, DefaultValue_}).first->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
