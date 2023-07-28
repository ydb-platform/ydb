#pragma once

#include <utility>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlying>
class TDefaultMap
    : public TUnderlying
{
public:
    using mapped_type = typename TUnderlying::mapped_type;

    explicit TDefaultMap(mapped_type defaultValue);

    template <class K>
    mapped_type& operator[](const K& key);

private:
    mapped_type DefaultValue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define DEFAULT_MAP_INL_H_
#include "default_map-inl.h"
#undef DEFAULT_MAP_INL_H_
