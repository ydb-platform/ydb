#ifndef CUBE_INL_H_
#error "Direct inclusion of this file is not allowed, include cube.h"
// For the sake of sane code completion.
#include "cube.h"
#endif

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class T>
template <class TValue>
size_t TCube<T>::TProjectionTagIdsHash::operator()(const TValue& value) const
{
    return THash<TTagIdList>()(GetTagIds(value));
}

template <class T>
template <class TLhs, class TRhs>
bool TCube<T>::TProjectionTagIdsEqual::operator()(const TLhs& lhs, const TRhs& rhs) const
{
    return GetTagIds(lhs) == GetTagIds(rhs);
}

template <class T>
const TTagIdList& TCube<T>::GetTagIds(const TTagIdList& tagIds)
{
    return tagIds;
}

template <class T>
const TTagIdList& TCube<T>::GetTagIds(const TProjectionPtr& projection)
{
    return projection->TagIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
