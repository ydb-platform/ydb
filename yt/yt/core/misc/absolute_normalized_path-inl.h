#ifndef ABSOLUTE_NORMALIZED_PATH_INL_H_
#error "Direct inclusion of this file is not allowed, include absolute_normalized_path.h"
#include "absolute_normalized_path.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TSource>
TAbsoluteNormalizedPath::TAbsoluteNormalizedPath(TSource&& path)
    : Path_(TryMakeAbsoluteNormalizedPath(std::filesystem::path(std::forward<TSource>(path))))
{ }

template <class TSource>
TAbsoluteNormalizedPath& TAbsoluteNormalizedPath::operator=(TSource&& path)
{
    Path_ = TryMakeAbsoluteNormalizedPath(std::filesystem::path(std::forward<TSource>(path)));
    return *this;
}

template <class TSource>
TAbsoluteNormalizedPath& TAbsoluteNormalizedPath::operator/=(const TSource& path)
{
    Path_ = TryMakeAbsoluteNormalizedPath(Path_ / path);
    return *this;
}

template <class TSource>
TAbsoluteNormalizedPath TAbsoluteNormalizedPath::operator/(const TSource& path)
{
    return TAbsoluteNormalizedPath(Path_ / path);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

