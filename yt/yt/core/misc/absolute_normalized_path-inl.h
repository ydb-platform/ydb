#ifndef ABSOLUTE_NORMALIZED_PATH_INL_H_
#error "Direct inclusion of this file is not allowed, include absolute_normalized_path.h"
#include "absolute_normalized_path.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TSource>
    requires (!std::same_as<
        std::remove_cvref_t<TSource>,
        TAbsoluteNormalizedPath
    >)
TAbsoluteNormalizedPath::TAbsoluteNormalizedPath(TSource&& path)
    : Path_(TryMakeAbsoluteNormalizedPath(std::filesystem::path(std::forward<TSource>(path))))
{ }

template <class TSource>
    requires (!std::same_as<
        std::remove_cvref_t<TSource>,
        TAbsoluteNormalizedPath
    >)
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

template <class TSource>
    requires (std::constructible_from<TAbsoluteNormalizedPath, TSource>)
bool TAbsoluteNormalizedPath::IsAncestorOf(const TSource& descendantPath, bool treatEqualPathAsAncestor) const
{
    TAbsoluteNormalizedPath normalizedDescendantPath(descendantPath);
    if (Path_.string() == "/" && normalizedDescendantPath.Path_.string() != "/") {
        return true;
    }
    if (normalizedDescendantPath.Path().string().starts_with(Path_.string())) {
        if (treatEqualPathAsAncestor && normalizedDescendantPath.Path().string().size() == Path_.string().size()) {
            return true;
        }
        if (normalizedDescendantPath.Path().string().size() > Path_.string().size() &&
            normalizedDescendantPath.Path().string()[Path_.string().size()] == '/') {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

