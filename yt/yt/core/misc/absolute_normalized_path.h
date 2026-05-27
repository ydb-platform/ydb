#pragma once

#include <util/generic/hash.h>

#include <filesystem>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TAbsoluteNormalizedPath
{
public:
    TAbsoluteNormalizedPath() noexcept = default;
    explicit TAbsoluteNormalizedPath(const TAbsoluteNormalizedPath& path) = default;
    explicit TAbsoluteNormalizedPath(TAbsoluteNormalizedPath&& path) noexcept = default;

    template <class TSource>
    explicit TAbsoluteNormalizedPath(TSource&& path);

    TAbsoluteNormalizedPath& operator=(const TAbsoluteNormalizedPath& path) = default;
    TAbsoluteNormalizedPath& operator=(TAbsoluteNormalizedPath&& path) noexcept = default;

    template <class TSource>
    TAbsoluteNormalizedPath& operator=(TSource&& path);

    void Clear() noexcept;

    void Swap(TAbsoluteNormalizedPath& path) noexcept;

    const std::filesystem::path& Path() const noexcept;

    template <class TSource>
    TAbsoluteNormalizedPath& operator/=(const TSource& path);

    template <class TSource>
    TAbsoluteNormalizedPath operator/(const TSource& path);

    auto operator<=>(const TAbsoluteNormalizedPath& path) const = default;

private:
    std::filesystem::path Path_;

    void ValidateDepthPath(const std::filesystem::path& path) const;
    std::filesystem::path TryMakeAbsoluteNormalizedPath(const std::filesystem::path& path) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

template <>
struct THash<NYT::TAbsoluteNormalizedPath>
{
    size_t operator()(const NYT::TAbsoluteNormalizedPath& value) const;
};

#define ABSOLUTE_NORMALIZED_PATH_INL_H_
#include "absolute_normalized_path-inl.h"
#undef ABSOLUTE_NORMALIZED_PATH_INL_H_
