#include "absolute_normalized_path.h"

#include <library/cpp/yt/error/error.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TAbsoluteNormalizedPath::Clear() noexcept
{
    Path_.clear();
}

void TAbsoluteNormalizedPath::Swap(TAbsoluteNormalizedPath& path) noexcept
{
    Path_.swap(path.Path_);
}

const std::filesystem::path& TAbsoluteNormalizedPath::Path() const noexcept
{
    return Path_;
}

void TAbsoluteNormalizedPath::ValidateDepthPath(const std::filesystem::path& path) const
{
    int currentDepth = 0;
    for (const auto& part : path) {
        if (part == path.root_name() || part == path.root_directory()) {
            continue;
        }
        if (part == "..") {
            --currentDepth;
            if (currentDepth < 0) {
                THROW_ERROR_EXCEPTION("Path escapes root");
            }
        } else if (part == ".") {
            // Do nothing.
        } else {
            ++currentDepth;
        }
    }
}

std::filesystem::path TAbsoluteNormalizedPath::TryMakeAbsoluteNormalizedPath(const std::filesystem::path& path) const
{
    if (path.empty()) {
        return path;
    }

    if (!path.is_absolute()) {
        THROW_ERROR_EXCEPTION("Path isn't absolute");
    }

    ValidateDepthPath(path);

    auto result = path.lexically_normal();

    if (result.filename().empty() && path != "/") {
        result = result.parent_path();
    }

    for (const auto& filename : result) {
        YT_VERIFY(filename != ".." && filename != ".");
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

size_t THash<NYT::TAbsoluteNormalizedPath>::operator()(const NYT::TAbsoluteNormalizedPath& value) const
{
    return std::hash<std::decay_t<decltype(value.Path().native())>>()(value.Path().native());
}

////////////////////////////////////////////////////////////////////////////////
