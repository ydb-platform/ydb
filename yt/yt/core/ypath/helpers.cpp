#include "helpers.h"

#include "tokenizer.h"

namespace NYT::NYPath {

////////////////////////////////////////////////////////////////////////////////

std::optional<TYPath> TryComputeYPathSuffix(const TYPath& path, const TYPath& prefix)
{
    // TODO(babenko): this check is pessimistic; consider using tokenizer
    if (!path.StartsWith(prefix)) {
        return std::nullopt;
    }

    if (path.length() == prefix.length()) {
        return TYPath();
    }

    if (path[prefix.length()] != '/') {
        return std::nullopt;
    }

    return path.substr(prefix.length());
}

std::pair<TYPath, TString> DirNameAndBaseName(const TYPath& path)
{
    if (path.empty()) {
        return {};
    }
    for (TTokenizer tokenizer(path); tokenizer.GetType() != ETokenType::EndOfStream; tokenizer.Advance()) {
        if (tokenizer.GetSuffix().empty()) {
            const auto& prefix = tokenizer.GetPrefix();
            TYPath dirName;
            if (prefix.ends_with('/')) {
                // Strip trailing '/'.
                dirName = prefix.substr(0, prefix.size() - 1);
            } else {
                dirName = prefix;
            }
            const auto& token = tokenizer.GetToken();
            return {dirName, TString(token)};
        }
    }
    Y_UNREACHABLE();
}

bool IsPathPointingToAttributes(const TYPath& path)
{
    for (TTokenizer tokenizer(path); tokenizer.GetType() != ETokenType::EndOfStream; tokenizer.Advance()) {
        if (tokenizer.GetType() == ETokenType::At) {
            return true;
        }
    }
    return false;
}

TYPath StripAttributes(const TYPath& path)
{
    if (path.empty()) {
        return {};
    }
    for (TTokenizer tokenizer(path); tokenizer.GetType() != ETokenType::EndOfStream; tokenizer.Advance()) {
        if (tokenizer.GetType() == ETokenType::At) {
            const auto& prefix = tokenizer.GetPrefix();
            if (prefix.ends_with('/')) {
                // Strip trailing '/'.
                return TYPath(prefix.substr(0, prefix.size() - 1));
            } else {
                return TYPath(prefix);
            }
        }
    }
    return path;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYPath
