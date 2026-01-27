#include "rpc_fs_path_validation.h"

#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

namespace {

template <typename TTraits>
bool ValidatePathComponentsRaw(const TString& path, const TString& pathDescription, TString& error) {
    if (path.empty()) {
        return true;
    }

    size_t pos = 0;
    while (pos < path.size()) {
        // Skip separators
        while (pos < path.size() && TTraits::IsPathSep(path[pos])) {
            ++pos;
        }

        if (pos >= path.size()) {
            break;
        }

        // Find component end
        const size_t start = pos;
        while (pos < path.size() && !TTraits::IsPathSep(path[pos])) {
            ++pos;
        }

        const TStringBuf component(path.data() + start, pos - start);

        if (component.empty()) {
            continue;
        }

        if (component == "..") {
            error = TStringBuilder() << pathDescription
                                     << " contains path traversal sequence (..)";
            return false;
        }

        if (component == ".") {
            error = TStringBuilder() << pathDescription
                                     << " contains current directory reference (.)";
            return false;
        }
    }

    return true;
}

} // anonymous namespace

bool ValidateFsPath(const TString& path, const TString& pathDescription, TString& error) {
    if (path.empty()) {
        return true;
    }

    // Check for null bytes
    if (path.find('\0') != TString::npos) {
        error = TStringBuilder() << pathDescription << " contains null byte";
        return false;
    }

#ifdef _win_
    return ValidatePathComponentsRaw<TPathSplitTraitsWindows>(path, pathDescription, error);
#else
    if (path.Contains('\\')) {
        error = TStringBuilder() << pathDescription
                                 << " contains invalid path separator backslash (\\)";
        return false;
    }
    return ValidatePathComponentsRaw<TPathSplitTraitsUnix>(path, pathDescription, error);
#endif
}

} // namespace NGRpcService
} // namespace NKikimr
