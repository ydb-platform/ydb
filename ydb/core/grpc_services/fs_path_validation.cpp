#include "fs_path_validation.h"

#include <util/string/builder.h>
#include <util/folder/pathsplit.h>

namespace NKikimr::NGRpcService {

namespace {

template <typename TTraits>
bool ValidatePathComponentsRaw(const TString& path, const TString& pathDescription, TString& error) {

    for (const auto& component : TPathSplit(path)) {
        if (component == "..") {
            error = TStringBuilder() << pathDescription << " contains path traversal sequence (..)";
            return false;
        }

        if (component == ".") {
            error = TStringBuilder() << pathDescription << " contains current directory reference (.)";
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

} // namespace NKikimr::NGRpcService
