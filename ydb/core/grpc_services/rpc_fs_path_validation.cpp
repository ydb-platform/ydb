#include "rpc_fs_path_validation.h"

#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

namespace {

bool ValidatePathComponents(const TString& path, char separator, const TString& pathDescription,
                           const TString& separatorName, TString& error) {
    TVector<TStringBuf> components;
    TStringBuf pathBuf(path);
    while (pathBuf) {
        TStringBuf component = pathBuf.NextTok(separator);
        if (component.empty()) {
            continue;
        }

        // Check for parent directory references
        if (component == "..") {
            error = TStringBuilder() << pathDescription << " contains " << separatorName
                                     << " path traversal sequence (..)";
            return false;
        }

        // Check for current directory references
        if (component == ".") {
            error = TStringBuilder() << pathDescription << " contains " << separatorName
                                     << " current directory reference (.)";
            return false;
        }
    }

    return true;
}

} // anonymous namespace

bool ValidateUnixPath(const TString& path, const TString& pathDescription, TString& error) {
    return ValidatePathComponents(path, '/', pathDescription, "Unix-style", error);
}

bool ValidateWindowsPath(const TString& path, const TString& pathDescription, TString& error) {
    return ValidatePathComponents(path, '\\', pathDescription, "Windows-style", error);
}

bool ValidateFsPath(const TString& path, const TString& pathDescription, TString& error) {
    if (path.empty()) {
        return true;
    }

    // Check for null bytes
    if (path.find('\0') != TString::npos) {
        error = TStringBuilder() << pathDescription << " contains null byte";
        return false;
    }

    // Detect path separator types
    const bool hasUnixSeparator = path.Contains('/');
    const bool hasWindowsSeparator = path.Contains('\\');

    // Reject mixed path separators
    if (hasUnixSeparator && hasWindowsSeparator) {
        error = TStringBuilder() << pathDescription
                                 << " contains mixed path separators (both / and \\)";
        return false;
    }

    if (hasUnixSeparator) {
        if (!ValidateUnixPath(path, pathDescription, error)) {
            return false;
        }
    } else if (hasWindowsSeparator) {
        if (!ValidateWindowsPath(path, pathDescription, error)) {
            return false;
        }
    }

    return true;
}

} // namespace NGRpcService
} // namespace NKikimr
