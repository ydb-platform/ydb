#pragma once

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/string/join.h>

namespace NKikimr {

TVector<TString> SplitPath(TString path);
TString JoinPath(const TVector<TString>& path);
TString CanonizePath(const TString &path);
TString CanonizePath(const TVector<TString>& path);
ui32 CanonizedPathLen(const TVector<TString>& path);
TStringBuf ExtractDomain(const TString& path) noexcept;
TStringBuf ExtractDomain(TStringBuf path) noexcept;
TStringBuf ExtractBase(const TString& path) noexcept;
TStringBuf ExtractParent(const TString& path) noexcept;
bool IsEqualPaths(const TString& l, const TString& r) noexcept;
bool IsStartWithSlash(const TString& l);
bool CheckDbPath(const TString &path, const TString &domain, TString &error);
bool IsPathPartContainsOnlyDots(const TString &part);
TString::const_iterator PathPartBrokenAt(const TString &part, const TStringBuf extraSymbols = {});
bool TrySplitPathByDb(const TString& path, const TString& database,
    std::pair<TString, TString>& result, TString& error);

template <typename TIter>
TString CombinePath(TIter begin, TIter end, bool canonize = true) {
    auto path = JoinRange("/", begin, end);
    return canonize
        ? CanonizePath(path)
        : path;
}

inline TVector<TString> ChildPath(const TVector<TString>& parentPath, const TString& childName) {
    auto path = parentPath;
    path.push_back(childName);
    return path;
}

inline TVector<TString> ChildPath(const TVector<TString>& parentPath, const TVector<TString>& childPath) {
    auto path = parentPath;
    for (const auto& childName : childPath) {
        path.push_back(childName);
    }
    return path;
}

}
