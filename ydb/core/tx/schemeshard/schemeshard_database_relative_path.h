#pragma once

#include <ydb/library/conclusion/result.h>

#include <util/generic/string.h>

namespace NKikimr::NSchemeShard {

// A canonical absolute path relative to a database root, as used in Schema CDC payloads.
// For database root "/Root/Database" the schemeshard path "/Root/Database/dir/table"
// has the value "/dir/table", and the database root itself has the value "/".
class TDatabaseRelativePath {
public:
    // Both arguments are absolute schemeshard paths, e.g. "/Root/Database", "/Root/Database/dir/table".
    static TConclusion<TDatabaseRelativePath> FromAbsolute(const TString& databaseRoot, const TString& absolutePath);

    // Joins workingDir with name, canonizes the result and relativizes it.
    // The resulting path is not required to exist.
    static TConclusion<TDatabaseRelativePath> FromWorkingDirAndName(const TString& databaseRoot, const TString& workingDir, const TString& name);

    // Always has a leading '/'; the database root is exactly "/".
    TStringBuf Value() const {
        return Path;
    }

    bool IsRoot() const {
        return Path == "/";
    }

private:
    explicit TDatabaseRelativePath(TString v)
        : Path(std::move(v))
    {}

private:
    TString Path;
};

}
