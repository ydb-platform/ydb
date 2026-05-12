#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NYql::NUserData {

enum class EType {
    LIBRARY,
    FILE,
    UDF
};

enum class EDisposition {
    INLINE,
    RESOURCE,
    RESOURCE_FILE,
    URL,
    FILESYSTEM
};

struct TUserData {
    EType Type;
    EDisposition Disposition;
    TString Name;
    TString Content;

    static void UserDataToLibraries(
        const TVector<TUserData>& userData,
        THashMap<TString, TString>& libraries);

    static void FillFromFolder(
        TFsPath root,
        EType type,
        TVector<TUserData>& userData);
};

} // namespace NYql::NUserData
