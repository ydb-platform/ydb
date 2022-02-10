#pragma once

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>

namespace NYql {
namespace NUserData {

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
    EType Type_;
    EDisposition Disposition_;
    TString Name_;
    TString Content_;

    static void UserDataToLibraries(
        const TVector<TUserData>& userData,
        THashMap<TString,TString>& libraries
    );

    static void FillFromFolder(
        TFsPath root,
        EType type,
        TVector<TUserData>& userData
    );
};


}
}
