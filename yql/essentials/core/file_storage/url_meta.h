#pragma once

#include <util/generic/string.h>

namespace NYql {

struct TUrlMeta {
    TString ETag;
    TString ContentFile;
    TString Md5;
    TString LastModified;

    void SaveTo(const TString& path);
    void TryReadFrom(const TString& path);
};

}
