#pragma once

#include <util/folder/path.h>
#include <util/system/tempfile.h>
#include <util/generic/string.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYql {

struct TTempFiles {
    TTempFiles(const TString& tmpDir);

    TString AddFile(const TString& fileName);

    const TFsPath TmpDir;
    TVector<THolder<TTempFile>> Files;
};

} // NYql
