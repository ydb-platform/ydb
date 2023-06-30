#include "temp_files.h"

#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

TTempFiles::TTempFiles(const TString& tmpDir)
    : TmpDir(tmpDir)
{
}

TString TTempFiles::AddFile(const TString& fileName) {
    TFsPath filePath = TmpDir / fileName;
    YQL_ENSURE(!filePath.Exists(), "Twice usage of the " << fileName << " temp file");
    Files.emplace_back(MakeHolder<TTempFile>(filePath));
    return filePath;
}

} // NYql
