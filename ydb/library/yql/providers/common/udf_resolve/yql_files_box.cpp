#include "yql_files_box.h"

#include <ydb/library/yql/core/file_storage/storage.h>
#include <ydb/library/yql/utils/log/log.h>

#include <util/system/fs.h>
#include <util/system/error.h>
#include <util/system/sysstat.h>
#include <util/folder/dirut.h>

namespace NYql {
namespace NCommon {

TFilesBox::TFilesBox(TFsPath dir, TRandGuid randGuid)
    : Dir(std::move(dir))
    , RandGuid(std::move(randGuid))
{
}

TFilesBox::~TFilesBox() {
    try {
        Destroy();
    } catch (...) {
        YQL_LOG(ERROR) << "Error occurred in files box destroy: " << CurrentExceptionMessage();
    }
}

TString TFilesBox::MakeLinkFrom(const TString& source, const TString& filename) {
    if (!filename) {
        if (auto* existingPath = Mapping.FindPtr(source)) {
            return *existingPath;
        }
    }

    TFsPath sourcePath(source);
    TString sourceAbsolutePath = sourcePath.IsAbsolute() ? source : (TFsPath::Cwd() / sourcePath).GetPath();
    TString path;
    if (filename) {
        path = Dir / filename;
        if (!NFs::SymLink(sourceAbsolutePath, path)) {
            ythrow TSystemError() << "Failed to create symlink for file " << sourceAbsolutePath.Quote() << " to file " << path.Quote();
        }
    } else {
        while (true) {
            path = Dir / RandGuid.GenGuid();
            if (NFs::SymLink(sourceAbsolutePath, path)) {
                break;
            } else if (LastSystemError() != EEXIST) {
                ythrow TSystemError() << "Failed to create symlink for file " << sourceAbsolutePath.Quote() << " to file " << path.Quote();
            }
        }
        Mapping.emplace(source, path);
    }
    return path;
}

TString TFilesBox::GetDir() const {
    return Dir;
}

void TFilesBox::Destroy() {
    Mapping.clear();
    Dir.ForceDelete();
}

THolder<TFilesBox> CreateFilesBox(const TFsPath& baseDir) {
    TRandGuid randGuid;
    TFsPath path = baseDir / randGuid.GenGuid();

    while (true) {
        if (!path.Exists()) {
            int r = Mkdir(path.c_str(), MODE0711);
            if (r == 0) {
                break;
            }
            if (LastSystemError() != EEXIST) {
                ythrow TIoSystemError() << "could not create directory " << path;
            }
        }
        path = baseDir / randGuid.GenGuid();
    }

    return MakeHolder<TFilesBox>(std::move(path), std::move(randGuid));
}

}
}
