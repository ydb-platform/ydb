#include "fs_mock.h"

#include <util/folder/path.h>
#include <util/stream/file.h>

#include <functional>

namespace NKikimr::NWrappers::NTestHelpers {

TFsMock::TFsMock(const TString& basePath)
    : BasePath_(basePath)
{
}

void TFsMock::Refresh() {
    Data_.clear();

    TFsPath baseDir(BasePath_);
    if (!baseDir.Exists()) {
        return;
    }

    std::function<void(const TFsPath&)> scan = [&](const TFsPath& dir) {
        TVector<TString> children;
        dir.ListNames(children);
        for (const auto& child : children) {
            TFsPath childPath = dir / child;
            if (childPath.IsDirectory()) {
                scan(childPath);
            } else if (childPath.IsFile()) {
                TString fullPath = childPath.GetPath();
                TString relativePath = fullPath.substr(BasePath_.size());
                if (relativePath.StartsWith("/")) {
                    relativePath = relativePath.substr(1);
                }
                TFileInput file(fullPath);
                Data_[relativePath] = file.ReadAll();
            }
        }
    };

    scan(baseDir);
}

const THashMap<TString, TString>& TFsMock::GetData() const {
    return Data_;
}

THashMap<TString, TString>& TFsMock::GetData() {
    return Data_;
}

const TString& TFsMock::GetBasePath() const {
    return BasePath_;
}

} // namespace NKikimr::NWrappers::NTestHelpers
