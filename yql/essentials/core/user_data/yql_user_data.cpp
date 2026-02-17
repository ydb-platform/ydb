#include "yql_user_data.h"
#include <util/folder/iterator.h>

namespace NYql::NUserData {

void TUserData::UserDataToLibraries(
    const TVector<TUserData>& userData,
    THashMap<TString, TString>& modules) {
    for (const TUserData& item : userData) {
        if (item.Type == EType::LIBRARY) {
            if (item.Disposition == EDisposition::RESOURCE) { // TODO: support other disposition options
                modules[to_lower(item.Name)] = item.Content;
            } else if (item.Disposition == EDisposition::RESOURCE_FILE) {
                modules[to_lower(item.Name)] = item.Name;
            }
        }
    }
    modules["core"] = "/lib/yql/core.yql";
}

void TUserData::FillFromFolder(
    TFsPath root,
    EType type,
    TVector<TUserData>& userData) {
    if (!root.Exists()) {
        return;
    }
    root = root.RealPath();
    TDirIterator dir(root, TDirIterator::TOptions(FTS_LOGICAL));
    for (auto file = dir.begin(), end = dir.end(); file != end; ++file) {
        if (file->fts_level == FTS_ROOTLEVEL) {
            continue;
        }
        TFsPath filePath(file->fts_path);
        userData.push_back({type, EDisposition::FILESYSTEM, filePath.RelativeTo(root), filePath});
    }
}

} // namespace NYql::NUserData
