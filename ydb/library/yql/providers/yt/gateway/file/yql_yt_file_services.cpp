#include "yql_yt_file_services.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <util/system/guard.h>
#include <util/system/fs.h>
#include <util/generic/yexception.h>
#include <util/folder/path.h>
#include <util/string/cast.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/stream/input.h>

#include <algorithm>
#include <iterator>

namespace NYql::NFile {

TYtFileServices::~TYtFileServices() {
    for (auto& x: Locks) {
        try {
            NFs::Remove(x.second);
            NFs::Remove(x.second + ".attr");
        } catch (...) {
        }
    }
}

TString TYtFileServices::GetTablePath(TStringBuf cluster, TStringBuf table, bool isTemp, bool noLocks) {
    if (isTemp) {
        return TString(TFsPath(TmpDir) / TString(table.substr(4)).append(TStringBuf(".tmp")));
    }

    const auto tablePrefix = TString(YtProviderName).append('.').append(cluster);
    const auto fullTableName = TString(tablePrefix).append('.').append(table);
    if (!noLocks) {
        auto guard = Guard(Mutex);
        if (auto p = Locks.FindPtr(fullTableName)) {
            return *p;
        }
    }
    if (auto p = TablesMapping.FindPtr(fullTableName)) {
        return *p;
    }
    if (auto dirPtr = TablesDirMapping.FindPtr(tablePrefix)) {
        return TFsPath(*dirPtr) / TString(table).append(".txt");
    }
    ythrow yexception() << "Table not found: " << cluster << '.' << table;
}

void TYtFileServices::LockPath(const TString& path, const TString& fullTableName) {
    auto name = TFsPath(path).GetName();
    auto lockPath = TFsPath(TmpDir) / (name + ToString(RandomNumber<float>()));
    while (true) {
        try {
            TUnbufferedFileInput src(path);
            TUnbufferedFileOutput dst(TFile(lockPath, CreateNew | WrOnly | Seq));
            TransferData(&src, &dst);
            break;
        } catch (const TFileError& e) {
            lockPath = TFsPath(TmpDir) / (name + ToString(RandomNumber<float>()));
        }
    }
    if (NFs::Exists(path + ".attr")) {
        NFs::Copy(path + ".attr", lockPath.GetPath() + ".attr");
    }
    auto guard = Guard(Mutex);
    if (auto p = Locks.FindPtr(fullTableName)) {
        try {
            NFs::Remove(*p);
            NFs::Remove(*p + ".attr");
        } catch (...) {
        }
    }
    Locks[fullTableName] = lockPath;
}

void TYtFileServices::PushTableContent(const TString& path, const TString& content) {
    auto guard = Guard(Mutex);
    Contents.emplace(path, content);
}

std::vector<TString> TYtFileServices::GetTableContent(const TString& path) {
    auto guard = Guard(Mutex);
    auto range = Contents.equal_range(path);
    std::vector<TString> res;
    std::transform(range.first, range.second, std::back_inserter(res), [](const auto& p) { return p.second; });
    Contents.erase(path);
    return res;
}

} // NYql::NFile
