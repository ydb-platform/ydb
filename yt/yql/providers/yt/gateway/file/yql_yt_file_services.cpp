#include "yql_yt_file_services.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/core/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>

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

TYtFileServices::TYtFileServices(
    const NKikimr::NMiniKQL::IFunctionRegistry* registry,
    const THashMap<TString, TString>& mapping,
    TFileStoragePtr fileStorage,
    const TString& tmpDir,
    bool keepTempTables,
    const THashMap<TString, TString>& dirMapping
)
    : FunctionRegistry_(registry)
    , TablesMapping_(mapping)
    , TablesDirMapping_(dirMapping)
    , TmpDir_(tmpDir)
    , KeepTempTables_(keepTempTables)
{
    FileStorage_ = fileStorage;
    if (!FileStorage_) {
        TFileStorageConfig params;
        FileStorage_ = CreateFileStorage(params);
    }
}

TYtFileServices::~TYtFileServices() {
    for (auto& x: Snapshots_) {
        try {
            NFs::Remove(x.second);
            NFs::Remove(x.second + ".attr");
        } catch (...) {
        }
    }
}

TString TYtFileServices::GetTmpTablePath(TStringBuf table) {
    YQL_ENSURE(table.SkipPrefix("tmp/"));
    return TString(TFsPath(TmpDir_) / TString(table).append(TStringBuf(".tmp")));
}

TString TYtFileServices::GetTablePath(TStringBuf cluster, TStringBuf table, bool isAnonimous) {
    if (isAnonimous) {
        return GetTmpTablePath(table);
    }
    const auto tablePrefix = TString(YtProviderName).append('.').append(cluster);
    const auto fullTableName = TString(tablePrefix).append('.').append(table);
    if (auto p = TablesMapping_.FindPtr(fullTableName)) {
        return *p;
    }
    if (auto dirPtr = TablesDirMapping_.FindPtr(tablePrefix)) {
        return TFsPath(*dirPtr) / TString(table).append(".txt");
    }
    throw TErrorException(TIssuesIds::YT_TABLE_NOT_FOUND) << "Table \"" << table << "\" does not exist on cluster \"" << cluster << "\"";
}

TString TYtFileServices::GetTableSnapshotPath(TStringBuf cluster, TStringBuf table, bool isAnonimous, ui32 epoch) {
    const auto tablePrefix = TString(YtProviderName).append('.').append(cluster);
    const auto fullTableName = TString(tablePrefix).append('.').append(table);
    with_lock(Mutex_) {
        if (auto p = Snapshots_.FindPtr(std::make_pair(fullTableName, epoch))) {
            return *p;
        }
    }
    return GetTablePath(cluster, table, isAnonimous);
}

TString TYtFileServices::SnapshotTable(const TString& tablePath, TStringBuf cluster, TStringBuf table, ui32 epoch) {
    auto guard = Guard(Mutex_);

    const TString lockKey = TStringBuilder() << YtProviderName << '.' << cluster << '.' << table;
    auto& lockPath = Snapshots_[std::make_pair(lockKey, epoch)];
    if (!lockPath) {
        auto name = TFsPath(tablePath).GetName();
        lockPath = TFsPath(TmpDir_) / (name + ToString(RandomNumber<float>()));
        while (true) {
            try {
                TUnbufferedFileInput src(tablePath);
                TUnbufferedFileOutput dst(TFile(lockPath, CreateNew | WrOnly | Seq));
                TransferData(&src, &dst);
                break;
            } catch (const TFileError& e) {
                lockPath = TFsPath(TmpDir_) / (name + ToString(RandomNumber<float>()));
            }
        }
        if (NFs::Exists(tablePath + ".attr")) {
            NFs::Copy(tablePath + ".attr", lockPath + ".attr");
        }
    }
    return lockPath;
}

void TYtFileServices::PushTableContent(const TString& path, const TString& content) {
    auto guard = Guard(Mutex_);
    Contents_.emplace(path, content);
}

std::vector<TString> TYtFileServices::GetTableContent(const TString& path) {
    auto guard = Guard(Mutex_);
    auto range = Contents_.equal_range(path);
    std::vector<TString> res;
    std::transform(range.first, range.second, std::back_inserter(res), [](const auto& p) { return p.second; });
    Contents_.erase(path);
    return res;
}

} // NYql::NFile
