#include "user_files.h"

#include <ydb/library/yql/providers/yt/common/yql_names.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <util/system/guard.h>
#include <library/cpp/string_utils/url/url.h>


namespace NYql {

TUserFiles::TUserFiles(const TYtUrlMapper& urlMapper, const TString& activeCluster)
    : UrlMapper(urlMapper)
    , ActiveCluster(activeCluster)
{
}

void TUserFiles::AddFile(const TUserDataKey& key, const TUserDataBlock& block) {
    with_lock(Mutex) {
        if (Files.contains(key.Alias())) {
            return;
        }
    }


    TFileInfo userFile;
    userFile.IsUdf = block.Usage.Test(EUserDataBlockUsage::Udf);
    userFile.IsPgExt = block.Usage.Test(EUserDataBlockUsage::PgExt);
    userFile.IsPgCatalog = (key.Alias() == NCommon::PgCatalogFileName);

    if (block.Options.contains("bypass_artifact_cache")) {
        auto option = block.Options.at(TString("bypass_artifact_cache"));
        try {
            userFile.BypassArtifactCache = FromString<bool>(option);
        } catch (const TFromStringException &) {
            YQL_LOG_CTX_THROW yexception() << "FileOption: invalid value for option bypass_artifact_cache: " << option;
        }
    }

    // we can optimize file copy if file resides on the same cluster
    // and provide only link
    TString cluster;
    TString remotePath;
    if ((block.Type == EUserDataType::URL) &&
        UrlMapper.MapYtUrl(block.Data, &cluster, &remotePath) &&
        (cluster == CurrentYtClusterShortcut || cluster == ActiveCluster)) {
        userFile.RemotePath = remotePath;
        userFile.RemoteMemoryFactor = 1.0;
        YQL_CLOG(INFO, Default) << "Using remote file " << userFile.RemotePath.Quote() << " from " << ActiveCluster.Quote();
    } else {
        if (!block.FrozenFile) {
            YQL_LOG_CTX_THROW yexception() << "File with key " << key << " is not frozen";
        }

        userFile.Path = block.FrozenFile;
        userFile.InMemorySize = userFile.Path->GetSize();
    }

    with_lock(Mutex) {
        Files[key.Alias()] = std::move(userFile);
    }
}

bool TUserFiles::HasFilePath(const TString& name) const {
    auto guard = Guard(Mutex);
    return Files.FindPtr(name) != nullptr;
}

TString TUserFiles::GetFilePath(const TString& name) const {
    auto guard = Guard(Mutex);
    auto x = Files.FindPtr(name);
    YQL_ENSURE(x);
    return x->Path->GetPath();
}

bool TUserFiles::FindFolder(const TString& name, TVector<TString>& files) const {
    auto guard = Guard(Mutex);
    auto prefix = TUserDataStorage::MakeFolderName(name);
    for (auto& x : Files) {
        if (x.first.StartsWith(prefix)) {
            files.push_back(x.first);
        }
    }

    return !files.empty();
}

const TUserFiles::TFileInfo* TUserFiles::GetFile(const TString& name) const {
    auto guard = Guard(Mutex);
    return Files.FindPtr(name);
}

THashMap<TString, TUserFiles::TFileInfo> TUserFiles::GetFiles() const {
    auto guard = Guard(Mutex);
    return Files;
}

inline bool TUserFiles::IsEmpty() const {
    auto guard = Guard(Mutex);
    return Files.empty();
}

} // NYql


