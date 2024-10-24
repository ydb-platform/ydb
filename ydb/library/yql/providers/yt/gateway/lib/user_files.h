#pragma once

#include <ydb/library/yql/providers/yt/lib/url_mapper/yql_yt_url_mapper.h>
#include <ydb/library/yql/core/file_storage/storage.h>
#include <ydb/library/yql/core/type_ann/type_ann_core.h>
#include <ydb/library/yql/core/yql_user_data.h>

#include <util/system/mutex.h>
#include <util/generic/ptr.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

class IThreadPool;

namespace NYql {

class TYtGatewayConfig;

class TUserFiles: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TUserFiles>;

    struct TFileInfo {
        TFileLinkPtr Path; // Real path in storage
        bool IsUdf = false;
        bool IsPgExt = false;
        bool IsPgCatalog = false;
        ui64 InMemorySize = 0;
        TString RemotePath;
        double RemoteMemoryFactor = 0.;
        bool BypassArtifactCache = false;
    };

public:
    TUserFiles(const TYtUrlMapper& urlMapper, const TString& activeCluster);

    void AddFile(const TUserDataKey& key, const TUserDataBlock& block);

    bool HasFilePath(const TString& name) const;
    bool FindFolder(const TString& name, TVector<TString>& files) const;
    TString GetFilePath(const TString& name) const;
    const TFileInfo* GetFile(const TString& name) const;
    THashMap<TString, TFileInfo> GetFiles() const;
    bool IsEmpty() const;

private:
    const TYtUrlMapper& UrlMapper;
    const TString ActiveCluster;
    THashMap<TString, TFileInfo> Files;
    TMutex Mutex;
};

} // NYql
