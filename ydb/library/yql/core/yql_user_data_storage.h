#pragma once

#include "yql_holding_file_storage.h"
#include "yql_udf_index.h"
#include "yql_udf_resolver.h"
#include "yql_user_data.h"

#include <ydb/library/yql/core/url_preprocessing/interface/url_preprocessing.h>
#include <ydb/library/yql/protos/yql_mount.pb.h>

#include <util/generic/maybe.h>

namespace NYql {

class TUserDataStorage : public TThrRefBase {
public:
    typedef TIntrusivePtr<TUserDataStorage> TPtr;

public:
    TUserDataStorage(TFileStoragePtr fileStorage, TUserDataTable data, IUdfResolver::TPtr udfResolver, TUdfIndex::TPtr udfIndex);
    void SetTokenResolver(TTokenResolver tokenResolver);
    void SetUrlPreprocessor(IUrlPreprocessing::TPtr urlPreprocessing);
    void SetUserDataTable(TUserDataTable data);

    void AddUserDataBlock(const TStringBuf& name, const TUserDataBlock& block);
    void AddUserDataBlock(const TUserDataKey& key, const TUserDataBlock& block);

    bool ContainsUserDataBlock(const TStringBuf& name) const;
    bool ContainsUserDataBlock(const TUserDataKey& key) const;
    TUserDataBlock& GetUserDataBlock(const TUserDataKey& key);
    TUserDataBlock* FindUserDataBlock(const TStringBuf& name);
    const TUserDataBlock* FindUserDataBlock(const TUserDataKey& key) const;
    TUserDataBlock* FindUserDataBlock(const TUserDataKey& key);

    static const TUserDataBlock* FindUserDataBlock(const TUserDataTable& userData, const TStringBuf& name);
    static const TUserDataBlock* FindUserDataBlock(const TUserDataTable& userData, const TUserDataKey& key);

    static TUserDataBlock* FindUserDataBlock(TUserDataTable& userData, const TStringBuf& name);
    static TUserDataBlock* FindUserDataBlock(TUserDataTable& userData, const TUserDataKey& key);

    bool ContainsUserDataFolder(const TStringBuf& name) const;
    TMaybe<std::map<TUserDataKey, const TUserDataBlock*>> FindUserDataFolder(const TStringBuf& name, ui32 maxFileCount = ~0u) const;
    static TMaybe<std::map<TUserDataKey, const TUserDataBlock*>> FindUserDataFolder(const TUserDataTable& userData, const TStringBuf& name, ui32 maxFileCount = ~0u);

    void FillUserDataUrls();
    std::map<TString, const TUserDataBlock*> GetDirectoryContent(const TStringBuf& path, ui32 maxFileCount = ~0u) const;
    static TString MakeFullName(const TStringBuf& name);
    static TString MakeFolderName(const TStringBuf& name);
    static TUserDataKey ComposeUserDataKey(const TStringBuf& name);
    static TString MakeRelativeName(const TStringBuf& name);
    TVector<TString> GetLibraries() const;

    // working with frozen files
    // download file and fill FrozenFile property of data block
    // returned pointer is valid until storage destruction, but keep in mind storage is used in multi-threading env and data could be changed
    // todo: consider more robust approach for multi-threading
    // there is a guarantee we return the same TFileLinkPtr for provided key if this method get called multiple times from different threads
    // program should work with single version of downloaded url when block type is URL
    // method should support parallel file downloading for different keys
    TUserDataBlock& Freeze(const TUserDataKey& key);
    TUserDataBlock* FreezeNoThrow(const TUserDataKey& key, TString& errorMessage);

    // as above + udf will be scanned and meta info put into UdfIndex
    TUserDataBlock* FreezeUdfNoThrow(const TUserDataKey& key, TString& errorMessage, const TString& customUdfPrefix = {});

    // returns function which will register value in cache after invocation
    NThreading::TFuture<std::function<TUserDataBlock()>> FreezeAsync(const TUserDataKey& key);

private:
    void TryFillUserDataUrl(TUserDataBlock& block) const;
    TUserDataBlock& RegisterLink(const TUserDataKey& key, TFileLinkPtr link);

private:
    THoldingFileStorage FileStorage_;
    TUserDataTable UserData_;
    IUdfResolver::TPtr UdfResolver_;
    TUdfIndex::TPtr UdfIndex_;
    TTokenResolver TokenResolver_;
    IUrlPreprocessing::TPtr UrlPreprocessing_;

    THashSet<TUserDataKey, TUserDataKey::THash, TUserDataKey::TEqualTo> ScannedUdfs_;
    std::function<void(const TUserDataBlock& block)> ScanUdfStrategy_;
};

const TString& GetDefaultFilePrefix();
void FillUserDataTableFromFileSystem(const NYqlMountConfig::TMountConfig& mount, TUserDataTable& userData);

/*
Start async freeze procedure for every not frozen element of the table (if FrozenFile is nullptr or if urlDownloadFilter returns true for url)
Frozen file will be cached under data storage.
Every key of not frozen block in input table should exist in data storage. No such requirements for other keys.
Usage of input blocks will be preserved.
*/
NThreading::TFuture<std::function<TUserDataTable()>> FreezeUserDataTableIfNeeded(TUserDataStorage::TPtr userDataStorage, const TUserDataTable& files, const std::function<bool(const TString&)>& urlDownloadFilter);
} // namespace NYql
