#pragma once

#include "storage.h"

#include <ydb/library/yql/core/file_storage/defs/downloader.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/uri/http_url.h>

#include <util/folder/path.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <tuple>

namespace NYql {

class TFileStorageConfig;

struct IFileStorage: public TThrRefBase {
    virtual ~IFileStorage() = default;
    virtual void AddDownloader(NYql::NFS::IDownloaderPtr downloader) = 0;
    virtual TFileLinkPtr PutFile(const TString& file, const TString& outFileName = {}) = 0;
    virtual TFileLinkPtr PutFileStripped(const TString& file, const TString& originalMd5 = {}) = 0;
    virtual TFileLinkPtr PutInline(const TString& data) = 0;
    virtual TFileLinkPtr PutUrl(const TString& url, const TString& oauthToken) = 0;
    // async versions
    virtual NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& file, const TString& outFileName = {}) = 0;
    virtual NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& data) = 0;
    virtual NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& url, const TString& oauthToken) = 0;

    virtual TFsPath GetRoot() const = 0;
    virtual TFsPath GetTemp() const = 0;
    virtual const TFileStorageConfig& GetConfig() const = 0;
};

using TFileStoragePtr = TIntrusivePtr<IFileStorage>;

// Will use auto-cleaned temporary directory if storagePath is empty
TFileStoragePtr CreateFileStorage(const TFileStorageConfig& params);

void LoadFsConfigFromFile(TStringBuf path, TFileStorageConfig& params);
void LoadFsConfigFromResource(TStringBuf path, TFileStorageConfig& params);

} // NYql
