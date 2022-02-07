#pragma once

#include "storage.h"

#include <library/cpp/threading/future/future.h>

#include <util/folder/path.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYql {

struct IFileStorage: public TThrRefBase {
    virtual ~IFileStorage() = default;
    virtual void AddAllowedUrlPattern(const TString& pattern) = 0;
    virtual TFileLinkPtr PutFile(const TString& file, const TString& outFileName = {}) = 0;
    virtual TFileLinkPtr PutFileStripped(const TString& file, const TString& originalMd5 = {}) = 0;
    virtual TFileLinkPtr PutInline(const TString& data) = 0;
    virtual TFileLinkPtr PutUrl(const TString& url, const TString& oauthToken) = 0;
    virtual void SetExternalUser(bool isExternal) = 0;
    // async versions
    virtual NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& file, const TString& outFileName = {}) = 0;
    virtual NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& data) = 0;
    virtual NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& url, const TString& oauthToken) = 0;

    virtual TFsPath GetRoot() const = 0;
    virtual TFsPath GetTemp() const = 0;
};

using TFileStoragePtr = TIntrusivePtr<IFileStorage>;

class TFileStorageConfig;

// Will use auto-cleaned temporary directory if storagePath is empty
TFileStoragePtr CreateFileStorage(const TFileStorageConfig& params);

} // NYql
