#pragma once

#include "file_storage.h"

namespace NYql {

class TFileStorageDecorator: public IFileStorage {
public:
    TFileStorageDecorator(TFileStoragePtr fs);
    ~TFileStorageDecorator() = default;

    TFileLinkPtr PutFile(const TString& file, const TString& outFileName) override;
    TFileLinkPtr PutFileStripped(const TString& file, const TString& originalMd5) override;
    TFileLinkPtr PutInline(const TString& data) override;
    TFileLinkPtr PutUrl(const TString& url, const TString& token) override;
    NThreading::TFuture<TFileLinkPtr> PutFileAsync(const TString& file, const TString& outFileName) override;
    NThreading::TFuture<TFileLinkPtr> PutInlineAsync(const TString& data) override;
    NThreading::TFuture<TFileLinkPtr> PutUrlAsync(const TString& url, const TString& token) override;
    TFsPath GetRoot() const override;
    TFsPath GetTemp() const override;
    const TFileStorageConfig& GetConfig() const override;

protected:
    TFileStoragePtr Inner_;
};

} // NYql
