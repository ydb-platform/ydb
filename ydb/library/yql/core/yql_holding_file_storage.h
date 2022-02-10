#pragma once

#include <ydb/library/yql/core/yql_user_data.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <util/generic/map.h>

namespace NYql {

class THoldingFileStorage {
private:
    struct TDataLess {
        bool operator()(const TUserDataBlock& b1, const TUserDataBlock& b2) const {
            return std::tie(b1.Type, b1.Data, b1.UrlToken) < std::tie(b2.Type, b2.Data, b2.UrlToken);
        }
    };

public:
    explicit THoldingFileStorage(TFileStoragePtr fileStorage);
    TFileStoragePtr GetRawStorage() const;
    bool CanFreeze() const;
    // it is safe to call this method from multiple threads
    // method will return the same link for single path even in multi-threaded env
    // if we have single url but with different tokens we can potentially download several different versions of url
    TFileLinkPtr FreezeFile(const TUserDataBlock& block);

    // downloaded file will be registered in cache after function invocation
    NThreading::TFuture<std::function<TFileLinkPtr()>> FreezeFileAsync(const TUserDataBlock& block);

private:
    TFileLinkPtr PutDataAndRegister(const TUserDataBlock& block);
    TFileLinkPtr PutData(const TUserDataBlock& block);
    TFileLinkPtr FindOrPutData(const TUserDataBlock& block);

    NThreading::TFuture<std::function<TFileLinkPtr()>> PutDataAndRegisterAsync(const TUserDataBlock& block);
    NThreading::TFuture<TFileLinkPtr> PutDataAsync(const TUserDataBlock& block);
    NThreading::TFuture<std::function<TFileLinkPtr()>> FindOrPutDataAsync(const TUserDataBlock& block);

    TFileLinkPtr RegisterLink(const TUserDataBlock& block, TFileLinkPtr link);

private:
    TFileStoragePtr FileStorage_;
    TMap<TUserDataBlock, TFileLinkPtr, TDataLess> Links_;
};

}
