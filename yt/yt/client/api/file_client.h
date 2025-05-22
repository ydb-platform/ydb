#pragma once

#include "client_common.h"

#include <yt/yt/core/ypath/public.h>

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct TFileReaderOptions
    : public TTransactionalOptions
    , public TSuppressableAccessTrackingOptions
{
    std::optional<i64> Offset;
    std::optional<i64> Length;
    TFileReaderConfigPtr Config;
};

struct TFileWriterOptions
    : public TTransactionalOptions
    , public TPrerequisiteOptions
{
    bool ComputeMD5 = false;
    TFileWriterConfigPtr Config;
    NConcurrency::IThroughputThrottlerPtr Throttler;
};

struct TGetFileFromCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
};

struct TGetFileFromCacheResult
{
    NYPath::TYPath Path;
};

struct TPutFileToCacheOptions
    : public TTimeoutOptions
    , public TMasterReadOptions
    , public TMutatingOptions
    , public TPrerequisiteOptions
    , public TTransactionalOptions
{
    NYPath::TYPath CachePath;
    bool PreserveExpirationTimeout = false;
    int RetryCount = 10;
};

struct TPutFileToCacheResult
{
    NYPath::TYPath Path;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileClientBase
{
    virtual ~IFileClientBase() = default;

    virtual TFuture<IFileReaderPtr> CreateFileReader(
        const NYPath::TYPath& path,
        const TFileReaderOptions& options = {}) = 0;

    virtual IFileWriterPtr CreateFileWriter(
        const NYPath::TRichYPath& path,
        const TFileWriterOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IFileClient
{
    virtual ~IFileClient() = default;

    virtual TFuture<TGetFileFromCacheResult> GetFileFromCache(
        const TString& md5,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const TString& expectedMD5,
        const TPutFileToCacheOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
