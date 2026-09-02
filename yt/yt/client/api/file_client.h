#pragma once

#include "client_common.h"

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/client/signature/public.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

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

//! Byte range of a file to be read as a single partition.
//! Offsets are logical (uncompressed) bytes.
struct TFileReadRange
    : public NYTree::TYsonStructLite
{
    i64 Begin;
    //! If missing, the range extends to the end of file.
    std::optional<i64> End;

    REGISTER_YSON_STRUCT_LITE(TFileReadRange)

    static void Register(TRegistrar registrar);
};

struct TPartitionFileOptions
    : public TTransactionalOptions
    , public TTimeoutOptions
{
    NChunkClient::TFetchChunkSpecConfigPtr FetchChunkSpecConfig;

    //! Whether to include node descriptors in the cookie.
    //! Increases cookie size but likely reduces read latency with ReadFilePartition.
    bool FetchCookieNodeDescriptors = true;
};

struct TReadFilePartitionOptions
    : public TTimeoutOptions
{
    TFileReaderConfigPtr Config;
};

YT_DEFINE_STRONG_TYPEDEF(TFilePartitionCookiePtr, NSignature::TSignaturePtr);

//! File partitioning result that matches the requested range.
struct TFilePartition
{
    //! Cookie that can be fed into CreateFilePartitionReader.
    TFilePartitionCookiePtr Cookie;

    //! Partition length in bytes.
    i64 Length = 0;
};

void Serialize(const TFilePartition& partition, NYson::IYsonConsumer* consumer);

struct TFilePartitions
{
    //! Partitions are listed in the order of the requested ranges, one per range.
    std::vector<TFilePartition> Partitions;
};

void Serialize(const TFilePartitions& partitions, NYson::IYsonConsumer* consumer);

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
        const std::string& md5,
        const TGetFileFromCacheOptions& options = {}) = 0;

    virtual TFuture<TPutFileToCacheResult> PutFileToCache(
        const NYPath::TYPath& path,
        const std::string& expectedMD5,
        const TPutFileToCacheOptions& options = {}) = 0;

    //! Splits a file into partitions according to the given byte ranges,
    //! one partition per range, preserving range order.
    virtual TFuture<TFilePartitions> PartitionFile(
        const NYPath::TYPath& path,
        const std::vector<TFileReadRange>& ranges,
        const TPartitionFileOptions& options = {}) = 0;

    //! Creates a reader for a file partition cookie produced by PartitionFile.
    virtual TFuture<IFileReaderPtr> CreateFilePartitionReader(
        const TFilePartitionCookiePtr& cookie,
        const TReadFilePartitionOptions& options = {}) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
