#pragma once

#include "public.h"
#include "versioned_io_options.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/config.h>

#include <yt/yt/library/quantile_digest/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TRetentionConfig
    : public virtual NYTree::TYsonStruct
{
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;
    bool IgnoreMajorTimestamp;

    REGISTER_YSON_STRUCT(TRetentionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

void FormatValue(TStringBuilderBase* builder, const TRetentionConfigPtr& obj, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESamplingMode,
    ((Row)               (1))
    ((Block)             (2))
);

struct TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
    std::optional<ESamplingMode> SamplingMode;
    std::optional<double> SamplingRate;
    std::optional<ui64> SamplingSeed;

    static TChunkReaderConfigPtr GetDefault();

    REGISTER_YSON_STRUCT(TChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterTestingOptions
    : public NYTree::TYsonStruct
{
    //! If true, unsupported chunk feature is added to chunk meta.
    bool AddUnsupportedFeature;

    REGISTER_YSON_STRUCT(TChunkWriterTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterTestingOptions)

////////////////////////////////////////////////////////////////////////////////

struct THashTableChunkIndexWriterConfig
    : public NYTree::TYsonStruct
{
    //! Hash table load factor.
    double LoadFactor;

    //! Final hash table seed will be picked considering this number of rehash trials.
    int RehashTrialCount;

    // TODO(akozhikhov).
    bool EnableGroupReordering;

    //! Unless null, key set will be split to produce multiple hash tables,
    //! each of which corresponds to a single system block and is not greater than #MaxBlockSize.
    std::optional<int> MaxBlockSize;

    REGISTER_YSON_STRUCT(THashTableChunkIndexWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THashTableChunkIndexWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkIndexesWriterConfig
    : public NYTree::TYsonStruct
{
    THashTableChunkIndexWriterConfigPtr HashTable;

    REGISTER_YSON_STRUCT(TChunkIndexesWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkIndexesWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TSlimVersionedWriterConfig
    : public NYTree::TYsonStruct
{
    double TopValueQuantile;
    bool EnablePerValueDictionaryEncoding;

    REGISTER_YSON_STRUCT(TSlimVersionedWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlimVersionedWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
    i64 BlockSize;

    i64 MaxSegmentValueCount;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyWeight;

    //! This limits ensures that chunk index is dense enough
    //! e.g. to produce good slices for reduce.
    i64 MaxDataWeightBetweenBlocks;

    double SampleRate;
    bool UseOriginalDataWeightInSamples;

    bool EnableLargeColumnarStatistics;

    TChunkIndexesWriterConfigPtr ChunkIndexes;

    TSlimVersionedWriterConfigPtr Slim;

    TVersionedRowDigestConfigPtr VersionedRowDigest;

    // Being overwritten by mount config, not registered in TChunkWriterConfig.
    TMinHashDigestConfigPtr MinHashDigest;

    TChunkWriterTestingOptionsPtr TestingOptions;

    TKeyFilterWriterConfigPtr KeyFilter;
    TKeyPrefixFilterWriterConfigPtr KeyPrefixFilter;

    REGISTER_YSON_STRUCT(TChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TKeyFilterWriterConfig
    : public virtual NYTree::TYsonStruct
{
    bool Enable;

    i64 BlockSize;

    int TrialCount;

    std::optional<int> BitsPerKey;
    std::optional<double> FalsePositiveRate;

    static constexpr int DefaultBitsPerKey = 8;
    int EffectiveBitsPerKey;

    REGISTER_YSON_STRUCT(TKeyFilterWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyFilterWriterConfig)

struct TKeyPrefixFilterWriterConfig
    : public TKeyFilterWriterConfig
{
    //! Will produce filters for key prefix of specified lengths.
    THashSet<int> PrefixLengths;

    REGISTER_YSON_STRUCT(TKeyPrefixFilterWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyPrefixFilterWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDictionaryCompressionConfig
    : public virtual NYTree::TYsonStruct
{
    bool Enable;

    //! Idle period after last successful or unsuccessful building iteration.
    TDuration RebuildPeriod;
    TDuration BackoffPeriod;

    //! Desired and maximum number of chunks in tablet to be sampled.
    int DesiredProcessedChunkCount;
    int MaxProcessedChunkCount;

    //! Column-wise desired number and weight of samples to be used when building dictionary.
    //! Sampling is stopped if that number of samples is encountered in all columns.
    //! NB: Sample count excludes null values.
    int DesiredSampleCount;
    i64 DesiredSampledDataWeight;

    //! Column-wise max number and weight of samples to be fetched.
    //! Sampling is stopped if that number of samples is encountered in any column.
    //! NB: Sample count includes null values.
    int MaxProcessedSampleCount;
    i64 MaxProcessedDataWeight;

    //! Limit on total size of fetched blocks.
    i64 MaxFetchedBlocksSize;

    //! Max size of result column dictionary.
    //! Recommended to be ~100 times less than weight of samples for that column.
    i64 ColumnDictionarySize;

    //! Subset of all dictionary building policies.
    //! Will build and apply dictionaries only from this subset.
    //! Upon each chunk compression will independently decide which dictionary fits best.
    THashSet<EDictionaryCompressionPolicy> AppliedPolicies;

    //! Upon each chunk compression will first accumulate samples of that weight
    //! before deciding dictionary of which policy fits the best.
    i64 PolicyProbationSamplesSize;
    //! Upper limit on acceptable compression ratio. No chunk compression is performed if this limit is exceeded.
    double MaxAcceptableCompressionRatio;

    //! For testing purposes only.
    bool ElectRandomPolicy;

    REGISTER_YSON_STRUCT(TDictionaryCompressionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionaryCompressionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDictionaryCompressionSessionConfig
    : public virtual NYTree::TYsonStruct
{
    // Compression session options.

    //! Level of compression algorithm.
    //! Applied to digested compression dictionary upon its construction.
    int CompressionLevel;

    // Decompression session options.

    //! Upper limit on content size of a batch that can be decompressed within a single iteration.
    i64 MaxDecompressionBlobSize;

    REGISTER_YSON_STRUCT(TDictionaryCompressionSessionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionaryCompressionSessionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TBatchHunkReaderConfig
    : public virtual NYTree::TYsonStruct
{
    int MaxHunkCountPerRead;
    i64 MaxTotalHunkLengthPerRead;

    REGISTER_YSON_STRUCT(TBatchHunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchHunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
    , public TBatchHunkReaderConfig
    , public NChunkClient::TChunkFragmentReaderConfig
    , public TDictionaryCompressionSessionConfig
{
    bool SuppressAccessTracking;
    bool SuppressExpirationTimeoutRenewal;
    EUnavailableChunkStrategy UnavailableChunkStrategy;
    NChunkClient::EChunkAvailabilityPolicy ChunkAvailabilityPolicy;
    std::optional<TDuration> MaxReadDuration;

    NTabletClient::TRetryingRemoteDynamicStoreReaderConfigPtr DynamicStoreReader;

    REGISTER_YSON_STRUCT(TTableReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTableReaderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TTableWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTypeConversionConfig
    : public NYTree::TYsonStruct
{
    bool EnableTypeConversion;
    bool EnableStringToAllConversion;
    bool EnableAllToStringConversion;
    bool EnableIntegralTypeConversion;
    bool EnableIntegralToDoubleConversion;

    REGISTER_YSON_STRUCT(TTypeConversionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTypeConversionConfig)

////////////////////////////////////////////////////////////////////////////////

struct TInsertRowsFormatConfig
    : public virtual NYTree::TYsonStruct
{
    bool EnableNullToYsonEntityConversion;

    REGISTER_YSON_STRUCT(TInsertRowsFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInsertRowsFormatConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderOptions
    : public virtual NYTree::TYsonStruct
{
    bool EnableTableIndex;
    bool EnableRangeIndex;
    bool EnableRowIndex;
    bool DynamicTable;
    bool EnableTabletIndex;
    bool EnableKeyWidening;
    bool EnableAnyUnpacking;

    static TChunkReaderOptionsPtr GetDefault();

    REGISTER_YSON_STRUCT(TChunkReaderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

struct TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
    bool ValidateSorted;
    bool ValidateRowWeight;
    bool ValidateKeyWeight;
    bool ValidateDuplicateIds;
    bool ValidateUniqueKeys;
    bool ExplodeOnValidationError;
    bool ValidateColumnCount;
    bool ValidateAnyIsValidYson;
    bool EvaluateComputedColumns;
    bool EnableSkynetSharing;
    bool ReturnBoundaryKeys;
    bool CastAnyToComposite = false;
    bool SingleColumnGroupByDefault = false;
    bool EnableColumnarValueStatistics;
    bool EnableRowCountInColumnarStatistics;
    bool EnableSegmentMetaInBlocks;
    bool EnableColumnMetaInChunkMeta;
    bool ConsiderMinRowRangeDataWeight;

    NYTree::INodePtr CastAnyToCompositeNode;

    ETableSchemaModification SchemaModification;

    TVersionedWriteOptions VersionedWriteOptions;

    EOptimizeFor OptimizeFor;
    std::optional<NChunkClient::EChunkFormat> ChunkFormat;
    NChunkClient::EChunkFormat GetEffectiveChunkFormat(bool versioned) const;

    //! Maximum number of heavy columns in approximate statistics.
    int MaxHeavyColumns;

    std::optional<i64> BlockSize;
    std::optional<i64> BufferSize;

    void EnableValidationOptions(bool validateAnyIsValidYson = false);

    REGISTER_YSON_STRUCT(TChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

struct TVersionedRowDigestConfig
    : public NYTree::TYsonStruct
{
    bool Enable;
    TTDigestConfigPtr TDigest;

    REGISTER_YSON_STRUCT(TVersionedRowDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVersionedRowDigestConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMinHashDigestConfig
    : public NYTree::TYsonStruct
{
    int WriteCount;
    int DeleteTombstoneCount;

    REGISTER_YSON_STRUCT(TMinHashDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMinHashDigestConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRowBatchReadOptions
{
    //! The desired number of rows to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxRowsPerRead = 10000;

    //! The desired data weight to read.
    //! This is just an estimate; not all readers support this limit.
    i64 MaxDataWeightPerRead = 16_MB;

    //! If true then the reader may return a columnar batch.
    //! If false then the reader must return a non-columnar batch.
    bool Columnar = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TSchemalessBufferedDynamicTableWriterConfig
    : public TTableWriterConfig
{
    i64 MaxBatchSize;
    TDuration FlushPeriod;
    TExponentialBackoffOptions RetryBackoff;

    REGISTER_YSON_STRUCT(TSchemalessBufferedDynamicTableWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchemalessBufferedDynamicTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
