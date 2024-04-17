#pragma once

#include "public.h"

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/library/quantile_digest/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TRetentionConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int MinDataVersions;
    int MaxDataVersions;
    TDuration MinDataTtl;
    TDuration MaxDataTtl;
    bool IgnoreMajorTimestamp;

    REGISTER_YSON_STRUCT(TRetentionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRetentionConfig)

TString ToString(const TRetentionConfigPtr& obj);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ESamplingMode,
    ((Row)               (1))
    ((Block)             (2))
);

class TChunkReaderConfig
    : public virtual NChunkClient::TBlockFetcherConfig
{
public:
    std::optional<ESamplingMode> SamplingMode;
    std::optional<double> SamplingRate;
    std::optional<ui64> SamplingSeed;

    static TChunkReaderConfigPtr GetDefault();

    REGISTER_YSON_STRUCT(TChunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterTestingOptions
    : public NYTree::TYsonStruct
{
public:
    //! If true, unsupported chunk feature is added to chunk meta.
    bool AddUnsupportedFeature;

    REGISTER_YSON_STRUCT(TChunkWriterTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterTestingOptions)

////////////////////////////////////////////////////////////////////////////////

class THashTableChunkIndexWriterConfig
    : public NYTree::TYsonStruct
{
public:
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

class TChunkIndexesWriterConfig
    : public NYTree::TYsonStruct
{
public:
    THashTableChunkIndexWriterConfigPtr HashTable;

    REGISTER_YSON_STRUCT(TChunkIndexesWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkIndexesWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlimVersionedWriterConfig
    : public NYTree::TYsonStruct
{
public:
    double TopValueQuantile;
    bool EnablePerValueDictionaryEncoding;

    REGISTER_YSON_STRUCT(TSlimVersionedWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlimVersionedWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterConfig
    : public NChunkClient::TEncodingWriterConfig
{
public:
    i64 BlockSize;

    i64 MaxSegmentValueCount;

    i64 MaxBufferSize;

    i64 MaxRowWeight;

    i64 MaxKeyWeight;

    //! This limits ensures that chunk index is dense enough
    //! e.g. to produce good slices for reduce.
    i64 MaxDataWeightBetweenBlocks;

    double SampleRate;

    TChunkIndexesWriterConfigPtr ChunkIndexes;

    TSlimVersionedWriterConfigPtr Slim;

    TVersionedRowDigestConfigPtr VersionedRowDigest;

    TChunkWriterTestingOptionsPtr TestingOptions;

    TKeyFilterWriterConfigPtr KeyFilter;
    TKeyPrefixFilterWriterConfigPtr KeyPrefixFilter;

    REGISTER_YSON_STRUCT(TChunkWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TKeyFilterWriterConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

class TKeyPrefixFilterWriterConfig
    : public TKeyFilterWriterConfig
{
public:
    //! Will produce filters for key prefix of specified lengths.
    THashSet<int> PrefixLengths;

    REGISTER_YSON_STRUCT(TKeyPrefixFilterWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKeyPrefixFilterWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TDictionaryCompressionConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

    //! Level of compression algorithm.
    //! Applied to digested compression dictionary upon its construction.
    int CompressionLevel;

    //! Subset of all dictionary building policies.
    //! Will build and apply dictionaries only from this subset.
    //! Upon each chunk compression will independently decide which dictionary fits best.
    THashSet<EDictionaryCompressionPolicy> AppliedPolicies;

    //! Upon each chunk compression will first accumulate samples of that weight
    //! before deciding dictionary of which policy fits the best.
    i64 PolicyProbationSamplesSize;
    //! Upper limit on acceptable compression ratio. No chunk compression is performed if this limit is exceeded.
    double MaxAcceptableCompressionRatio;

    REGISTER_YSON_STRUCT(TDictionaryCompressionConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDictionaryCompressionConfig)

////////////////////////////////////////////////////////////////////////////////

class TDictionaryCompressionSessionConfig
    : public virtual NYTree::TYsonStruct
{
public:
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

class TBatchHunkReaderConfig
    : public virtual NYTree::TYsonStruct
{
public:
    int MaxHunkCountPerRead;
    i64 MaxTotalHunkLengthPerRead;

    REGISTER_YSON_STRUCT(TBatchHunkReaderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBatchHunkReaderConfig)

///////////////////////////////////////////////////////////////////////////////

class TTableReaderConfig
    : public virtual NChunkClient::TMultiChunkReaderConfig
    , public virtual TChunkReaderConfig
    , public TBatchHunkReaderConfig
    , public NChunkClient::TChunkFragmentReaderConfig
    , public TDictionaryCompressionSessionConfig
{
public:
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

class TTableWriterConfig
    : public TChunkWriterConfig
    , public NChunkClient::TMultiChunkWriterConfig
{
    REGISTER_YSON_STRUCT(TTableWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TTableWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TTypeConversionConfig
    : public NYTree::TYsonStruct
{
public:
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

class TInsertRowsFormatConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool EnableNullToYsonEntityConversion;

    REGISTER_YSON_STRUCT(TInsertRowsFormatConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TInsertRowsFormatConfig)

////////////////////////////////////////////////////////////////////////////////

class TChunkReaderOptions
    : public virtual NYTree::TYsonStruct
{
public:
    bool EnableTableIndex;
    bool EnableRangeIndex;
    bool EnableRowIndex;
    bool DynamicTable;
    bool EnableTabletIndex;
    bool EnableKeyWidening;

    static TChunkReaderOptionsPtr GetDefault();

    REGISTER_YSON_STRUCT(TChunkReaderOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkReaderOptions)

////////////////////////////////////////////////////////////////////////////////

class TChunkWriterOptions
    : public virtual NChunkClient::TEncodingWriterOptions
{
public:
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

    NYTree::INodePtr CastAnyToCompositeNode;

    ETableSchemaModification SchemaModification;

    EOptimizeFor OptimizeFor;
    std::optional<NChunkClient::EChunkFormat> ChunkFormat;
    NChunkClient::EChunkFormat GetEffectiveChunkFormat(bool versioned) const;

    //! Maximum number of heavy columns in approximate statistics.
    int MaxHeavyColumns;

    void EnableValidationOptions(bool validateAnyIsValidYson = false);

    REGISTER_YSON_STRUCT(TChunkWriterOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkWriterOptions)

////////////////////////////////////////////////////////////////////////////////

class TVersionedRowDigestConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    TTDigestConfigPtr TDigest;

    REGISTER_YSON_STRUCT(TVersionedRowDigestConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVersionedRowDigestConfig)

///////////////////////////////////////////////////////////////////////////////

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

} // namespace NYT::NTableClient
