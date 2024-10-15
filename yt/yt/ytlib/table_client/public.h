#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TVirtualValueDirectory;

} // namespace NProto

constexpr int DefaultPartitionTag = -1;

// TODO(ifsmirnov): calculate actual estimates.
constexpr i64 DefaultRemoteDynamicStoreReaderMemoryEstimate = 64_MB;

DECLARE_REFCOUNTED_CLASS(TChunkColumnMapping)

struct TColumnIdMapping
{
    int ChunkSchemaIndex;
    int ReaderSchemaIndex;
};

DECLARE_REFCOUNTED_CLASS(TTableSchema)

class TSchemaDictionary;

template <typename TColumnName>
class TGenericColumnFilterDictionary;

using TColumnFilterDictionary = TGenericColumnFilterDictionary<TString>;
using TColumnStableNameFilterDictionary = TGenericColumnFilterDictionary<TColumnStableName>;

class THorizontalBlockReader;

struct THunkChunkRef;
struct THunkChunksInfo;
struct THunkChunkMeta;

struct TTableReadSpec;
struct TFetchSingleTableReadSpecOptions;

struct TBoundaryKeysExtension;
struct TColumnMetaExtension;
struct TKeyColumnsExtension;
struct TSamplesExtension;

DECLARE_REFCOUNTED_STRUCT(TOffloadingReaderOptions)
DECLARE_REFCOUNTED_STRUCT(IOffloadingReader)

DECLARE_REFCOUNTED_CLASS(TSamplesFetcher)

DECLARE_REFCOUNTED_STRUCT(IChunkSliceFetcher)

DECLARE_REFCOUNTED_CLASS(TChunkSliceSizeFetcher)

DECLARE_REFCOUNTED_CLASS(TKeySetWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessChunkWriter)

DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkReader)
DECLARE_REFCOUNTED_STRUCT(ISchemalessMultiChunkWriter)

DECLARE_REFCOUNTED_CLASS(TPartitionChunkReader)
DECLARE_REFCOUNTED_CLASS(TPartitionMultiChunkReader)

DECLARE_REFCOUNTED_STRUCT(IVersionedChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IVersionedMultiChunkWriter)

DECLARE_REFCOUNTED_STRUCT(IHunkChunkPayloadWriter)

DECLARE_REFCOUNTED_STRUCT(ITimingReader)

DECLARE_REFCOUNTED_STRUCT(IPartitioner)

DECLARE_REFCOUNTED_CLASS(TVersionedRowsetReader)

DECLARE_REFCOUNTED_STRUCT(TXorFilterMeta)
DECLARE_REFCOUNTED_CLASS(TColumnarChunkMeta)
DECLARE_REFCOUNTED_CLASS(TCachedVersionedChunkMeta)

DECLARE_REFCOUNTED_CLASS(TColumnarStatisticsFetcher)

DECLARE_REFCOUNTED_STRUCT(TChunkReaderPerformanceCounters)

DECLARE_REFCOUNTED_STRUCT(TChunkLookupHashTable)

DECLARE_REFCOUNTED_STRUCT(TChunkState)

DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)

DECLARE_REFCOUNTED_STRUCT(TVirtualValueDirectory)

DECLARE_REFCOUNTED_STRUCT(IVersionedRowDigestBuilder)

struct TOwningBoundaryKeys;

struct TBlobTableSchema;
class TBlobTableWriter;

struct TChunkTimestamps;

DECLARE_REFCOUNTED_CLASS(TSkynetColumnEvaluator)

DECLARE_REFCOUNTED_CLASS(TCachedBlockMeta)
DECLARE_REFCOUNTED_CLASS(TBlockMetaCache)

DECLARE_REFCOUNTED_CLASS(TTableColumnarStatisticsCache)

class TSchemafulRowMerger;
class TUnversionedRowMerger;
class TSamplingRowMerger;

struct IVersionedRowMerger;

DECLARE_REFCOUNTED_CLASS(TTableWriterOptions)
DECLARE_REFCOUNTED_CLASS(TTableReaderOptions)

DECLARE_REFCOUNTED_CLASS(TBlobTableWriterConfig)
DECLARE_REFCOUNTED_CLASS(TBufferedTableWriterConfig)
DECLARE_REFCOUNTED_CLASS(TPartitionConfig)
DECLARE_REFCOUNTED_CLASS(TTableColumnarStatisticsCacheConfig)
DECLARE_REFCOUNTED_CLASS(THunkChunkPayloadWriterConfig)

DECLARE_REFCOUNTED_STRUCT(IHunkChunkReaderStatistics)
DECLARE_REFCOUNTED_STRUCT(IHunkChunkWriterStatistics)
class THunkChunkReaderCounters;
class THunkChunkWriterCounters;

class TSliceBoundaryKey;

DEFINE_ENUM(ETableCollocationType,
    ((Replication)  (0))
);

DECLARE_REFCOUNTED_STRUCT(IChunkIndexBuilder)

DECLARE_REFCOUNTED_STRUCT(IKeyFilter)
DECLARE_REFCOUNTED_STRUCT(IKeyFilterBuilder)

DECLARE_REFCOUNTED_STRUCT(TKeyFilterStatistics)

constexpr int VersionedBlockValueSize = 16;

constexpr int IndexedRowTypicalGroupCount = 1;

class TIndexedVersionedBlockFormatDetail;

DECLARE_REFCOUNTED_STRUCT(IChunkIndexReadController)

DECLARE_REFCOUNTED_STRUCT(TTabletPerformanceCounters)

DECLARE_REFCOUNTED_STRUCT(IGranuleFilter)

struct TVersionedRowDigest;

DECLARE_REFCOUNTED_STRUCT(IDictionaryCompressionSession)
DECLARE_REFCOUNTED_STRUCT(IDictionaryDecompressionSession)
DECLARE_REFCOUNTED_STRUCT(IDictionaryCompressionFactory)

struct TTimestampReadOptions;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
