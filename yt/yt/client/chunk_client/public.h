#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>
#include <library/cpp/yt/small_containers/compact_flat_map.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TChunkInfo;
class TChunkSpec;
class TChunkMeta;
class TBlocksExt;
class TMiscExt;

class TConfirmChunkReplicaInfo;

class TDataStatistics;

class TLegacyReadRange;

class TMediumDirectory;

} // namespace NProto

YT_DEFINE_ERROR_ENUM(
    ((AllTargetNodesFailed)                  (700))
    ((SendBlocksFailed)                      (701))
    ((NoSuchSession)                         (702))
    ((SessionAlreadyExists)                  (703))
    ((ChunkAlreadyExists)                    (704))
    ((WindowError)                           (705))
    ((BlockContentMismatch)                  (706))
    ((NoSuchBlock)                           (707))
    ((NoSuchChunk)                           (708))
    ((NoLocationAvailable)                   (710))
    ((IOError)                               (711))
    ((MasterCommunicationFailed)             (712))
    ((NoSuchChunkTree)                       (713))
    ((NoSuchChunkList)                       (717))
    ((MasterNotConnected)                    (714))
    ((ChunkUnavailable)                      (716))
    ((WriteThrottlingActive)                 (718))
    ((NoSuchMedium)                          (719))
    ((OptimisticLockFailure)                 (720))
    ((InvalidBlockChecksum)                  (721))
    ((MalformedReadRequest)                  (722))
    ((MissingExtension)                      (724))
    ((ReaderThrottlingFailed)                (725))
    ((ReaderTimeout)                         (726))
    ((NoSuchChunkView)                       (727))
    ((IncorrectChunkFileChecksum)            (728))
    ((BrokenChunkFileMeta)                   (729))
    ((IncorrectLayerFileSize)                (730))
    ((NoSpaceLeftOnDevice)                   (731))
    ((ConcurrentChunkUpdate)                 (732))
    ((InvalidInputChunk)                     (733))
    ((UnsupportedChunkFeature)               (734))
    ((IncompatibleChunkMetas)                (735))
    ((AutoRepairFailed)                      (736))
    ((ChunkBlockFetchFailed)                 (737))
    ((ChunkMetaFetchFailed)                  (738))
    ((RowsLookupFailed)                      (739))
    ((BlockChecksumMismatch)                 (740))
    ((NoChunkSeedsKnown)                     (741))
    ((NoChunkSeedsGiven)                     (742))
    ((ChunkIsLost)                           (743))
    ((ChunkReadSessionSlow)                  (744))
    ((NodeProbeFailed)                       (745))
    ((UnrecoverableRepairError)              (747))
    ((MissingJournalChunkRecord)             (748))
    ((LocationDiskFailed)                    (749))
    ((LocationCrashed)                       (750))
    ((LocationDiskWaitingReplacement)        (751))
    ((ChunkMetaCacheFetchFailed)             (752))
    ((LocationMediumIsMisconfigured)         (753))
    ((DiskIdsMismatched)                     (754))
    ((LocationDisabled)                      (755))
    ((DiskFailed)                            (756))
    ((DiskWaitingReplacement)                (757))
    ((LockFileIsFound)                       (758))
    ((DiskHealthCheckFailed)                 (759))
    ((TooManyChunksToFetch)                  (760))
);

using TChunkId = NObjectClient::TObjectId;
extern const TChunkId NullChunkId;

using TChunkViewId = NObjectClient::TObjectId;
extern const TChunkViewId NullChunkViewId;

using TChunkListId = NObjectClient::TObjectId;
extern const TChunkListId NullChunkListId;

using TChunkTreeId = NObjectClient::TObjectId;
extern const TChunkTreeId NullChunkTreeId;

using TChunkLocationUuid = TGuid;
constexpr auto EmptyChunkLocationUuid = TChunkLocationUuid(0, 0);
constexpr auto InvalidChunkLocationUuid = TChunkLocationUuid(-1, -1);

constexpr int MinReplicationFactor = 1;
constexpr int MaxReplicationFactor = 20;
constexpr int DefaultReplicationFactor = 3;

constexpr int MaxMediumCount = 120; // leave some room for sentinels

template <typename T>
using TMediumMap = THashMap<int, T>;
template <typename T>
using TCompactMediumMap = TCompactFlatMap<int, T, 4>;

//! Used as an expected upper bound in TCompactVector.
/*
 *  Maximum regular number of replicas is 16 (for LRC codec).
 *  Additional +8 enables some flexibility during balancing.
 */
constexpr int TypicalReplicaCount = 24;
constexpr int GenericChunkReplicaIndex = 16;  // no specific replica; the default one for non-erasure chunks

//! Valid indexes are in range |[0, ChunkReplicaIndexBound)|.
constexpr int ChunkReplicaIndexBound = 32;

constexpr int GenericMediumIndex      = 126; // internal sentinel meaning "no specific medium"
constexpr int AllMediaIndex           = 127; // passed to various APIs to indicate that any medium is OK
constexpr int DefaultStoreMediumIndex =   0;
constexpr int DefaultSlotsMediumIndex =   0;

//! Valid indexes (including sentinels) are in range |[0, MediumIndexBound)|.
constexpr int MediumIndexBound = AllMediaIndex + 1;

class TChunkReplicaWithMedium;
using TChunkReplicaWithMediumList = TCompactVector<TChunkReplicaWithMedium, TypicalReplicaCount>;

class TChunkReplicaWithLocation;
using TChunkReplicaWithLocationList = TCompactVector<TChunkReplicaWithLocation, TypicalReplicaCount>;

struct TWrittenChunkReplicasInfo;

class TChunkReplica;
using TChunkReplicaList = TCompactVector<TChunkReplica, TypicalReplicaCount>;

extern const TString DefaultStoreAccountName;
extern const TString DefaultStoreMediumName;
extern const TString DefaultCacheMediumName;
extern const TString DefaultSlotsMediumName;

DECLARE_REFCOUNTED_STRUCT(IReaderBase)

DECLARE_REFCOUNTED_CLASS(TFetchChunkSpecConfig)
DECLARE_REFCOUNTED_CLASS(TFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TChunkSliceFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterConfig)
DECLARE_REFCOUNTED_CLASS(TErasureReaderConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TBlockFetcherConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationReaderConfig)
DECLARE_REFCOUNTED_CLASS(TReplicationWriterConfig)
DECLARE_REFCOUNTED_CLASS(TErasureWriterConfig)
DECLARE_REFCOUNTED_CLASS(TMultiChunkWriterConfig)
DECLARE_REFCOUNTED_CLASS(TEncodingWriterOptions)
DECLARE_REFCOUNTED_CLASS(TBlockReordererConfig)
DECLARE_REFCOUNTED_CLASS(TChunkFragmentReaderConfig)

struct TCodecDuration;
class TCodecStatistics;

class TLegacyReadLimit;
class TLegacyReadRange;
class TReadLimit;
class TReadRange;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChunkAvailabilityPolicy,
    ((DataPartsAvailable)           (0))
    ((AllPartsAvailable)            (1))
    ((Repairable)                   (2))
);

// Keep in sync with NChunkServer::ETableChunkFormat.
DEFINE_ENUM(EChunkFormat,
    // Sentinels.
    ((Unknown)                             (-1))

    // File chunks.
    ((FileDefault)                          (1))

    // Table chunks.
    ((TableUnversionedSchemaful)            (3))
    ((TableUnversionedSchemalessHorizontal) (4))
    ((TableUnversionedColumnar)             (6))
    ((TableVersionedSimple)                 (2))
    ((TableVersionedColumnar)               (5))
    ((TableVersionedIndexed)                (8))
    ((TableVersionedSlim)                   (9))

    // Journal chunks.
    ((JournalDefault)                       (0))

    // Hunk chunks.
    ((HunkDefault)                          (7))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
