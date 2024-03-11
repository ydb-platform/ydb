#pragma once

#include <library/cpp/yt/misc/enum.h>
#include <library/cpp/yt/misc/guid.h>
#include <library/cpp/yt/misc/hash.h>

#include <yt/yt/client/election/public.h>
#include <yt/yt/client/job_tracker_client/public.h>

#include <library/cpp/yt/misc/strong_typedef.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((PrerequisiteCheckFailed)                   (1000))
    ((InvalidObjectLifeStage)                    (1001))
    ((CrossCellAdditionalPath)                   (1002))
    ((CrossCellRevisionPrerequisitePath)         (1003))
    ((ForwardedRequestFailed)                    (1004))
    ((CannotCacheMutatingRequest)                (1005))
    ((InvalidObjectType)                         (1006))
    ((RequestInvolvesSequoia)                    (1007))
    ((RequestInvolvesCypress)                    (1008))
);

////////////////////////////////////////////////////////////////////////////////

//! Provides a globally unique identifier for an object.
/*!
 *  TGuid consists of four 32-bit parts.
 *  For TObjectId, these parts have the following meaning:
 *
 *  Part 0: some hash
 *  Part 1: bits 0..15:  object type
 *          bits 16..31: cell id
 *  Part 2: the lower  part of 64-bit sequential counter
 *  Part 3: the higher part of 64-bit sequential counter
 */
using TObjectId = TGuid;

//! The all-zero id used to denote a non-existing object.
constexpr TObjectId NullObjectId = {};

//! |#|-prefix.
extern const TStringBuf ObjectIdPathPrefix;

//! Used to mark counters for well-known ids.
constexpr ui64 WellKnownCounterMask = 0x8000000000000000;

//! Used to mark counters for Sequoia objects.
constexpr ui64 SequoiaCounterMask = 0x4000000000000000;

using NElection::TCellId;
using NElection::NullCellId;

//! Identifies a particular cell of YT cluster.
//! Must be globally unique to prevent object ids from colliding.
YT_DEFINE_STRONG_TYPEDEF(TCellTag, ui16)

//! The minimum valid cell tag.
constexpr auto MinValidCellTag = TCellTag(0x0001);

//! The maximum valid cell tag.
constexpr auto MaxValidCellTag = TCellTag(0xf000);

//! A sentinel cell tag indicating that the request does not need replication.
constexpr auto NotReplicatedCellTagSentinel = TCellTag(0xf001);

//! A sentinel cell tag representing the primary master.
constexpr auto PrimaryMasterCellTagSentinel = TCellTag(0xf003);

//! A sentinel cell tag meaning nothing.
constexpr auto InvalidCellTag = TCellTag(0xf004);

//! A static limit for the number of secondary master cells.
constexpr int MaxSecondaryMasterCells = 48;

using TCellTagList = TCompactVector<TCellTag, MaxSecondaryMasterCells + 1>;
using TCellIdList = TCompactVector<TCellId, MaxSecondaryMasterCells + 1>;

//! Currently at most one additional path is expected (source paths for Copy and Move verbs).
constexpr int TypicalAdditionalPathCount = 1;

//! Describes the runtime type of an object.
DEFINE_ENUM(EObjectType,
    // Does not represent any actual type.
    ((Null)                                           (0))

    // The following represent non-versioned objects.
    // These must be created by calling TMasterYPathProxy::CreateObjects.

    // Transaction Manager stuff
    ((Transaction)                                  (  1))
    ((AtomicTabletTransaction)                      (  2))
    ((NonAtomicTabletTransaction)                   (  3))
    ((NestedTransaction)                            (  4))
    ((ExternalizedTransaction)                      (  5))
    ((ExternalizedNestedTransaction)                (  6))
    ((UploadTransaction)                            (  7))
    ((UploadNestedTransaction)                      (  8))
    ((SystemTransaction)                            (  9))
    ((SystemNestedTransaction)                      ( 10))
    ((ExternalizedSystemTabletTransaction)          ( 11))
    ((ExternalizedAtomicTabletTransaction)          ( 12))
    ((ExternalizedNonAtomicTabletTransaction)       ( 13))
    ((TransactionMap)                               (407))
    ((TopmostTransactionMap)                        (418))
    ((LockMap)                                      (422))

    // Chunk Manager stuff
    ((Chunk)                                        (100))
    ((ErasureChunk)                                 (102)) // erasure chunk as a whole
    ((ErasureChunkPart_0)                           (103)) // erasure chunk parts, mnemonic names are for debugging convenience only
    ((ErasureChunkPart_1)                           (104))
    ((ErasureChunkPart_2)                           (105))
    ((ErasureChunkPart_3)                           (106))
    ((ErasureChunkPart_4)                           (107))
    ((ErasureChunkPart_5)                           (108))
    ((ErasureChunkPart_6)                           (109))
    ((ErasureChunkPart_7)                           (110))
    ((ErasureChunkPart_8)                           (111))
    ((ErasureChunkPart_9)                           (112))
    ((ErasureChunkPart_10)                          (113))
    ((ErasureChunkPart_11)                          (114))
    ((ErasureChunkPart_12)                          (115))
    ((ErasureChunkPart_13)                          (116))
    ((ErasureChunkPart_14)                          (117))
    ((ErasureChunkPart_15)                          (118))
    ((JournalChunk)                                 (119))
    ((Artifact)                                     (121))
    ((ChunkMap)                                     (402))
    ((LostChunkMap)                                 (403))
    ((LostVitalChunkMap)                            (413))
    ((PrecariousChunkMap)                           (410))
    ((PrecariousVitalChunkMap)                      (411))
    ((OverreplicatedChunkMap)                       (404))
    ((UnderreplicatedChunkMap)                      (405))
    ((DataMissingChunkMap)                          (419))
    ((ParityMissingChunkMap)                        (420))
    ((OldestPartMissingChunkMap)                    (428))
    ((QuorumMissingChunkMap)                        (424))
    ((UnsafelyPlacedChunkMap)                       (120))
    ((InconsistentlyPlacedChunkMap)                 (160))
    ((UnexpectedOverreplicatedChunkMap)             (434))
    ((ReplicaTemporarilyUnavailableChunkMap)        (436))
    ((ForeignChunkMap)                              (122))
    ((LocalLostChunkMap)                            (450))
    ((LocalLostVitalChunkMap)                       (451))
    ((LocalPrecariousChunkMap)                      (452))
    ((LocalPrecariousVitalChunkMap)                 (453))
    ((LocalOverreplicatedChunkMap)                  (454))
    ((LocalUnderreplicatedChunkMap)                 (455))
    ((LocalDataMissingChunkMap)                     (456))
    ((LocalParityMissingChunkMap)                   (457))
    ((LocalOldestPartMissingChunkMap)               (458))
    ((LocalQuorumMissingChunkMap)                   (459))
    ((LocalUnsafelyPlacedChunkMap)                  (460))
    ((LocalInconsistentlyPlacedChunkMap)            (461))
    ((LocalUnexpectedOverreplicatedChunkMap)        (462))
    ((LocalReplicaTemporarilyUnavailableChunkMap)   (463))
    ((ChunkList)                                    (101))
    ((ChunkListMap)                                 (406))
    ((ChunkView)                                    (123))
    ((ChunkViewMap)                                 (430))
    ((DomesticMedium)                               (408))
    ((S3Medium)                                     (435))
    ((MediumMap)                                    (409))
    ((ErasureJournalChunk)                          (124)) // erasure journal chunk as a whole
    ((ErasureJournalChunkPart_0)                    (125)) // erasure chunk parts, mnemonic names are for debugging convenience only
    ((ErasureJournalChunkPart_1)                    (126))
    ((ErasureJournalChunkPart_2)                    (127))
    ((ErasureJournalChunkPart_3)                    (128))
    ((ErasureJournalChunkPart_4)                    (129))
    ((ErasureJournalChunkPart_5)                    (130))
    ((ErasureJournalChunkPart_6)                    (131))
    ((ErasureJournalChunkPart_7)                    (132))
    ((ErasureJournalChunkPart_8)                    (133))
    ((ErasureJournalChunkPart_9)                    (134))
    ((ErasureJournalChunkPart_10)                   (135))
    ((ErasureJournalChunkPart_11)                   (136))
    ((ErasureJournalChunkPart_12)                   (137))
    ((ErasureJournalChunkPart_13)                   (138))
    ((ErasureJournalChunkPart_14)                   (139))
    ((ErasureJournalChunkPart_15)                   (140))
    ((ChunkLocation)                                (141))
    ((ChunkLocationMap)                             (142))

    // The following represent versioned objects (AKA Cypress nodes).
    // These must be created by calling TCypressYPathProxy::Create.
    // NB: When adding a new type, don't forget to update IsVersionedType.

    // Auxiliary
    ((Lock)                                         (200))

    // Static nodes
    ((StringNode)                                   (300))
    ((Int64Node)                                    (301))
    ((Uint64Node)                                   (306))
    ((DoubleNode)                                   (302))
    ((MapNode)                                      (303))
    ((ListNode)                                     (304))
    ((BooleanNode)                                  (305))

    // Dynamic nodes
    ((File)                                         (400))
    ((Table)                                        (401))
    ((Journal)                                      (423))
    ((Orchid)                                       (412))
    ((Link)                                         (417))
    ((Document)                                     (421))
    ((ReplicatedTable)                              (425))
    ((ReplicationLogTable)                          (431))

    ((AccessControlObject)                          (307))
    ((AccessControlObjectNamespace)                 (432))
    ((AccessControlObjectNamespaceMap)              (433))

    // Sequoia nodes
    ((SequoiaMapNode)                              (1504))

    // Cypress shards
    ((CypressShard)                               (11004))
    ((CypressShardMap)                            (11005))

    // Portals
    ((PortalEntrance)                             (11000))
    ((PortalExit)                                 (11001))
    ((PortalEntranceMap)                          (11002))
    ((PortalExitMap)                              (11003))

    // Grafting
    ((Rootstock)                                  (12000))
    ((Scion)                                      (12001))
    ((RootstockMap)                               (12002))
    ((ScionMap)                                   (12003))

    // Security Manager stuff
    ((Account)                                      (500))
    ((AccountMap)                                   (414))
    ((AccountResourceUsageLease)                    (505))
    ((AccountResourceUsageLeaseMap)                 (506))
    ((User)                                         (501))
    ((UserMap)                                      (415))
    ((Group)                                        (502))
    ((GroupMap)                                     (416))
    ((NetworkProject)                               (503))
    ((NetworkProjectMap)                            (426))
    ((ProxyRole)                                    (504))
    ((HttpProxyRoleMap)                             (427))
    ((RpcProxyRoleMap)                              (429))

    // Table Manager stuff
    ((MasterTableSchema)                           (1300))
    ((MasterTableSchemaMap)                        (1301))
    ((TableCollocation)                            (1302))
    ((SecondaryIndex)                              (1303))

    // Global stuff
    // A mysterious creature representing the master as a whole.
    ((Master)                                       (600))
    ((MasterCell)                                   (601))
    ((SysNode)                                      (602))
    // Next two types would end with 'a' and 'b' in hex.
    ((AliceAvenueEndpoint)                          (618))
    ((BobAvenueEndpoint)                            (619))

    // Tablet Manager stuff
    ((TabletCell)                                   (700))
    ((TabletCellNode)                               (701))
    ((Tablet)                                       (702))
    ((TabletMap)                                    (703))
    ((TabletCellMap)                                (710))
    ((SortedDynamicTabletStore)                     (704))
    ((OrderedDynamicTabletStore)                    (708))
    ((TabletPartition)                              (705))
    ((TabletCellBundle)                             (706))
    ((TabletCellBundleMap)                          (707))
    ((TableReplica)                                 (709))
    ((TabletAction)                                 (711))
    ((TabletActionMap)                              (712))
    ((Area)                                         (713))
    ((AreaMap)                                      (714))
    ((HunkStorage)                                  (715))
    ((HunkTablet)                                   (716))
    ((VirtualTabletCellMap)                         (717))

    // Node Tracker stuff
    ((Rack)                                         (800))
    ((RackMap)                                      (801))
    ((ClusterNode)                                  (802))
    ((ClusterNodeNode)                              (803))
    ((ClusterNodeMap)                               (807))
    ((DataNodeMap)                                  (808))
    ((ExecNodeMap)                                  (809))
    ((TabletNodeMap)                                (810))
    ((ChaosNodeMap)                                 (811))
    ((DataCenter)                                   (805))
    ((DataCenterMap)                                (806))
    ((Host)                                         (812))
    ((HostMap)                                      (813))

    // Job Tracker stuff
    ((SchedulerJob)                                 (900))
    ((MasterJob)                                    (901))

    // Scheduler
    ((Operation)                                   (1000))
    ((SchedulerPool)                               (1001))
    ((SchedulerPoolTree)                           (1002))
    ((SchedulerPoolTreeMap)                        (1003))

    // Object manager stuff
    ((EstimatedCreationTimeMap)                    (1100))

    // Chaos stuff
    ((ChaosCell)                                   (1200))
    ((ChaosCellBundle)                             (1201))
    ((ChaosCellMap)                                (1202))
    ((ChaosCellBundleMap)                          (1203))
    ((ReplicationCard)                             (1204))
    ((ChaosTableReplica)                           (1205))
    ((ChaosReplicatedTable)                        (1206))
    ((ReplicationCardCollocation)                  (1207))
    ((VirtualChaosCellMap)                         (1208))

    // Maintenance tracker stuff
    ((ClusterProxyNode)                            (1500))

    // Zookeeper stuff
    ((ZookeeperShard)                              (1400))
    ((ZookeeperShardMap)                           (1401))

    // Flow stuff
    ((Pipeline)                                    (1600))
);

//! A bit mask marking schema types.
constexpr ui32 SchemaObjectTypeMask = 0x8000;

// The range of erasure chunk part types.
constexpr EObjectType MinErasureChunkPartType = EObjectType::ErasureChunkPart_0;
constexpr EObjectType MaxErasureChunkPartType = EObjectType::ErasureChunkPart_15;

// The range of erasure journal chunk part types.
constexpr EObjectType MinErasureJournalChunkPartType = EObjectType::ErasureJournalChunkPart_0;
constexpr EObjectType MaxErasureJournalChunkPartType = EObjectType::ErasureJournalChunkPart_15;

////////////////////////////////////////////////////////////////////////////////

using TTransactionId = TObjectId;
constexpr TTransactionId NullTransactionId = {};

using NJobTrackerClient::TOperationId;

////////////////////////////////////////////////////////////////////////////////

//! Identifies a node possibly branched by a transaction.
struct TVersionedObjectId
{
    //! Id of the node itself.
    TObjectId ObjectId;

    //! Id of the transaction that had branched the node.
    //! #NullTransactionId if the node is not branched.
    TTransactionId TransactionId;

    //! Initializes a null instance.
    /*!
     *  #NodeId is #NullObjectId, #TransactionId is #NullTransactionId.
     */
    TVersionedObjectId() = default;

    //! Initializes an instance by given node. Sets #TransactionId to #NullTransactionId.
    explicit TVersionedObjectId(TObjectId objectId);

    //! Initializes an instance by given node and transaction ids.
    TVersionedObjectId(TObjectId objectId, TTransactionId transactionId);

    //! Checks that the id is branched, i.e. #TransactionId is not #NullTransactionId.
    bool IsBranched() const;


    static TVersionedObjectId FromString(TStringBuf str);
};

//! Formats id into a string (for debugging and logging purposes mainly).
void FormatValue(TStringBuilderBase* builder, const TVersionedObjectId& id, TStringBuf spec);

//! Converts id into a string (for debugging and logging purposes mainly).
TString ToString(const TVersionedObjectId& id);

//! Compares TVersionedNodeId s for equality.
bool operator == (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs);

//! Compares TVersionedNodeId s for "less than".
bool operator <  (const TVersionedObjectId& lhs, const TVersionedObjectId& rhs);

class TObjectServiceProxy;

struct TDirectObjectIdHash;
struct TDirectVersionedObjectIdHash;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NObjectClient::TVersionedObjectId>
{
    size_t operator()(const NYT::NObjectClient::TVersionedObjectId& id) const
    {
        auto result = THash<NYT::NObjectClient::TObjectId>()(id.ObjectId);
        HashCombine(result, id.TransactionId);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

Y_DECLARE_PODTYPE(NYT::NObjectClient::TVersionedObjectId);
