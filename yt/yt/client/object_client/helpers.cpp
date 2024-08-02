#include "helpers.h"

#include <yt/yt/core/misc/guid.h>

namespace NYT::NObjectClient {

////////////////////////////////////////////////////////////////////////////////

NYPath::TYPath FromObjectId(TObjectId id)
{
    return TString(ObjectIdPathPrefix) + ToString(id);
}

bool IsScalarType(EObjectType type)
{
    return
        type == EObjectType::StringNode ||
        type == EObjectType::Int64Node ||
        type == EObjectType::Uint64Node ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::BooleanNode;
}

bool IsVersionedType(EObjectType type)
{
    return
        type == EObjectType::StringNode ||
        type == EObjectType::Int64Node ||
        type == EObjectType::Uint64Node ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::BooleanNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::ReplicationLogTable ||
        type == EObjectType::Journal ||
        type == EObjectType::ChunkMap ||
        type == EObjectType::LostChunkMap ||
        type == EObjectType::LostVitalChunkMap ||
        type == EObjectType::PrecariousChunkMap ||
        type == EObjectType::PrecariousVitalChunkMap ||
        type == EObjectType::OverreplicatedChunkMap ||
        type == EObjectType::UnderreplicatedChunkMap ||
        type == EObjectType::DataMissingChunkMap ||
        type == EObjectType::ParityMissingChunkMap ||
        type == EObjectType::OldestPartMissingChunkMap ||
        type == EObjectType::QuorumMissingChunkMap ||
        type == EObjectType::UnsafelyPlacedChunkMap ||
        type == EObjectType::InconsistentlyPlacedChunkMap ||
        type == EObjectType::UnexpectedOverreplicatedChunkMap ||
        type == EObjectType::ReplicaTemporarilyUnavailableChunkMap ||
        type == EObjectType::ForeignChunkMap ||
        type == EObjectType::LocalLostChunkMap ||
        type == EObjectType::LocalLostVitalChunkMap ||
        type == EObjectType::LocalPrecariousChunkMap ||
        type == EObjectType::LocalPrecariousVitalChunkMap ||
        type == EObjectType::LocalOverreplicatedChunkMap ||
        type == EObjectType::LocalUnderreplicatedChunkMap ||
        type == EObjectType::LocalDataMissingChunkMap ||
        type == EObjectType::LocalParityMissingChunkMap ||
        type == EObjectType::LocalOldestPartMissingChunkMap ||
        type == EObjectType::LocalQuorumMissingChunkMap ||
        type == EObjectType::LocalUnsafelyPlacedChunkMap ||
        type == EObjectType::LocalInconsistentlyPlacedChunkMap ||
        type == EObjectType::LocalUnexpectedOverreplicatedChunkMap ||
        type == EObjectType::LocalReplicaTemporarilyUnavailableChunkMap ||
        type == EObjectType::RackMap ||
        type == EObjectType::DataCenterMap ||
        type == EObjectType::HostMap ||
        type == EObjectType::ChunkLocationMap ||
        type == EObjectType::ChunkListMap ||
        type == EObjectType::ChunkViewMap ||
        type == EObjectType::MediumMap ||
        type == EObjectType::ForeignTransactionMap ||
        type == EObjectType::TopmostTransactionMap ||
        type == EObjectType::TransactionMap ||
        type == EObjectType::ClusterNodeNode ||
        type == EObjectType::ClusterNodeMap ||
        type == EObjectType::DataNodeMap ||
        type == EObjectType::ExecNodeMap ||
        type == EObjectType::TabletNodeMap ||
        type == EObjectType::ChaosNodeMap ||
        type == EObjectType::Orchid ||
        type == EObjectType::AccountMap ||
        type == EObjectType::UserMap ||
        type == EObjectType::GroupMap ||
        type == EObjectType::AccountResourceUsageLeaseMap ||
        type == EObjectType::SchedulerPoolTreeMap ||
        type == EObjectType::Link ||
        type == EObjectType::SequoiaLink ||
        type == EObjectType::Document ||
        type == EObjectType::LockMap ||
        type == EObjectType::TabletMap ||
        type == EObjectType::TabletCellMap ||
        type == EObjectType::VirtualTabletCellMap ||
        type == EObjectType::TabletCellNode ||
        type == EObjectType::TabletCellBundleMap ||
        type == EObjectType::TabletActionMap ||
        type == EObjectType::CellOrchidNode ||
        type == EObjectType::AreaMap ||
        type == EObjectType::ChaosCellMap ||
        type == EObjectType::VirtualChaosCellMap ||
        type == EObjectType::ChaosCellBundleMap ||
        type == EObjectType::SysNode ||
        type == EObjectType::PortalEntrance ||
        type == EObjectType::PortalExit ||
        type == EObjectType::PortalEntranceMap ||
        type == EObjectType::PortalExitMap ||
        type == EObjectType::CypressShardMap ||
        type == EObjectType::EstimatedCreationTimeMap ||
        type == EObjectType::NetworkProjectMap ||
        type == EObjectType::HttpProxyRoleMap ||
        type == EObjectType::RpcProxyRoleMap ||
        type == EObjectType::MasterTableSchemaMap ||
        type == EObjectType::ChaosReplicatedTable ||
        type == EObjectType::AccessControlObjectNamespaceMap ||
        type == EObjectType::HunkStorage ||
        type == EObjectType::ZookeeperShardMap ||
        type == EObjectType::Rootstock ||
        type == EObjectType::RootstockMap ||
        type == EObjectType::Scion ||
        type == EObjectType::ScionMap ||
        type == EObjectType::ClusterProxyNode ||
        type == EObjectType::SequoiaMapNode ||
        type == EObjectType::Pipeline ||
        type == EObjectType::QueueConsumer ||
        type == EObjectType::QueueProducer;
}

bool IsUserType(EObjectType type)
{
    return
        type == EObjectType::Transaction ||
        type == EObjectType::SystemTransaction ||
        type == EObjectType::Chunk ||
        type == EObjectType::JournalChunk ||
        type == EObjectType::ErasureChunk ||
        type == EObjectType::ErasureJournalChunk ||
        type == EObjectType::ChunkList ||
        type == EObjectType::StringNode ||
        type == EObjectType::Int64Node ||
        type == EObjectType::Uint64Node ||
        type == EObjectType::DoubleNode ||
        type == EObjectType::BooleanNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::ListNode ||
        type == EObjectType::File ||
        type == EObjectType::Table ||
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::ReplicationLogTable ||
        type == EObjectType::TableReplica ||
        type == EObjectType::TabletAction ||
        type == EObjectType::Journal ||
        type == EObjectType::Link ||
        type == EObjectType::AccessControlObject ||
        type == EObjectType::Document ||
        type == EObjectType::Account ||
        type == EObjectType::SchedulerPool ||
        type == EObjectType::SchedulerPoolTree ||
        type == EObjectType::ChaosReplicatedTable ||
        type == EObjectType::HunkStorage ||
        type == EObjectType::SecondaryIndex;
}

bool IsSchemafulType(EObjectType type)
{
    return
        IsTableType(type) ||
        type == EObjectType::ChaosReplicatedTable;
}

bool IsTableType(EObjectType type)
{
    return
        type == EObjectType::Table ||
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::ReplicationLogTable;
}

bool IsLogTableType(EObjectType type)
{
    return
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::ReplicationLogTable;
}

bool IsTabletOwnerType(EObjectType type)
{
    return
        IsTableType(type) ||
        type == EObjectType::HunkStorage;
}

bool IsChunkOwnerType(EObjectType type)
{
    return
        IsTableType(type) ||
        type == EObjectType::File ||
        type == EObjectType::Journal;
}

bool IsCellType(EObjectType type)
{
    return
        type == EObjectType::TabletCell ||
        type == EObjectType::ChaosCell;
}

bool IsCellBundleType(EObjectType type)
{
    return
        type == EObjectType::TabletCellBundle ||
        type == EObjectType::ChaosCellBundle;
}

bool IsAlienType(EObjectType type)
{
    return type == EObjectType::ChaosCell;
}

bool IsTabletType(EObjectType type)
{
    return
        type == EObjectType::Tablet ||
        type == EObjectType::HunkTablet;
}

bool IsReplicatedTableType(EObjectType type)
{
    return
        type == EObjectType::ReplicatedTable ||
        type == EObjectType::ReplicationCard;
}

bool IsTableReplicaType(EObjectType type)
{
    return
        type == EObjectType::TableReplica ||
        IsChaosTableReplicaType(type);
}

bool IsChaosTableReplicaType(EObjectType type)
{
    return type == EObjectType::ChaosTableReplica;
}

bool IsCollocationType(EObjectType type)
{
    return
        type == EObjectType::TableCollocation ||
        type == EObjectType::ReplicationCardCollocation;
}

bool IsMediumType(EObjectType type)
{
    return
        type == EObjectType::DomesticMedium ||
        type == EObjectType::S3Medium;
}

bool IsCypressTransactionType(EObjectType type)
{
    return
        type == EObjectType::Transaction ||
        type == EObjectType::NestedTransaction;
}

bool IsSystemTransactionType(EObjectType type)
{
    return
        type == EObjectType::SystemTransaction ||
        type == EObjectType::SystemNestedTransaction ||
        type == EObjectType::UploadTransaction ||
        type == EObjectType::UploadNestedTransaction ||
        type == EObjectType::ExternalizedTransaction ||
        type == EObjectType::ExternalizedNestedTransaction;
}

bool IsUploadTransactionType(EObjectType type)
{
    return
        type == EObjectType::UploadTransaction ||
        type == EObjectType::UploadNestedTransaction;
}

bool IsCompositeNodeType(EObjectType type)
{
    return type == EObjectType::MapNode || type == EObjectType::ListNode;
}

bool IsLinkType(EObjectType type)
{
    return type == EObjectType::Link || type == EObjectType::SequoiaLink;
}

bool HasSchema(EObjectType type)
{
    if (type == EObjectType::Master) {
        return false;
    }
    if (IsSchemaType(type)) {
        return false;
    }
    return true;
}

bool IsSchemaType(EObjectType type)
{
    return (static_cast<ui32>(type) & SchemaObjectTypeMask) != 0;
}

bool IsGlobalCellId(TCellId cellId)
{
    auto type = TypeFromId(cellId);
    return
        type == EObjectType::MasterCell ||
        type == EObjectType::ChaosCell;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectClient

