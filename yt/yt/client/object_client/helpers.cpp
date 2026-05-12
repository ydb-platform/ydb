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

bool IsSequoiaNode(NObjectClient::EObjectType type)
{
    return
        type == EObjectType::SequoiaMapNode ||
        type == EObjectType::SequoiaLink ||
        type == EObjectType::Scion;
}

bool IsVersionedType(EObjectType type)
{
    switch (type) {
        case EObjectType::StringNode:
        case EObjectType::Int64Node:
        case EObjectType::Uint64Node:
        case EObjectType::DoubleNode:
        case EObjectType::BooleanNode:
        case EObjectType::MapNode:
        case EObjectType::File:
        case EObjectType::Table:
        case EObjectType::ReplicatedTable:
        case EObjectType::ReplicationLogTable:
        case EObjectType::Journal:
        case EObjectType::ChunkMap:
        case EObjectType::LostChunkMap:
        case EObjectType::LostVitalChunkMap:
        case EObjectType::LostVitalChunksSampleMap:
        case EObjectType::PrecariousChunkMap:
        case EObjectType::PrecariousVitalChunkMap:
        case EObjectType::OverreplicatedChunkMap:
        case EObjectType::UnderreplicatedChunkMap:
        case EObjectType::DataMissingChunkMap:
        case EObjectType::DataMissingChunksSampleMap:
        case EObjectType::ParityMissingChunkMap:
        case EObjectType::ParityMissingChunksSampleMap:
        case EObjectType::OldestPartMissingChunkMap:
        case EObjectType::OldestPartMissingChunksSampleMap:
        case EObjectType::QuorumMissingChunkMap:
        case EObjectType::QuorumMissingChunksSampleMap:
        case EObjectType::UnsafelyPlacedChunkMap:
        case EObjectType::InconsistentlyPlacedChunkMap:
        case EObjectType::InconsistentlyPlacedChunksSampleMap:
        case EObjectType::UnexpectedOverreplicatedChunkMap:
        case EObjectType::ReplicaTemporarilyUnavailableChunkMap:
        case EObjectType::ForeignChunkMap:
        case EObjectType::LocalLostChunkMap:
        case EObjectType::LocalLostVitalChunkMap:
        case EObjectType::LocalPrecariousChunkMap:
        case EObjectType::LocalPrecariousVitalChunkMap:
        case EObjectType::LocalOverreplicatedChunkMap:
        case EObjectType::LocalUnderreplicatedChunkMap:
        case EObjectType::LocalDataMissingChunkMap:
        case EObjectType::LocalParityMissingChunkMap:
        case EObjectType::LocalOldestPartMissingChunkMap:
        case EObjectType::LocalQuorumMissingChunkMap:
        case EObjectType::LocalUnsafelyPlacedChunkMap:
        case EObjectType::LocalInconsistentlyPlacedChunkMap:
        case EObjectType::LocalUnexpectedOverreplicatedChunkMap:
        case EObjectType::LocalReplicaTemporarilyUnavailableChunkMap:
        case EObjectType::RackMap:
        case EObjectType::DataCenterMap:
        case EObjectType::HostMap:
        case EObjectType::ChunkLocationMap:
        case EObjectType::ChunkListMap:
        case EObjectType::ChunkViewMap:
        case EObjectType::MediumMap:
        case EObjectType::ForeignTransactionMap:
        case EObjectType::TopmostTransactionMap:
        case EObjectType::TransactionMap:
        case EObjectType::ClusterNodeNode:
        case EObjectType::ClusterNodeMap:
        case EObjectType::DataNodeMap:
        case EObjectType::ExecNodeMap:
        case EObjectType::TabletNodeMap:
        case EObjectType::ChaosNodeMap:
        case EObjectType::Orchid:
        case EObjectType::AccountMap:
        case EObjectType::UserMap:
        case EObjectType::GroupMap:
        case EObjectType::AccountResourceUsageLeaseMap:
        case EObjectType::SchedulerPoolTreeMap:
        case EObjectType::Link:
        case EObjectType::SequoiaLink:
        case EObjectType::Document:
        case EObjectType::LockMap:
        case EObjectType::TabletMap:
        case EObjectType::TabletCellMap:
        case EObjectType::VirtualTabletCellMap:
        case EObjectType::TabletCellNode:
        case EObjectType::TabletCellBundleMap:
        case EObjectType::TabletActionMap:
        case EObjectType::CellOrchidNode:
        case EObjectType::AreaMap:
        case EObjectType::ChaosCellMap:
        case EObjectType::VirtualChaosCellMap:
        case EObjectType::ChaosCellBundleMap:
        case EObjectType::SysNode:
        case EObjectType::PortalEntrance:
        case EObjectType::PortalExit:
        case EObjectType::PortalEntranceMap:
        case EObjectType::PortalExitMap:
        case EObjectType::CypressShardMap:
        case EObjectType::EstimatedCreationTimeMap:
        case EObjectType::NetworkProjectMap:
        case EObjectType::HttpProxyRoleMap:
        case EObjectType::RpcProxyRoleMap:
        case EObjectType::MasterTableSchemaMap:
        case EObjectType::ChaosReplicatedTable:
        case EObjectType::AccessControlObjectNamespaceMap:
        case EObjectType::HunkStorage:
        case EObjectType::ZookeeperShardMap:
        case EObjectType::Rootstock:
        case EObjectType::RootstockMap:
        case EObjectType::Scion:
        case EObjectType::ScionMap:
        case EObjectType::ClusterProxyNode:
        case EObjectType::SequoiaMapNode:
        case EObjectType::Pipeline:
        case EObjectType::QueueConsumer:
        case EObjectType::QueueProducer:
        case EObjectType::CypressProxyMap:
            return true;

        default:
            return false;
    };
}

bool IsChunkType(EObjectType type)
{
    switch (type) {
        case EObjectType::Chunk:
        case EObjectType::ErasureChunk:
        case EObjectType::JournalChunk:
        case EObjectType::ErasureJournalChunk:
            return true;

        default:
            if (ToUnderlying(type) >= ToUnderlying(MinErasureChunkPartType) &&
                ToUnderlying(type) <= ToUnderlying(MaxErasureChunkPartType))
            {
                return true;
            }

            if (ToUnderlying(type) >= ToUnderlying(MinErasureJournalChunkPartType) &&
                ToUnderlying(type) <= ToUnderlying(MaxErasureJournalChunkPartType))
            {
                return true;
            }

            return false;
    }
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
        type == EObjectType::HunkStorage;
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
        type == EObjectType::MasterCell ||
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

bool IsReplicationCardType(EObjectType type)
{
    return type == EObjectType::ReplicationCard;
}

bool IsChaosLeaseType(EObjectType type)
{
    return type == EObjectType::ChaosLease;
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

bool IsExternalizedTransactionType(EObjectType type)
{
    return
        type == EObjectType::ExternalizedTransaction ||
        type == EObjectType::ExternalizedNestedTransaction;
}

bool IsCompositeNodeType(EObjectType type)
{
    return
        type == EObjectType::SequoiaMapNode ||
        type == EObjectType::MapNode ||
        type == EObjectType::Scion ||
        type == EObjectType::PortalExit ||
        type == EObjectType::ClusterProxyNode ||
        type == EObjectType::SysNode;
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

std::string FormatObjectType(EObjectType type)
{
    return IsSchemaType(type)
        ? std::string(Format("schema:%v", TypeFromSchemaType(type)))
        : FormatEnum(type);
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
