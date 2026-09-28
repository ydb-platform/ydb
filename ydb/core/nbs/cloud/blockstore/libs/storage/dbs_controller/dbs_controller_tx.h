#pragma once

#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <util/generic/vector.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS(xxx, ...)                       \
    xxx(InitSchema, __VA_ARGS__)                                               \
    xxx(LoadState, __VA_ARGS__)                                                \
    xxx(UpdateDDiskMap, __VA_ARGS__)                                           \
    xxx(RemoveTabletDDiskMap, __VA_ARGS__)                                     \
    xxx(GetPartitionsForNode, __VA_ARGS__)                                     \
    xxx(NodeMaintenancePermission, __VA_ARGS__)

// BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS

////////////////////////////////////////////////////////////////////////////////

struct TTxDbsController
{
    //
    // InitSchema
    //
    struct TInitSchema
    {
        explicit TInitSchema()
        {}

        void Clear()
        {}
    };

    //
    // LoadState
    //
    struct TLoadState
    {
        explicit TLoadState()
        {}

        void Clear()
        {}
    };

    //
    // UpdateDDiskMap
    //
    struct TUpdateDDiskMap
    {
        const NBS::NStorage::TRequestInfoPtr RequestInfo;

        const ui64 PartitionTabletId;
        const NProto::TPartitionDDisks DDisks;

        THashMap<
            TDbsControllerDatabase::TInverseKey,
            NProto::TDDiskDirectBlockGroups>
            ModifiedInverseRecords;

        explicit TUpdateDDiskMap(
            NBS::NStorage::TRequestInfoPtr requestInfo,
            const ui64 partitionTabletId,
            NProto::TPartitionDDisks ddisks)
            : RequestInfo(std::move(requestInfo))
            , PartitionTabletId(partitionTabletId)
            , DDisks(std::move(ddisks))
        {}

        void Clear()
        {
            ModifiedInverseRecords.clear();
        }
    };

    //
    // RemoveTabletDDiskMap
    //
    struct TRemoveTabletDDiskMap
    {
        const NBS::NStorage::TRequestInfoPtr RequestInfo;

        const ui64 PartitionTabletId;

        TVector<TDbsControllerDatabase::TDirectKey> DirectKeys;
        TVector<TDbsControllerDatabase::TInverseKey> InverseKeys;

        THashMap<
            TDbsControllerDatabase::TInverseKey,
            NProto::TDDiskDirectBlockGroups>
            ModifiedInverseRecords;

        explicit TRemoveTabletDDiskMap(
            NBS::NStorage::TRequestInfoPtr requestInfo,
            const ui64 partitionTabletId)
            : RequestInfo(std::move(requestInfo))
            , PartitionTabletId(partitionTabletId)
        {}

        void Clear()
        {
            DirectKeys.clear();
            InverseKeys.clear();
            ModifiedInverseRecords.clear();
        }
    };

    //
    // GetPartitionsForNode
    //
    struct TGetPartitionsForNode
    {
        const NBS::NStorage::TRequestInfoPtr RequestInfo;

        const ui32 NodeId;

        // Output
        TVector<ui64> Tablets;

        explicit TGetPartitionsForNode(
            NBS::NStorage::TRequestInfoPtr requestInfo,
            const ui32 nodeId)
            : RequestInfo(std::move(requestInfo))
            , NodeId(nodeId)
        {}

        void Clear()
        {
            Tablets.clear();
        }
    };

    //
    // NodeMaintenancePermission
    //
    struct TNodeMaintenancePermission
    {
        const NBS::NStorage::TRequestInfoPtr RequestInfo;

        TVector<ui32> NodeIds;

        // Output
        bool Allowed = false;
        TVector<ui64> BlockingTablets;

        explicit TNodeMaintenancePermission(
            NBS::NStorage::TRequestInfoPtr requestInfo,
            TVector<ui32> nodeIds)
            : RequestInfo(std::move(requestInfo))
            , NodeIds(std::move(nodeIds))
        {}

        void Clear()
        {
            Allowed = false;
            BlockingTablets.clear();
        }
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
