#pragma once

#include "dbs_controller_database.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/request_info.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <util/generic/vector.h>

#include <utility>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DBS_CONTROLLER_TRANSACTIONS(xxx, ...) \
    xxx(InitSchema, __VA_ARGS__)                         \
    xxx(LoadState, __VA_ARGS__)                          \
    xxx(UpdateDDiskMap, __VA_ARGS__)                     \
    xxx(GetPartitionsForNode, __VA_ARGS__)               \
    xxx(GetNodesForPartition, __VA_ARGS__)

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

        TVector<TDbsControllerDatabase::TRecordKey> TabletRecordsKeys;

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
            TabletRecordsKeys.clear();
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
    // GetNodesForPartition
    //
    struct TGetNodesForPartition
    {
        const NBS::NStorage::TRequestInfoPtr RequestInfo;

        const ui64 TabletId;

        // Output
        TVector<ui32> Nodes;

        explicit TGetNodesForPartition(
            NBS::NStorage::TRequestInfoPtr requestInfo,
            const ui64 tabletId)
            : RequestInfo(std::move(requestInfo))
            , TabletId(tabletId)
        {}

        void Clear()
        {
            Nodes.clear();
        }
    };
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
