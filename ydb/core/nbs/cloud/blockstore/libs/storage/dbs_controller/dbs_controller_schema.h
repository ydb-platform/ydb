#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

struct TDbsControllerSchema: public NKikimr::NIceDb::Schema
{
    struct DDiskMap: TTableSchema<1>
    {
        struct TabletId: Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        // Unique within TabletId
        struct DirectBlockGroupId: Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        // DDiskId.NodeId
        struct NodeId: Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        // DDiskId.PDiskId
        struct PDiskId: Column<4, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        // DDiskId.DDiskSlotId
        struct DDiskSlotId: Column<5, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct IsPBuffer: Column<6, NKikimr::NScheme::NTypeIds::Bool>
        {
        };

        using TKey = TableKey<
            TabletId,
            DirectBlockGroupId,
            NodeId,
            PDiskId,
            DDiskSlotId>;

        using TColumns = TableColumns<
            TabletId,
            DirectBlockGroupId,
            NodeId,
            PDiskId,
            DDiskSlotId,
            IsPBuffer>;
    };

    struct InverseDDiskMap: TTableSchema<2>
    {
        // DDiskId.NodeId
        struct NodeId: Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        // DDiskId.PDiskId
        struct PDiskId: Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        // DDiskId.DDiskSlotId
        struct DDiskSlotId: Column<3, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct TabletId: Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<NodeId, PDiskId, DDiskSlotId, TabletId>;

        using TColumns = TableColumns<NodeId, PDiskId, DDiskSlotId, TabletId>;
    };

    using TTables = SchemaTables<DDiskMap, InverseDDiskMap>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
