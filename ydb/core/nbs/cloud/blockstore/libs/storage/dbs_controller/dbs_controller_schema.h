#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

struct TDbsControllerSchema: public NKikimr::NIceDb::Schema
{
    struct DirectMap: TTableSchema<1>
    {
        struct TabletId: Column<1, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        // Unique within TabletId
        struct DirectBlockGroupIndex
            : Column<2, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        struct DDisks: Column<3, NKikimr::NScheme::NTypeIds::String>
        {
        };

        struct LogicalNodesCount: Column<4, NKikimr::NScheme::NTypeIds::Uint64>
        {
        };

        using TKey = TableKey<TabletId, DirectBlockGroupIndex>;

        using TColumns = TableColumns<
            TabletId,
            DirectBlockGroupIndex,
            DDisks,
            LogicalNodesCount>;
    };

    struct InverseMap: TTableSchema<2>
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

        struct DirectBlockGroups: Column<4, NKikimr::NScheme::NTypeIds::String>
        {
        };

        using TKey = TableKey<NodeId, PDiskId, DDiskSlotId>;

        using TColumns =
            TableColumns<NodeId, PDiskId, DDiskSlotId, DirectBlockGroups>;
    };

    using TTables = SchemaTables<DirectMap, InverseMap>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
