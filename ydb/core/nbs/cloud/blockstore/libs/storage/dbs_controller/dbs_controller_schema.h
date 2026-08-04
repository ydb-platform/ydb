#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

struct TDbsControllerSchema: public NKikimr::NIceDb::Schema
{
    struct DDiskMap : Table<1> {
        struct TabletId : Column<1, NKikimr::NScheme::NTypeIds::Uint64> {};

        // DDiskId.NodeId
        struct NodeId : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};

        // DDiskId.PDiskId
        struct PDiskId : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};

        // DDiskId.DDiskSlotId
        struct DDiskSlotId : Column<1, NKikimr::NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<TabletId, NodeId, PDiskId, DDiskSlotId>;
        using TColumns = TableColumns<TabletId, NodeId, PDiskId, DDiskSlotId>;
    };

    using TTables = SchemaTables<DDiskMap>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
