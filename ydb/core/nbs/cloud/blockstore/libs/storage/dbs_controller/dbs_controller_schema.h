#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/protos/dbs_controller.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NDbsController {

////////////////////////////////////////////////////////////////////////////////

struct TDbsControllerSchema: public NKikimr::NIceDb::Schema
{
    enum EChannels
    {
        SystemChannel,
        LogChannel,
        IndexChannel,
    };

    struct Dummy: public TTableSchema<1>
    {
        struct DummyA: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct DummyB: public Column<2, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        using TKey = TableKey<DummyA>;
        using TColumns = TableColumns<DummyA, DummyB>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    using TTables = SchemaTables<Dummy>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0>>;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NDbsController
