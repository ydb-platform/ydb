#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/core/tablet_schema.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

struct TPartitionSchema: public NKikimr::NIceDb::Schema
{
    enum EChannels
    {
        SystemChannel,
        LogChannel,
        IndexChannel,
    };

    struct Meta: public TTableSchema<1>
    {
        struct Id: public Column<1, NKikimr::NScheme::NTypeIds::Uint32>
        {
        };

        struct PartitionMeta
            : public Column<2, NKikimr::NScheme::NTypeIds::String>
        {
            using Type = TStringBuf;
        };

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, PartitionMeta>;

        using StoragePolicy = TStoragePolicy<IndexChannel>;
    };

    using TTables = SchemaTables<Meta>;

    using TSettings =
        SchemaSettings<ExecutorLogBatching<true>, ExecutorLogFlushPeriod<0> >;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
