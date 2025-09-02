#pragma once

#include "ydb/core/tablet_flat/flat_cxx_database.h"
#include <ydb/core/tablet_flat/flat_dbase_scheme.h>


namespace NKikimr {
namespace NPQ {
namespace NPQRBPrivate {

    struct Schema : NIceDb::Schema {
        struct Data : Table<32> {
            struct Key : Column<32, NScheme::NTypeIds::Uint32> {};
            struct PathId : Column<33, NScheme::NTypeIds::Uint64> {};
            struct Topic : Column<34, NScheme::NTypeIds::Utf8> {};
            struct Path : Column<35, NScheme::NTypeIds::Utf8> {};
            struct Version : Column<36, NScheme::NTypeIds::Uint32> {};
            struct Config : Column<40, NScheme::NTypeIds::Utf8> {};
            struct MaxPartsPerTablet : Column<41, NScheme::NTypeIds::Uint32> {};
            struct SchemeShardId : Column<42, NScheme::NTypeIds::Uint64> {};
            struct NextPartitionId : Column<43, NScheme::NTypeIds::Uint64> {};
            struct SubDomainPathId : Column<44, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<Key, PathId, Topic, Path, Version, Config, MaxPartsPerTablet, SchemeShardId, NextPartitionId, SubDomainPathId>;
        };

        struct Partitions : Table<33> {
            struct Partition : Column<32, NScheme::NTypeIds::Uint32> {};
            struct TabletId : Column<33, NScheme::NTypeIds::Uint64> {};

            struct State : Column<34, NScheme::NTypeIds::Uint32> {};
            struct DataSize : Column<35, NScheme::NTypeIds::Uint64> {};
            struct UsedReserveSize : Column<36, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<Partition>;
            using TColumns = TableColumns<Partition, TabletId, State, DataSize, UsedReserveSize>;
        };

        struct Groups : Table<34> {
            struct GroupId : Column<32, NScheme::NTypeIds::Uint32> {};
            struct Partition : Column<33, NScheme::NTypeIds::Uint32> {};

            using TKey = TableKey<GroupId, Partition>;
            using TColumns = TableColumns<GroupId, Partition>;
        };

        struct Tablets : Table<35> {
            struct Owner : Column<32, NScheme::NTypeIds::Uint64> {};
            struct Idx : Column<33, NScheme::NTypeIds::Uint64> {};
            struct TabletId : Column<34, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TabletId>;
            using TColumns = TableColumns<Owner, Idx, TabletId>;
        };

        struct Operations : Table<36> {
            struct Idx : Column<33, NScheme::NTypeIds::Uint64> {};
            struct State : Column<34, NScheme::NTypeIds::Utf8> {}; //serialzed protobuf

            using TKey = TableKey<Idx>;
            using TColumns = TableColumns<Idx, State>;
        };

        using TTables = SchemaTables<Data, Partitions, Groups, Tablets, Operations>;
    };

}
}
}
