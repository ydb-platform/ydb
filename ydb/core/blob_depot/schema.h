#pragma once

#include "defs.h"
#include "types.h"

namespace NKikimr::NBlobDepot {

    struct Schema : NIceDb::Schema {
        struct Config : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::Uint32> { static constexpr Type Value = 0; };
            struct ConfigProtobuf : Column<2, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<
                Key,
                ConfigProtobuf
            >;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // BlobStorage-related parts

        struct Blocks : Table<2> {
            struct TabletId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct BlockedGeneration : Column<2, NScheme::NTypeIds::Uint32> {};
            struct IssueTimestamp : Column<3, NScheme::NTypeIds::Uint64> { using Type = TInstant; };
            struct IssuedByNode : Column<4, NScheme::NTypeIds::Uint32> {};

            using TKey = TableKey<TabletId>;
            using TColumns = TableColumns<
                TabletId,
                BlockedGeneration,
                IssueTimestamp,
                IssuedByNode
            >;
        };

        struct Barriers : Table<3> {
            struct TabletId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Channel : Column<2, NScheme::NTypeIds::Uint8> {};
            struct SoftGenStep : Column<3, NScheme::NTypeIds::Uint64> {};
            struct HardGenStep : Column<4, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TabletId, Channel>;
            using TColumns = TableColumns<
                TabletId,
                Channel,
                SoftGenStep,
                HardGenStep
            >;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Common parts

        struct Data : Table<4> {
            struct Key : Column<1, NScheme::NTypeIds::String> {};
            struct Value : Column<2, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<
                Key,
                Value
            >;
        };

        using TTables = SchemaTables<
            Config,
            Blocks,
            Barriers,
            Data
        >;

        using TSettings = SchemaSettings<
            ExecutorLogBatching<true>,
            ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
        >;
    };

} // NKikimr::NBlobDepot
