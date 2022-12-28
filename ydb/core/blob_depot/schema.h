#pragma once

#include "defs.h"
#include "types.h"

namespace NKikimr::NBlobDepot {

    enum class EDecommitState : ui32 {
        Default = 0, // at start or not running
        BlocksFinished = 1,
        BarriersFinished = 2,
        BlobsFinished = 3,
        BlobsCopied = 4,
        Done = 5,
    };

    struct Schema : NIceDb::Schema {
        struct Config : Table<1> {
            struct Key : Column<1, NScheme::NTypeIds::Uint32> { static constexpr Type Value = 0; };
            struct ConfigProtobuf : Column<2, NScheme::NTypeIds::String> {};
            struct DecommitState : Column<3, NScheme::NTypeIds::Uint32> { using Type = EDecommitState; static constexpr Type Default = EDecommitState::Default; };
            struct AssimilatorState : Column<4, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<
                Key,
                ConfigProtobuf,
                DecommitState,
                AssimilatorState
            >;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // BlobStorage-related parts

        struct Blocks : Table<2> {
            struct TabletId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct BlockedGeneration : Column<2, NScheme::NTypeIds::Uint32> {};
            struct IssuerGuid : Column<3, NScheme::NTypeIds::Uint64> {};
            struct IssueTimestamp : Column<4, NScheme::NTypeIds::Uint64> { using Type = TInstant; };
            struct IssuedByNode : Column<5, NScheme::NTypeIds::Uint32> {};

            using TKey = TableKey<TabletId>;
            using TColumns = TableColumns<
                TabletId,
                BlockedGeneration,
                IssuerGuid,
                IssueTimestamp,
                IssuedByNode
            >;
        };

        struct Barriers : Table<3> {
            struct TabletId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Channel : Column<2, NScheme::NTypeIds::Uint8> {};
            struct HardGenCtr : Column<7, NScheme::NTypeIds::Uint64> {};
            struct Soft : Column<5, NScheme::NTypeIds::Uint64> {};
            struct SoftGenCtr : Column<8, NScheme::NTypeIds::Uint64> {};
            struct Hard : Column<6, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<TabletId, Channel>;
            using TColumns = TableColumns<
                TabletId,
                Channel,
                HardGenCtr,
                Soft,
                SoftGenCtr,
                Hard
            >;
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Common parts

        struct Data : Table<4> {
            struct Key : Column<1, NScheme::NTypeIds::String> {};
            struct Value : Column<2, NScheme::NTypeIds::String> {};
            struct UncertainWrite : Column<3, NScheme::NTypeIds::Bool> { static constexpr Type Default = false; };

            using TKey = TableKey<Key>;
            using TColumns = TableColumns<
                Key,
                Value,
                UncertainWrite
            >;

            using Precharge = NoAutoPrecharge;
        };

        struct Trash : Table<5> {
            struct BlobId : Column<1, NScheme::NTypeIds::String> {};

            using TKey = TableKey<BlobId>;
            using TColumns = TableColumns<BlobId>;

            using Precharge = NoAutoPrecharge;
        };

        struct GC : Table<6> {
            struct Channel : Column<1, NScheme::NTypeIds::Uint8> {};
            struct GroupId : Column<2, NScheme::NTypeIds::Uint32> {};
            struct IssuedGenStep : Column<3, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 0; };
            struct ConfirmedGenStep : Column<4, NScheme::NTypeIds::Uint64> { static constexpr Type Default = 0; };

            using TKey = TableKey<Channel, GroupId>;
            using TColumns = TableColumns<Channel, GroupId, IssuedGenStep, ConfirmedGenStep>;
        };

        using TTables = SchemaTables<
            Config,
            Blocks,
            Barriers,
            Data,
            Trash,
            GC
        >;

        using TSettings = SchemaSettings<
            ExecutorLogBatching<true>,
            ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
        >;
    };

} // NKikimr::NBlobDepot
