#pragma once
#include "defs.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr {
namespace NSequenceShard {

    struct TSequenceShardSchema : NIceDb::Schema {
        enum ESysParam : ui64 {
            SysParam_SchemeShardId = 1,
            SysParam_ProcessingParams = 2,
        };

        enum class ESequenceState : ui8 {
            Active = 0,
            Frozen = 1,
            Moved = 2,
        };

        struct SysParams : Table<1> {
            struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
            struct IntValue : Column<2, NScheme::NTypeIds::Uint64> {};
            struct TextValue : Column<3, NScheme::NTypeIds::Utf8> {};
            struct BinaryValue : Column<4, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Id>;
            using TColumns = TableColumns<Id, IntValue, TextValue, BinaryValue>;
        };

        struct Sequences : Table<2> {
            struct OwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct LocalId : Column<2, NScheme::NTypeIds::Uint64> {};
            struct MinValue : Column<3, NScheme::NTypeIds::Int64> {};
            struct MaxValue : Column<4, NScheme::NTypeIds::Int64> {};
            struct StartValue : Column<5, NScheme::NTypeIds::Int64> {};
            struct NextValue : Column<6, NScheme::NTypeIds::Int64> {};
            struct NextUsed : Column<7, NScheme::NTypeIds::Bool> {};
            struct Cache : Column<8, NScheme::NTypeIds::Uint64> {};
            struct Increment : Column<9, NScheme::NTypeIds::Int64> {};
            struct Cycle : Column<10, NScheme::NTypeIds::Bool> {};
            struct State : Column<11, NScheme::NTypeIds::Uint8> {
                using Type = ESequenceState;
                static constexpr ESequenceState Default = ESequenceState::Active;
            };
            struct MovedTo : Column<12, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<OwnerId, LocalId>;
            using TColumns = TableColumns<
                OwnerId,
                LocalId,
                MinValue,
                MaxValue,
                StartValue,
                NextValue,
                NextUsed,
                Cache,
                Increment,
                Cycle,
                State,
                MovedTo>;
        };

        struct SchemeShardRounds : Table<3> {
            struct SchemeShardId : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Generation : Column<2, NScheme::NTypeIds::Uint64> {};
            struct Round : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<SchemeShardId>;
            using TColumns = TableColumns<SchemeShardId, Generation, Round>;
        };

        using TTables = SchemaTables<
            SysParams,
            Sequences,
            SchemeShardRounds>;

        using TSettings = SchemaSettings<
            ExecutorLogBatching<true>,
            ExecutorLogFlushPeriod<500 /*500us*/>>;
    };

} // namespace NSequenceShard
} // namespace NKikimr
