#pragma once

#include "defs.h"

#include <ydb/core/base/subdomain.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimrNodeBroker {
    class TNodeInfoSchema;
}

namespace NKikimr {
namespace NNodeBroker {

enum class ENodeState : ui8;

struct Schema : NIceDb::Schema {
    enum class EMainNodesTable : ui64 {
        Nodes = 0,
        NodesV2 = 1,
    };

    struct Nodes : Table<1> {
        struct ID : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Host : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Port : Column<3, NScheme::NTypeIds::Uint32> {};
        struct ResolveHost : Column<4, NScheme::NTypeIds::Utf8> {};
        struct Address : Column<5, NScheme::NTypeIds::Utf8> {};
        // struct DataCenter : Column<6, NScheme::NTypeIds::Uint64> {};
        // struct Room : Column<7, NScheme::NTypeIds::Uint64> {};
        // struct Rack : Column<8, NScheme::NTypeIds::Uint64> {};
        // struct Body : Column<9, NScheme::NTypeIds::Uint64> {};
        struct Lease : Column<10, NScheme::NTypeIds::Uint32> {};
        struct Expire : Column<11, NScheme::NTypeIds::Uint64> {};
        struct Location : Column<12, NScheme::NTypeIds::String> {};
        struct ServicedSubDomain : Column<13, NScheme::NTypeIds::String> { using Type = NKikimrSubDomains::TDomainKey; };
        struct SlotIndex : Column<14, NScheme::NTypeIds::Uint32> {};
        struct AuthorizedByCertificate : Column<15, NScheme::NTypeIds::Bool> {};
        struct BridgePileId : Column<16, NScheme::NTypeIds::Uint32> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<
            ID,
            Host,
            Port,
            ResolveHost,
            Address,
            Lease,
            Expire,
            Location,
            ServicedSubDomain,
            SlotIndex,
            AuthorizedByCertificate,
            BridgePileId
        >;
    };

    struct NodesV2 : Table<4> {
        struct NodeId : Column<1, NScheme::NTypeIds::Uint32> {};
        struct NodeInfo : Column<2, NScheme::NTypeIds::String> { using Type = NKikimrNodeBroker::TNodeInfoSchema; };
        struct State : Column<3, NScheme::NTypeIds::Uint8> { using Type = ENodeState; };
        struct Version : Column<4, NScheme::NTypeIds::Uint64> {};
        struct SchemaVersion : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<NodeId>;
        using TColumns = TableColumns<NodeId, NodeInfo, State, Version, SchemaVersion>;
    };

    enum EConfigKey : ui32 {
        ConfigKeyConfig = 1,
    };

    struct Config : Table<2> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    enum EParamKey : ui32 {
        ParamKeyConfigSubscription = 1,
        ParamKeyCurrentEpochId = 2,
        ParamKeyCurrentEpochVersion = 3,
        ParamKeyCurrentEpochStart = 4,
        ParamKeyCurrentEpochEnd = 5,
        ParamKeyNextEpochEnd = 6,
        ParamKeyApproximateEpochStartId = 7,
        ParamKeyApproximateEpochStartVersion = 8,
        ParamKeyMainNodesTable = 9,
    };

    struct Params : Table<3> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    using TTables = SchemaTables<Nodes, Config, Params, NodesV2>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                     ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

} // NNodeBroker
} // NKikimr
