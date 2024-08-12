#pragma once

#include "defs.h"

#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NKikimr::NConsole {

struct Schema : NIceDb::Schema {
    struct Config : Table<1> {
        struct Key : Column<1, NScheme::NTypeIds::Uint32> {};
        struct Value : Column<2, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Key>;
        using TColumns = TableColumns<Key, Value>;
    };

    struct Tenants : Table<2> {
        struct Path : Column<1, NScheme::NTypeIds::Utf8> {};
        struct State : Column<2, NScheme::NTypeIds::Uint32> {};
        struct Coordinators : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Mediators : Column<4, NScheme::NTypeIds::Uint64> {};
        struct PlanResolution : Column<5, NScheme::NTypeIds::Uint64> {};
        struct Issue : Column<6, NScheme::NTypeIds::Utf8> {};
        struct TxId : Column<7, NScheme::NTypeIds::Uint64> {};
        struct UserToken : Column<8, NScheme::NTypeIds::Utf8> {};
        struct SubdomainVersion : Column<10, NScheme::NTypeIds::Uint64> {};
        struct ConfirmedSubdomain : Column<11, NScheme::NTypeIds::Uint64> {};
        struct TimeCastBucketsPerMediator : Column<12, NScheme::NTypeIds::Uint32> {};
        struct Attributes : Column<13, NScheme::NTypeIds::String> { using Type = NKikimrSchemeOp::TAlterUserAttributes; };
        struct Generation : Column<14, NScheme::NTypeIds::Uint64> {};
        // DomainId {
        struct SchemeShardId : Column<15, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<16, NScheme::NTypeIds::Uint64> {};
        // } // DomainId
        struct ErrorCode : Column<17, NScheme::NTypeIds::Uint32> {};
        struct IsExternalSubDomain : Column<18, NScheme::NTypeIds::Bool> {};
        struct IsExternalHive : Column<19, NScheme::NTypeIds::Bool> {};
        struct AreResourcesShared : Column<20, NScheme::NTypeIds::Bool> {};
        // SharedDomainId {
        struct SharedDomainSchemeShardId : Column<21, NScheme::NTypeIds::Uint64> {};
        struct SharedDomainPathId : Column<22, NScheme::NTypeIds::Uint64> {};
        struct IsExternalSysViewProcessor : Column<23, NScheme::NTypeIds::Bool> {};
        // } // SharedDomainId
        struct SchemaOperationQuotas : Column<24, NScheme::NTypeIds::String> {};
        struct CreateIdempotencyKey : Column<25, NScheme::NTypeIds::Utf8> {};
        struct AlterIdempotencyKey : Column<26, NScheme::NTypeIds::Utf8> {};
        struct DatabaseQuotas : Column<27, NScheme::NTypeIds::String> {};
        struct IsExternalStatisticsAggregator : Column<28, NScheme::NTypeIds::Bool> {};
        struct IsExternalBackupController : Column<29, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Path>;
        using TColumns = TableColumns<Path, State, Coordinators, Mediators, PlanResolution,
            Issue, TxId, UserToken, SubdomainVersion, ConfirmedSubdomain, TimeCastBucketsPerMediator,
            Attributes, Generation, SchemeShardId, PathId, ErrorCode, IsExternalSubDomain, IsExternalHive,
            AreResourcesShared, SharedDomainSchemeShardId, SharedDomainPathId, IsExternalSysViewProcessor,
            SchemaOperationQuotas, CreateIdempotencyKey, AlterIdempotencyKey, DatabaseQuotas, IsExternalStatisticsAggregator,
            IsExternalBackupController>;
    };

    struct TenantPools : Table<3> {
        struct Tenant : Column<1, NScheme::NTypeIds::Utf8> {};
        struct PoolType : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Config : Column<3, NScheme::NTypeIds::Utf8> {};
        struct AllocatedNumGroups : Column<4, NScheme::NTypeIds::Uint32> {};
        struct State : Column<5, NScheme::NTypeIds::Uint32> {};
        struct Borrowed : Column<6, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Tenant, PoolType>;
        using TColumns = TableColumns<Tenant, PoolType, Config, AllocatedNumGroups, State, Borrowed>;
    };

    struct TenantUnits : Table<4> {
        struct Tenant : Column<1, NScheme::NTypeIds::Utf8> {};
        struct UnitKind : Column<2, NScheme::NTypeIds::Utf8> {};
        struct AvailabilityZone : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Count : Column<4, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Tenant, UnitKind, AvailabilityZone>;
        using TColumns = TableColumns<Tenant, UnitKind, AvailabilityZone, Count>;
    };

    struct RemovedTenants : Table<5> {
        struct Path : Column<1, NScheme::NTypeIds::Utf8> {};
        struct TxId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Issue : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Code : Column<4, NScheme::NTypeIds::Uint32> {};
        struct ErrorCode : Column<17, NScheme::NTypeIds::Uint32> {};
        struct CreateIdempotencyKey : Column<25, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Path>;
        using TColumns = TableColumns<Path, TxId, Issue, Code, ErrorCode, CreateIdempotencyKey>;
    };

    struct RegisteredUnits : Table<6> {
        struct Tenant : Column<1, NScheme::NTypeIds::Utf8> {};
        struct Host : Column<2, NScheme::NTypeIds::Utf8> {};
        struct Port : Column<3, NScheme::NTypeIds::Uint32> {};
        struct Kind : Column<4, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Tenant, Host, Port>;
        using TColumns = TableColumns<Tenant, Host, Port, Kind>;
    };

    struct LogRecords : Table<7> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Timestamp : Column<2, NScheme::NTypeIds::Uint64> {};
        struct UserSID : Column<3, NScheme::NTypeIds::String> {};
        struct Data : Column<4, NScheme::NTypeIds::String> {};

        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Timestamp, UserSID, Data>;
    };

    struct ConfigItems : Table<100> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Generation : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Kind : Column<3, NScheme::NTypeIds::Uint32> {};
        struct NodeIds : Column<4, NScheme::NTypeIds::String> { using Type = TVector<ui32>; };
        struct Hosts : Column<5, NScheme::NTypeIds::String> {};
        struct Tenant : Column<6, NScheme::NTypeIds::Utf8> {};
        struct NodeType : Column<7, NScheme::NTypeIds::Utf8> {};
        struct Order : Column<8, NScheme::NTypeIds::Uint32> {};
        struct Merge : Column<9, NScheme::NTypeIds::Uint32> {};
        struct Config : Column<10, NScheme::NTypeIds::String> {};
        struct Cookie : Column<11, NScheme::NTypeIds::String> {};


        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, Generation, Kind, NodeIds, Hosts, Tenant, NodeType, Order, Merge, Config, Cookie>;
    };

    struct ConfigSubscriptions : Table<101> {
        struct Id : Column<1, NScheme::NTypeIds::Uint64> {};
        struct TabletId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct ServiceId : Column<3, NScheme::NTypeIds::ActorId> {};
        struct NodeId : Column<4, NScheme::NTypeIds::Uint32> {};
        struct Host : Column<5, NScheme::NTypeIds::Utf8> {};
        struct Tenant : Column<6, NScheme::NTypeIds::Utf8> {};
        struct NodeType : Column<7, NScheme::NTypeIds::Utf8> {};
        struct ItemKinds : Column<8, NScheme::NTypeIds::String> { using Type = TVector<ui32>; };
        struct LastProvidedConfig : Column<9, NScheme::NTypeIds::String> { using Type = TVector<std::pair<ui64, ui64>>; };


        using TKey = TableKey<Id>;
        using TColumns = TableColumns<Id, TabletId, ServiceId, NodeId, Host, Tenant, NodeType, ItemKinds, LastProvidedConfig>;
    };

    struct DisabledValidators : Table<102> {
        struct Name : Column<1, NScheme::NTypeIds::Utf8> {};

        using TKey = TableKey<Name>;
        using TColumns = TableColumns<Name>;
    };

    struct YamlConfig : Table<103> {
        struct Version : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> {};
        struct Dropped : Column<3, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<Version>;
        using TColumns = TableColumns<Version, Config, Dropped>;
    };

    using TTables = SchemaTables<Config, Tenants, TenantPools, TenantUnits, RemovedTenants,
                                 RegisteredUnits, LogRecords, ConfigItems, ConfigSubscriptions, DisabledValidators,
                                 YamlConfig>;
    using TSettings = SchemaSettings<ExecutorLogBatching<true>,
                                     ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>>;
};

} // namespace NKikimr::NConsole
