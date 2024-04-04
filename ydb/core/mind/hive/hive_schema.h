#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/protos/metrics.pb.h>
#include "hive.h"

namespace NKikimr {
namespace NHive {

struct TSchemeIds {
    enum State {
        NextTabletId,
        DefaultState = NextTabletId,
        DatabaseVersion,
        MaxTabletsScheduled,
        MaxResourceCounter,
        MaxResourceCPU,
        MaxResourceMemory,
        MaxResourceNetwork,
        MinScatterToBalance,
        SpreadNeighbours,
        MaxBootBatchSize,
        DrainInflight,
        DefaultUnitIOPS,
        DefaultUnitThroughput,
        DefaultUnitSize,
        StorageOvercommit,
        StorageBalanceStrategy,
        StorageSafeMode,
        StorageSelectStrategy,
        RequestSequenceSize,
        MinRequestSequenceSize,
        MaxRequestSequenceSize,
        MetricsWindowSize,
        MaxNodeUsageToKick,
        ResourceChangeReactionPeriod,
        TabletKickCooldownPeriod,
        ResourceOvercommitment,
        TabletOwnersSynced,
    };
};

struct Schema : NIceDb::Schema {
    struct State : Table<0> {
        struct KeyCol : Column<0, NScheme::NTypeIds::Uint64> { static TString GetColumnName(const TString&) { return "Key"; } using Type = TSchemeIds::State; };
        struct Value : Column<1, NScheme::NTypeIds::Uint64> {};
        struct Config : Column<2, NScheme::NTypeIds::String> { using Type = NKikimrConfig::THiveConfig; };

        using TKey = TableKey<KeyCol>;
        using TColumns = TableColumns<KeyCol, Value, Config>;
    };

    struct OldTablet : Table<1> {
        struct ID : Column<0, NScheme::NTypeIds::Uint64> {};
        struct FollowerCount : Column<14, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 0; };
        struct AllowFollowerPromotion : Column<15, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct CrossDataCenterFollowers : Column<16, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct CrossDataCenterFollowerCount : Column<18, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 0; };

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, FollowerCount, AllowFollowerPromotion, CrossDataCenterFollowers, CrossDataCenterFollowerCount>;
    };

    struct Tablet : Table<1> {
        struct ID : Column<0, NScheme::NTypeIds::Uint64> {};
        struct Owner : Column<1, NScheme::NTypeIds::PairUi64Ui64> {};
        struct KnownGeneration : Column<2, NScheme::NTypeIds::Uint64> {};
        struct TabletType : Column<3, NScheme::NTypeIds::Uint64> { using Type = TTabletTypes::EType;  };
        struct LeaderNode : Column<4, NScheme::NTypeIds::Uint64> {};
        struct State : Column<5, NScheme::NTypeIds::Uint64> { using Type = ETabletState; };
        struct AllowedNodes : Column<7, NScheme::NTypeIds::String> { using Type = TVector<TNodeId>; };
        struct ActorToNotify : Column<11, NScheme::NTypeIds::ActorId> {}; // deprecated because of ActorsToNotify down here
        //struct Weight : Column<12, NScheme::NTypeIds::Uint64> {};
        struct Category : Column<13, NScheme::NTypeIds::Uint64> {};
        struct AllowedDataCenters : Column<17, NScheme::NTypeIds::String> { using Type = TVector<ui32>; };
        struct TabletStorageVersion : Column<18, NScheme::NTypeIds::Uint32> { static constexpr ui32 Default = 0; };
        struct ObjectID : Column<19, NScheme::NTypeIds::Uint64> { using Type = TObjectId; };
        struct ActorsToNotify : Column<111, NScheme::NTypeIds::String> { using Type = TVector<TActorId>; };
        struct AllowedDomains : Column<112, NScheme::NTypeIds::String> { using Type = TVector<TSubDomainKey>; }; //order sets priority
        struct BootMode : Column<113, NScheme::NTypeIds::Uint64> { using Type = NKikimrHive::ETabletBootMode; static constexpr NKikimrHive::ETabletBootMode Default = NKikimrHive::TABLET_BOOT_MODE_DEFAULT; };
        struct LockedToActor : Column<114, NScheme::NTypeIds::String> { using Type = TActorId; };
        struct LockedReconnectTimeout : Column<115, NScheme::NTypeIds::Uint64> { static constexpr ui64 Default = 0; };
        struct ObjectDomain : Column<116, NScheme::NTypeIds::String> { using Type = NKikimrSubDomains::TDomainKey; };

        struct SeizedByChild : Column<117, NScheme::NTypeIds::Bool> {};
        struct NeedToReleaseFromParent : Column<118, NScheme::NTypeIds::Bool> {};

        struct ReassignReason : Column<119, NScheme::NTypeIds::Uint64> {
            using Type = NKikimrHive::TEvReassignTablet::EHiveReassignReason;
            static constexpr NKikimrHive::TEvReassignTablet::EHiveReassignReason Default = NKikimrHive::TEvReassignTablet::HIVE_REASSIGN_REASON_NO;
        };
        struct Statistics : Column<120, NScheme::NTypeIds::String> { using Type = NKikimrHive::TTabletStatistics; };
        struct DataCentersPreference : Column<121, NScheme::NTypeIds::String> { using Type = NKikimrHive::TDataCentersPreference; };
        struct AllowedDataCenterIds : Column<122, NScheme::NTypeIds::String> { using Type = TVector<TString>; };

        struct BalancerPolicy : Column<123, NScheme::NTypeIds::Uint64> { using Type = NKikimrHive::EBalancerPolicy; static constexpr NKikimrHive::EBalancerPolicy Default = NKikimrHive::EBalancerPolicy::POLICY_BALANCE; };

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<
            ID,
            Owner,
            KnownGeneration,
            TabletType,
            LeaderNode,
            State,
            AllowedNodes,
            ActorToNotify,
            Category,
            AllowedDataCenters,
            TabletStorageVersion,
            ObjectID,
            ActorsToNotify,
            AllowedDomains,
            BootMode,
            LockedToActor,
            LockedReconnectTimeout,
            ObjectDomain,
            SeizedByChild,
            NeedToReleaseFromParent,
            ReassignReason,
            Statistics,
            DataCentersPreference,
            AllowedDataCenterIds,
            BalancerPolicy
        >;
    };

    struct TabletFollowerGroup : Table<9> {
        struct TabletID : Column<1, Schema::Tablet::ID::ColumnType> {};
        struct GroupID : Column<2, NScheme::NTypeIds::Uint32> {};
        struct FollowerCount : Column<3, NScheme::NTypeIds::Uint32> {};
        struct AllowLeaderPromotion : Column<4, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct AllowClientRead : Column<5, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct AllowedNodes : Column<6, NScheme::NTypeIds::String> { using Type = TVector<TNodeId>; };
        struct AllowedDataCenters : Column<7, NScheme::NTypeIds::String> { using Type = TVector<ui32>; };
        struct RequireAllDataCenters : Column<8, NScheme::NTypeIds::Bool> {};
        struct LocalNodeOnly : Column<9, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct FollowerCountPerDataCenter : Column<10, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct RequireDifferentNodes : Column<11, NScheme::NTypeIds::Bool> {};
        struct AllowedDataCenterIds : Column<12, NScheme::NTypeIds::String> { using Type = TVector<TString>; };

        using TKey = TableKey<TabletID, GroupID>;
        using TColumns = TableColumns<TabletID, GroupID, FollowerCount, AllowLeaderPromotion, AllowClientRead,
                                      AllowedNodes, AllowedDataCenters, RequireAllDataCenters, LocalNodeOnly,
                                      FollowerCountPerDataCenter, RequireDifferentNodes, AllowedDataCenterIds>;
    };

    struct TabletFollowerTablet : Table<10> {
        struct TabletID : Column<1, Schema::Tablet::ID::ColumnType> {};
        struct FollowerID : Column<2, NScheme::NTypeIds::Uint32> {};
        struct GroupID : Column<3, Schema::TabletFollowerGroup::GroupID::ColumnType> {};
        struct FollowerNode : Column<4, NScheme::NTypeIds::Uint32> {};
        struct Statistics : Column<5, NScheme::NTypeIds::String> { using Type = NKikimrHive::TTabletStatistics; };

        using TKey = TableKey<TabletID, FollowerID>;
        using TColumns = TableColumns<TabletID, GroupID, FollowerID, FollowerNode, Statistics>;
    };

    struct TabletChannel : Table<2> {
        struct Tablet : Column<0, Schema::Tablet::ID::ColumnType> {};
        struct Channel : Column<1, NScheme::NTypeIds::Uint64> {};
        //struct ErasureSpecies : Column<2, NScheme::NTypeIds::Uint64> { using Type = TBlobStorageGroupType::EErasureSpecies; };
        //struct PDiskCategory : Column<3, NScheme::NTypeIds::Uint64> {};
        //struct VDiskCategory : Column<4, NScheme::NTypeIds::Uint64> { using Type = NKikimrBlobStorage::TVDiskKind::EVDiskKind; };
        struct NeedNewGroup : Column<5, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct StoragePool : Column<7, NScheme::NTypeIds::Utf8> {};
        struct Binding : Column<8, NScheme::NTypeIds::String> { using Type = NKikimrStoragePool::TChannelBind; };

        using TKey = TableKey<Tablet, Channel>;
        using TColumns = TableColumns<Tablet, Channel, NeedNewGroup, StoragePool, Binding>;
    };

    struct TabletChannelGen : Table<3> {
        struct Tablet : Column<0, TabletChannel::Tablet::ColumnType> {};
        struct Channel : Column<1, TabletChannel::Channel::ColumnType> {};
        struct Generation : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Group : Column<3, NScheme::NTypeIds::Uint64> {};
        struct Version : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Timestamp : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Tablet, Channel, Generation>;
        using TColumns = TableColumns<Tablet, Channel, Generation, Group, Version, Timestamp>;
    };

    struct Node : Table<4> {
        struct ID : Column<0, NScheme::NTypeIds::Uint64> {};
        struct Local : Column<1, NScheme::NTypeIds::ActorId> {};
        struct Down : Column<2, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct Freeze : Column<3, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct ServicedDomains : Column<4, NScheme::NTypeIds::String> { using Type = TVector<TSubDomainKey>; };
        struct Statistics : Column<5, NScheme::NTypeIds::String> { using Type = NKikimrHive::TNodeStatistics; };
        struct Drain : Column<6, NScheme::NTypeIds::Bool> { static constexpr bool Default = false; };
        struct DrainInitiators : Column<8, NScheme::NTypeIds::String> { using Type = TVector<TActorId>; };
        struct Location : Column<9, NScheme::NTypeIds::String> { using Type = NActorsInterconnect::TNodeLocation; };
        struct Name : Column<10, NScheme::NTypeIds::String> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, Local, Down, Freeze, ServicedDomains, Statistics, Drain, DrainInitiators, Location, Name>;
    };

    struct TabletCategory : Table<6> {
        struct ID : Column<0, NScheme::NTypeIds::Uint64> {};
        struct MaxDisconnectTimeout : Column<1, NScheme::NTypeIds::Uint64> {};
        struct StickTogetherInDC : Column<2, NScheme::NTypeIds::Bool> {};

        using TKey = TableKey<ID>;
        using TColumns = TableColumns<ID, MaxDisconnectTimeout, StickTogetherInDC>;
    };

    struct OldTabletMetrics : Table<7> {
        struct TabletID : Column<1, Tablet::ID::ColumnType> {};

        struct CPU : Column<300 + (int)NMetrics::EResource::CPU, NScheme::NTypeIds::Uint64> {};
        struct Memory : Column<300 + (int)NMetrics::EResource::Memory, NScheme::NTypeIds::Uint64> {};
        struct Network : Column<300 + (int)NMetrics::EResource::Network, NScheme::NTypeIds::Uint64> {};
        struct Metrics : Column<500, NScheme::NTypeIds::String> { using Type = NKikimrTabletBase::TMetrics; };

        using TKey = TableKey<TabletID>;
        using TColumns = TableColumns<TabletID, CPU, Memory, Network, Metrics>;
    };

    struct Metrics : Table<16> {
        struct TabletID : Column<1, Tablet::ID::ColumnType> {};
        struct FollowerID : Column<2, TabletFollowerTablet::FollowerID::ColumnType> {};
        struct ProtoMetrics : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrTabletBase::TMetrics; };

        struct MaximumCPU : Column<100 + (int)NMetrics::EResource::CPU, NScheme::NTypeIds::String> { using Type = NKikimrMetricsProto::TMaximumValueUI64; };
        struct MaximumMemory : Column<100 + (int)NMetrics::EResource::Memory, NScheme::NTypeIds::String> { using Type = NKikimrMetricsProto::TMaximumValueUI64; };
        struct MaximumNetwork : Column<100 + (int)NMetrics::EResource::Network, NScheme::NTypeIds::String> { using Type = NKikimrMetricsProto::TMaximumValueUI64; };

        using TKey = TableKey<TabletID, FollowerID>;
        using TColumns = TableColumns<TabletID, FollowerID, ProtoMetrics, MaximumCPU, MaximumMemory, MaximumNetwork>;
    };

    struct TabletTypeMetrics : Table<13> {
        struct TabletType : Column<1, Tablet::TabletType::ColumnType> { using Type = TTabletTypes::EType; };
        struct AllowedMetricIDs : Column<2, NScheme::NTypeIds::String> { using Type = TVector<i64>; };

        using TKey = TableKey<TabletType>;
        using TColumns = TableColumns<TabletType, AllowedMetricIDs>;
    };

    struct Sequences : Table<14> {
        struct OwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct OwnerIdx : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Begin : Column<3, NScheme::NTypeIds::Uint64> {};
        struct End : Column<4, NScheme::NTypeIds::Uint64> {};
        struct Next : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerId, OwnerIdx>;
        using TColumns = TableColumns<OwnerId, OwnerIdx, Begin, End, Next>;
    };

    /*struct PendingCreateTablet : Table<15> {
        struct OwnerId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct OwnerIdx : Column<2, NScheme::NTypeIds::Uint64> {};
        struct RequestData : Column<3, NScheme::NTypeIds::String> { using Type = NKikimrHive::TEvCreateTablet; };
        struct Sender : Column<4, NScheme::NTypeIds::ActorId> {};
        struct Cookie : Column<5, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerId, OwnerIdx>;
        using TColumns = TableColumns<OwnerId, OwnerIdx, RequestData, Sender, Cookie>;
    };*/

    struct SubDomain : Table<17> {
        struct SchemeshardId : Column<1, NScheme::NTypeIds::Uint64> {};
        struct PathId : Column<2, NScheme::NTypeIds::Uint64> {};
        struct Path : Column<3, NScheme::NTypeIds::Utf8> {};
        struct Primary : Column<4, NScheme::NTypeIds::Bool> {};
        struct HiveId : Column<5, NScheme::NTypeIds::Uint64> {};
        struct ServerlessComputeResourcesMode : Column<6, NScheme::NTypeIds::Uint32> { using Type = NKikimrSubDomains::EServerlessComputeResourcesMode; };

        using TKey = TableKey<SchemeshardId, PathId>;
        using TColumns = TableColumns<SchemeshardId, PathId, Path, Primary, HiveId, ServerlessComputeResourcesMode>;
    };

    struct BlockedOwner : Table<18> {
        struct OwnerId : Column<1, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<OwnerId>;
        using TColumns = TableColumns<OwnerId>;
    };

    struct TabletOwners : Table<19> {
        struct Begin : Column<1, NScheme::NTypeIds::Uint64> {};
        struct End : Column<2, NScheme::NTypeIds::Uint64> {};
        struct OwnerId : Column<3, NScheme::NTypeIds::Uint64> {};

        using TKey = TableKey<Begin, End>;
        using TColumns = TableColumns<Begin, End, OwnerId>;
    };

    using TTables = SchemaTables<
                                State,
                                Tablet,
                                TabletChannel,
                                TabletChannelGen,
                                Node,
                                TabletCategory,
                                TabletFollowerGroup,
                                TabletFollowerTablet,
                                TabletTypeMetrics,
                                Sequences,
                                Metrics,
                                SubDomain,
                                BlockedOwner,
                                TabletOwners
                                >;
    using TSettings = SchemaSettings<
                                    ExecutorLogBatching<true>,
                                    ExecutorLogFlushPeriod<TDuration::MicroSeconds(512).GetValue()>
                                    >;
};

} // NHive
} // NKikimr
