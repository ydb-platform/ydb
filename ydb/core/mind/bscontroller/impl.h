#pragma once
#include "defs.h"
#include "bsc.h"
#include "scheme.h"
#include "mood.h"
#include "types.h"
#include "resources.h"
#include "stat_processor.h"
#include "indir.h"
#include "self_heal.h"
#include "storage_pool_stat.h"

inline IOutputStream& operator <<(IOutputStream& o, NKikimr::TErasureType::EErasureSpecies p) {
    return o << NKikimr::TErasureType::ErasureSpeciesName(p);
}

namespace NKikimr {

namespace NBsController {

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

class TRequestCounter {
    TTabletCountersBase *Counters;
    THPTimer Timer;
    int MicrosecIndex;

public:
    TRequestCounter(TTabletCountersBase *counters, int microsecIndex)
        : Counters(counters)
        , MicrosecIndex(microsecIndex)
    {}

    ~TRequestCounter() {
        TDuration passed = TDuration::Seconds(Timer.Passed());
        Counters->Cumulative()[MicrosecIndex].Increment(passed.MicroSeconds());
    }
};

class TBlobStorageController : public TActor<TBlobStorageController>, public TTabletExecutedFlat {
public:
    using THostConfigId = Schema::HostConfig::TKey::Type;
    using THostId = std::tuple<TString, i32>; // (Host, IcPort) identifier

    class TTxInitScheme;
    class TTxMigrate;
    class TTxLoadEverything;
    class TTxMonEvent_OperationLog;
    class TTxMonEvent_OperationLogEntry;
    class TTxMonEvent_HealthEvents;
    class TTxMonEvent_SetDown;
    class TTxMonEvent_GetDown;
    class TTxUpdateDiskMetrics;
    class TTxUpdateGroupLatencies;
    class TTxGroupMetricsExchange;
    class TTxNodeReport;
    class TTxUpdateSeenOperational;
    class TTxConfigCmd;
    class TTxProposeGroupKey;
    class TTxRegisterNode;
    class TTxGetGroup;
    class TTxRequestControllerInfo;
    class TTxSelectGroups;
    class TTxDropDonor;
    class TTxScrubStart;
    class TTxScrubQuantumFinished;
    class TTxUpdateLastSeenReady;
    class TTxUpdateNodeDrives;
    class TTxUpdateNodeDisconnectTimestamp;

    class TVSlotInfo;
    class TPDiskInfo;
    class TGroupInfo;
    class TConfigState;
    class TGroupSelector;
    class TGroupFitter;
    class TSelfHealActor;

    using TVSlotReadyTimestampQ = std::list<std::pair<TMonotonic, TVSlotInfo*>>;

    // VDisk will be considered READY during this period after reporting its READY state
    static constexpr TDuration ReadyStablePeriod = TDuration::Seconds(15);

    class TVSlotInfo : public TIndirectReferable<TVSlotInfo> {
    public:
        using Table = Schema::VSlot;

        const TVSlotId VSlotId;
        TIndirectReferable<TPDiskInfo>::TPtr PDisk; // PDisk this slot resides on
        TGroupId GroupId = TGroupId::Zero();
        Table::GroupGeneration::Type GroupPrevGeneration = 0;
        Table::GroupGeneration::Type GroupGeneration = 0;
        Table::Category::Type Kind = NKikimrBlobStorage::TVDiskKind::Default;
        Table::RingIdx::Type RingIdx = 0;
        Table::FailDomainIdx::Type FailDomainIdx = 0;
        Table::VDiskIdx::Type VDiskIdx = 0;
        Table::Mood::Type Mood;
        TIndirectReferable<TGroupInfo>::TPtr Group; // group to which this VSlot belongs (or nullptr if it doesn't belong to any)
        THashSet<TVSlotId> Donors; // a set of alive donors for this disk (which are not being deleted)
        TInstant LastSeenReady;
        TInstant LastGotReplicating;
        TDuration ReplicationTime;

        // volatile state
        mutable NKikimrBlobStorage::TVDiskMetrics Metrics;
        mutable bool MetricsDirty = false;
        mutable TResourceRawValues DiskResourceValues;
        mutable TResourceRawValues MaximumResourceValues{
            1ULL << 40, // 1 TB
        };

        // not persistent field; set only when VSlot is scheduled for deletion
        std::optional<TBoxStoragePoolId> DeletedFromStoragePoolId;

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // VDisk status management
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private:
        TVSlotReadyTimestampQ& VSlotReadyTimestampQ;
        TVSlotReadyTimestampQ::iterator VSlotReadyTimestampIter;

    public:
        std::optional<NKikimrBlobStorage::EVDiskStatus> VDiskStatus;
        NHPTimer::STime VDiskStatusTimestamp = GetCycleCountFast();
        bool IsReady = false;
        bool OnlyPhantomsRemain = false;

    public:
        void SetStatus(NKikimrBlobStorage::EVDiskStatus status, TMonotonic now, TInstant instant, bool onlyPhantomsRemain) {
            if (status != VDiskStatus) {
                if (status == NKikimrBlobStorage::EVDiskStatus::REPLICATING) { // became "replicating"
                    LastGotReplicating = instant;
                } else if (VDiskStatus == NKikimrBlobStorage::EVDiskStatus::REPLICATING) { // was "replicating"
                    Y_DEBUG_ABORT_UNLESS(LastGotReplicating != TInstant::Zero());
                    ReplicationTime += instant - LastGotReplicating;
                    LastGotReplicating = {};
                }
                if (status == NKikimrBlobStorage::EVDiskStatus::READY) {
                    ReplicationTime = TDuration::Zero();
                }
                if (IsReady) {
                    LastSeenReady = instant;
                }

                VDiskStatus = status;
                IsReady = false;
                if (status == NKikimrBlobStorage::EVDiskStatus::READY) {
                    PutInVSlotReadyTimestampQ(now);
                } else {
                    DropFromVSlotReadyTimestampQ();
                }
                const_cast<TGroupInfo&>(*Group).CalculateGroupStatus();
            }
            if (status == NKikimrBlobStorage::EVDiskStatus::REPLICATING) {
                OnlyPhantomsRemain = onlyPhantomsRemain;
            }
        }

        NKikimrBlobStorage::EVDiskStatus GetStatus() const {
            return VDiskStatus.value_or(NKikimrBlobStorage::EVDiskStatus::ERROR);
        }

        void PutInVSlotReadyTimestampQ(TMonotonic now) {
            const TMonotonic readyAfter = now + ReadyStablePeriod; // vdisk will be treated as READY one shortly, but not now
            Y_ABORT_UNLESS(VSlotReadyTimestampIter == TVSlotReadyTimestampQ::iterator());
            Y_ABORT_UNLESS(Group);
            Y_DEBUG_ABORT_UNLESS(VSlotReadyTimestampQ.empty() || VSlotReadyTimestampQ.back().first <= readyAfter);
            VSlotReadyTimestampIter = VSlotReadyTimestampQ.emplace(VSlotReadyTimestampQ.end(), readyAfter, this);
        }

        void DropFromVSlotReadyTimestampQ() {
            if (VSlotReadyTimestampIter != TVSlotReadyTimestampQ::iterator()) {
                VSlotReadyTimestampQ.erase(VSlotReadyTimestampIter);
                ResetVSlotReadyTimestampIter();
            }
        }

        void ResetVSlotReadyTimestampIter() {
            VSlotReadyTimestampIter = {};
        }

        bool IsInVSlotReadyTimestampQ() const {
            return VSlotReadyTimestampIter != TVSlotReadyTimestampQ::iterator();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TVSlotInfo,
                    Table::Category,
                    Table::GroupID,
                    Table::GroupGeneration,
                    Table::RingIdx,
                    Table::FailDomainIdx,
                    Table::VDiskIdx,
                    Table::GroupPrevGeneration,
                    Table::Mood,
                    Table::LastSeenReady,
                    Table::LastGotReplicating,
                    Table::ReplicationTime
                > adapter(
                    &TVSlotInfo::Kind,
                    &TVSlotInfo::GroupId,
                    &TVSlotInfo::GroupGeneration,
                    &TVSlotInfo::RingIdx,
                    &TVSlotInfo::FailDomainIdx,
                    &TVSlotInfo::VDiskIdx,
                    &TVSlotInfo::GroupPrevGeneration,
                    &TVSlotInfo::Mood,
                    &TVSlotInfo::LastSeenReady,
                    &TVSlotInfo::LastGotReplicating,
                    &TVSlotInfo::ReplicationTime
                );
            callback(&adapter);
        }

        TVSlotInfo() = delete;

        TVSlotInfo(TVSlotId vSlotId, TPDiskInfo *pdisk, TGroupId groupId, Table::GroupGeneration::Type groupPrevGeneration,
                Table::GroupGeneration::Type groupGeneration, Table::Category::Type kind, Table::RingIdx::Type ringIdx,
                Table::FailDomainIdx::Type failDomainIdx, Table::VDiskIdx::Type vDiskIdx, Table::Mood::Type mood,
                TGroupInfo *group, TVSlotReadyTimestampQ *vslotReadyTimestampQ, TInstant lastSeenReady,
                TDuration replicationTime); // implemented in bsc.cpp

        // is the slot being deleted (marked as deleted)
        bool IsBeingDeleted() const {
            return Mood == TMood::Delete;
        }

        void ScheduleForDeletion(TBoxStoragePoolId deletedFromStoragePoolId) {
            Y_ABORT_UNLESS(!IsBeingDeleted());
            Mood = TMood::Delete;
            GroupPrevGeneration = std::exchange(GroupGeneration, 0);
            Group = nullptr;
            DeletedFromStoragePoolId = deletedFromStoragePoolId;
        }

        void MakeDonorFor(TVSlotInfo *newSlot) {
            Y_ABORT_UNLESS(newSlot);
            Y_ABORT_UNLESS(!IsBeingDeleted());
            Y_ABORT_UNLESS(GroupId == newSlot->GroupId);
            Y_ABORT_UNLESS(GetShortVDiskId() == newSlot->GetShortVDiskId());
            Mood = TMood::Donor;
            Group = nullptr; // we are not part of this group anymore
            Donors.insert(VSlotId);
            Donors.swap(newSlot->Donors);
            Y_ABORT_UNLESS(Donors.empty());
        }

        TVDiskID GetVDiskId() const {
            ui32 generation = IsBeingDeleted() // when the slot is scheduled for deletion, its last known generation is
                ? GroupPrevGeneration          // stored in GroupPrevGeneration
                : GroupGeneration;             // otherwise actual value is held in GroupGeneration
            Y_ABORT_UNLESS(generation);
            return TVDiskID(GroupId, generation, RingIdx, FailDomainIdx, VDiskIdx);
        }

        TVDiskIdShort GetShortVDiskId() const {
            return TVDiskIdShort(RingIdx, FailDomainIdx, VDiskIdx);
        }

        bool IsSameVDisk(const TVDiskID &id) {
            return (GroupId == id.GroupID &&
                    RingIdx == id.FailRealm &&
                    FailDomainIdx == id.FailDomain &&
                    VDiskIdx == id.VDisk);
        }

        void UpdateVDiskMetrics() const {
            DiskResourceValues.DataSize = Metrics.GetAllocatedSize();
            MaximumResourceValues.DataSize = Metrics.GetAvailableSize() + DiskResourceValues.DataSize;
        }

        bool UpdateVDiskMetrics(const NKikimrBlobStorage::TVDiskMetrics& vDiskMetrics, i64 *allocatedSizeIncrementPtr) const {
            const ui64 allocatedSizeBefore = Metrics.GetAllocatedSize();
            const ui32 prevStatusFlags = Metrics.GetStatusFlags();
            Metrics.MergeFrom(vDiskMetrics);
            MetricsDirty = true;
            UpdateVDiskMetrics();
            *allocatedSizeIncrementPtr = Metrics.GetAllocatedSize() - allocatedSizeBefore;
            return prevStatusFlags != Metrics.GetStatusFlags();
        }

        TResourceRawValues GetResourceCurrentValues() const {
            return DiskResourceValues;
        }

        TResourceRawValues GetResourceMaximumValues() const {
            return MaximumResourceValues;
        }

        TString GetStatusString() const {
            TStringStream s;
            const auto status = GetStatus();
            s << NKikimrBlobStorage::EVDiskStatus_Name(status);
            if (status == NKikimrBlobStorage::REPLICATING && OnlyPhantomsRemain) {
                s << "/p";
            }
            return s.Str();
        }

        bool IsOperational() const {
            return GetStatus() >= NKikimrBlobStorage::REPLICATING;
        }

        void OnCommit();
    };

    using TVSlots = TMap<TVSlotId, THolder<TVSlotInfo>>;

    class TPDiskInfo : public TIndirectReferable<TPDiskInfo> {
    public:
        using Table = Schema::PDisk;

        THostId HostId;
        Table::Path::Type Path;
        Table::Category::Type Kind = 0;
        Table::Guid::Type Guid = 0;
        TMaybe<Table::SharedWithOs::Type> SharedWithOs; // null on old versions
        TMaybe<Table::ReadCentric::Type> ReadCentric; // null on old versions
        Table::NextVSlotId::Type NextVSlotId; // null on old versions
        Table::PDiskConfig::Type PDiskConfig;
        TBoxId BoxId;
        ui32 ExpectedSlotCount = 0;
        bool HasExpectedSlotCount = false;
        ui32 NumActiveSlots = 0; // number of active VSlots created over this PDisk
        TMap<Schema::VSlot::VSlotID::Type, TIndirectReferable<TVSlotInfo>::TPtr> VSlotsOnPDisk; // vslots over this PDisk

        bool Operational = false; // set to true when both containing node is connected and Operational is reported in Metrics

        NKikimrBlobStorage::TPDiskMetrics Metrics;
        bool MetricsDirty = false;

        NKikimrBlobStorage::EDriveStatus Status;
        TInstant StatusTimestamp;
        NKikimrBlobStorage::EDecommitStatus DecommitStatus;
        Table::Mood::Type Mood;
        TString ExpectedSerial;
        TString LastSeenSerial;
        TString LastSeenPath;
        const ui32 StaticSlotUsage = 0;

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TPDiskInfo,
                    Table::Path,
                    Table::Category,
                    Table::Guid,
                    Table::SharedWithOs,
                    Table::ReadCentric,
                    Table::NextVSlotId,
                    Table::PDiskConfig,
                    Table::Status,
                    Table::Timestamp,
                    Table::Mood,
                    Table::ExpectedSerial,
                    Table::LastSeenSerial,
                    Table::LastSeenPath,
                    Table::DecommitStatus
                > adapter(
                    &TPDiskInfo::Path,
                    &TPDiskInfo::Kind,
                    &TPDiskInfo::Guid,
                    &TPDiskInfo::SharedWithOs,
                    &TPDiskInfo::ReadCentric,
                    &TPDiskInfo::NextVSlotId,
                    &TPDiskInfo::PDiskConfig,
                    &TPDiskInfo::Status,
                    &TPDiskInfo::StatusTimestamp,
                    &TPDiskInfo::Mood,
                    &TPDiskInfo::ExpectedSerial,
                    &TPDiskInfo::LastSeenSerial,
                    &TPDiskInfo::LastSeenPath,
                    &TPDiskInfo::DecommitStatus
                );
            callback(&adapter);
        }

        TPDiskInfo(THostId hostId,
                   Table::Path::Type path,
                   Table::Category::Type kind,
                   Table::Guid::Type guid,
                   TMaybe<Table::SharedWithOs::Type> sharedWithOs,
                   TMaybe<Table::ReadCentric::Type> readCentric,
                   Table::NextVSlotId::Type nextVSlotId,
                   Table::PDiskConfig::Type pdiskConfig,
                   TBoxId boxId,
                   ui32 defaultMaxSlots,
                   NKikimrBlobStorage::EDriveStatus status,
                   TInstant statusTimestamp,
                   NKikimrBlobStorage::EDecommitStatus decommitStatus,
                   Table::Mood::Type mood,
                   const TString& expectedSerial,
                   const TString& lastSeenSerial,
                   const TString& lastSeenPath,
                   ui32 staticSlotUsage)
            : HostId(hostId)
            , Path(path)
            , Kind(kind)
            , Guid(guid)
            , SharedWithOs(sharedWithOs)
            , ReadCentric(readCentric)
            , NextVSlotId(nextVSlotId)
            , PDiskConfig(std::move(pdiskConfig))
            , BoxId(boxId)
            , Status(status)
            , StatusTimestamp(statusTimestamp)
            , DecommitStatus(decommitStatus)
            , Mood(mood)
            , ExpectedSerial(expectedSerial)
            , LastSeenSerial(lastSeenSerial)
            , LastSeenPath(lastSeenPath)
            , StaticSlotUsage(staticSlotUsage)
        {
            ExtractConfig(defaultMaxSlots);
        }

        void ExtractConfig(ui32 defaultMaxSlots) {
            ExpectedSlotCount = defaultMaxSlots;

            NKikimrBlobStorage::TPDiskConfig pdiskConfig;
            if (pdiskConfig.ParseFromString(PDiskConfig) && pdiskConfig.HasExpectedSlotCount()) {
                ExpectedSlotCount = pdiskConfig.GetExpectedSlotCount();
                HasExpectedSlotCount = true;
            }
        }

        bool SlotSpaceEnforced(TBlobStorageController& self) const {
            return Metrics.HasEnforcedDynamicSlotSize() &&
                self.PDiskSpaceColorBorder >= NKikimrBlobStorage::TPDiskSpaceColor::YELLOW;
        }

        bool HasFullMetrics() const {
            return Metrics.GetTotalSize()
                && Metrics.HasMaxIOPS()
                && Metrics.HasMaxReadThroughput()
                && Metrics.HasMaxWriteThroughput();
        }

        bool UpdatePDiskMetrics(const NKikimrBlobStorage::TPDiskMetrics& pDiskMetrics, TInstant now) {
            const bool hadMetrics = HasFullMetrics();
            Metrics.CopyFrom(pDiskMetrics);
            Metrics.SetUpdateTimestamp(now.GetValue());
            MetricsDirty = true;
            return !hadMetrics && HasFullMetrics(); // true if metrics have just arrived
        }

        void UpdateOperational(bool nodeConnected) {
            Operational = nodeConnected && (!Metrics.HasState() ||
                Metrics.GetState() == NKikimrBlobStorage::TPDiskState::Normal);
        }

        bool ShouldBeSettledBySelfHeal() const {
            return Status == NKikimrBlobStorage::EDriveStatus::FAULTY
                || Status == NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED
                || DecommitStatus == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT;
        }

        bool IsSelfHealReasonDecommit() const {
            return DecommitStatus == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT &&
                Status != NKikimrBlobStorage::EDriveStatus::FAULTY &&
                Status != NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED;
        }

        bool UsableInTermsOfDecommission(bool isSelfHealReasonDecommit) const {
            return DecommitStatus == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE // acceptable in any case
                || DecommitStatus == NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED && !isSelfHealReasonDecommit;
        }

        bool BadInTermsOfSelfHeal() const {
            return Status == NKikimrBlobStorage::EDriveStatus::FAULTY
                || Status == NKikimrBlobStorage::EDriveStatus::INACTIVE;
        }

        auto GetSelfHealStatusTuple() const {
            return std::make_tuple(ShouldBeSettledBySelfHeal(), BadInTermsOfSelfHeal(), Decommitted(), IsSelfHealReasonDecommit());
        }

        bool AcceptsNewSlots() const {
            return Status == NKikimrBlobStorage::EDriveStatus::ACTIVE;
        }

        bool Decommitted() const {
            switch (DecommitStatus) {
                case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_NONE:
                    return false;
                case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_PENDING:
                case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_IMMINENT:
                case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_REJECTED:
                    return true;
                case NKikimrBlobStorage::EDecommitStatus::DECOMMIT_UNSET:
                case NKikimrBlobStorage::EDecommitStatus::EDecommitStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBlobStorage::EDecommitStatus::EDecommitStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
                    break;
            }
            Y_ABORT("unexpected EDecommitStatus");
        }

        bool HasGoodExpectedStatus() const {
            switch (Status) {
                case NKikimrBlobStorage::EDriveStatus::UNKNOWN:
                case NKikimrBlobStorage::EDriveStatus::BROKEN:
                case NKikimrBlobStorage::EDriveStatus::INACTIVE:
                case NKikimrBlobStorage::EDriveStatus::FAULTY:
                case NKikimrBlobStorage::EDriveStatus::TO_BE_REMOVED:
                    return false;

                case NKikimrBlobStorage::EDriveStatus::ACTIVE:
                    return true;

                case NKikimrBlobStorage::EDriveStatus::EDriveStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
                case NKikimrBlobStorage::EDriveStatus::EDriveStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
                    break;
            }
            Y_ABORT("unexpected EDriveStatus");
        }

        TString PathOrSerial() const {
            return Path ? Path : ExpectedSerial;
        }

        void OnCommit();
    };

    using TPDisks = TMap<TPDiskId, THolder<TPDiskInfo>>;

    using TGroupSpecies = std::tuple<Schema::Group::ErasureSpecies::Type,
                                     Schema::Group::DesiredPDiskCategory::Type,
                                     Schema::Group::DesiredVDiskCategory::Type>;

    class TGroupInfo : public TIndirectReferable<TGroupInfo> {
    public:
        using Table = Schema::Group;

        TGroupId ID;
        Table::Generation::Type Generation = 0;
        Table::Owner::Type Owner = 0;
        Table::ErasureSpecies::Type ErasureSpecies = Schema::Group::ErasureSpecies::Type();
        Table::DesiredPDiskCategory::Type DesiredPDiskCategory = 0;
        Table::DesiredVDiskCategory::Type DesiredVDiskCategory = NKikimrBlobStorage::TVDiskKind::Default;
        TMaybe<Table::EncryptionMode::Type> EncryptionMode; // null on old versions
        TMaybe<Table::LifeCyclePhase::Type> LifeCyclePhase; // null on old versions
        TMaybe<Table::MainKeyId::Type> MainKeyId; // null on old versions
        TMaybe<Table::EncryptedGroupKey::Type> EncryptedGroupKey; // null on old versions
        TMaybe<Table::GroupKeyNonce::Type> GroupKeyNonce; // null on old versions
        TMaybe<Table::MainKeyVersion::Type> MainKeyVersion; // null on old verstions
        bool PersistedDown = false; // the value stored in the database
        bool SeenOperational = false;

        Table::DecommitStatus::Type DecommitStatus = NKikimrBlobStorage::TGroupDecommitStatus::NONE;

        TMaybe<Table::VirtualGroupName::Type> VirtualGroupName;
        TMaybe<Table::VirtualGroupState::Type> VirtualGroupState;
        TMaybe<Table::HiveId::Type> HiveId;
        TMaybe<Table::Database::Type> Database;
        TMaybe<Table::BlobDepotConfig::Type> BlobDepotConfig;
        TMaybe<Table::BlobDepotId::Type> BlobDepotId;
        TMaybe<Table::ErrorReason::Type> ErrorReason;
        TMaybe<Table::NeedAlter::Type> NeedAlter;
        std::optional<NKikimrBlobStorage::TGroupMetrics> GroupMetrics;

        bool Down = false; // is group are down right now (not selectable)
        TVector<TIndirectReferable<TVSlotInfo>::TPtr> VDisksInGroup;
        THashSet<TVSlotId> VSlotsBeingDeleted;
        TGroupLatencyStats LatencyStats;
        TBoxStoragePoolId StoragePoolId;
        mutable TStorageStatusFlags StatusFlags;

        TActorId VirtualGroupSetupMachineId;

        // nodes waiting for this group to become listable
        THashSet<TNodeId> WaitingNodes;

        // group's geometry; it doesn't ever change since the group is created
        const ui32 NumFailRealms = 0;
        const ui32 NumFailDomainsPerFailRealm = 0;
        const ui32 NumVDisksPerFailDomain = 0;

        // topology according to the geometry
        std::shared_ptr<TBlobStorageGroupInfo::TTopology> Topology;

        struct TGroupStatus {
            // status derived from the actual state of VDisks (IsReady() to be exact)
            NKikimrBlobStorage::TGroupStatus::E OperatingStatus = NKikimrBlobStorage::TGroupStatus::UNKNOWN;
            // status derived by adding underlying PDisk status (some of them are assumed to be not working ones)
            NKikimrBlobStorage::TGroupStatus::E ExpectedStatus = NKikimrBlobStorage::TGroupStatus::UNKNOWN;

            void MakeWorst(NKikimrBlobStorage::TGroupStatus::E operating, NKikimrBlobStorage::TGroupStatus::E expected) {
                OperatingStatus = Max(OperatingStatus, operating);
                ExpectedStatus = Max(ExpectedStatus, expected);
            }
        } Status;

        // group status depends on the IsReady value for every VDisk; so it has to be updated every time there is possible
        // change; source of this change include:
        // 1. Node disconnection
        // 2. VDisk status update
        // 3. VSlotReadyUpdate timer hit
        // 4. Group contents change
        //
        // also it depends on the Status of underlying PDisks, so every time their status change, group status has to
        // be recalculated too
        void CalculateGroupStatus();

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TGroupInfo,
                    Table::Generation,
                    Table::Owner,
                    Table::ErasureSpecies,
                    Table::DesiredPDiskCategory,
                    Table::DesiredVDiskCategory,
                    Table::EncryptionMode,
                    Table::LifeCyclePhase,
                    Table::MainKeyId,
                    Table::EncryptedGroupKey,
                    Table::GroupKeyNonce,
                    Table::MainKeyVersion,
                    Table::SeenOperational,
                    Table::DecommitStatus,
                    Table::VirtualGroupName,
                    Table::VirtualGroupState,
                    Table::HiveId,
                    Table::Database,
                    Table::BlobDepotConfig,
                    Table::BlobDepotId,
                    Table::ErrorReason,
                    Table::NeedAlter
                > adapter(
                    &TGroupInfo::Generation,
                    &TGroupInfo::Owner,
                    &TGroupInfo::ErasureSpecies,
                    &TGroupInfo::DesiredPDiskCategory,
                    &TGroupInfo::DesiredVDiskCategory,
                    &TGroupInfo::EncryptionMode,
                    &TGroupInfo::LifeCyclePhase,
                    &TGroupInfo::MainKeyId,
                    &TGroupInfo::EncryptedGroupKey,
                    &TGroupInfo::GroupKeyNonce,
                    &TGroupInfo::MainKeyVersion,
                    &TGroupInfo::SeenOperational,
                    &TGroupInfo::DecommitStatus,
                    &TGroupInfo::VirtualGroupName,
                    &TGroupInfo::VirtualGroupState,
                    &TGroupInfo::HiveId,
                    &TGroupInfo::Database,
                    &TGroupInfo::BlobDepotConfig,
                    &TGroupInfo::BlobDepotId,
                    &TGroupInfo::ErrorReason,
                    &TGroupInfo::NeedAlter
                );
            callback(&adapter);
        }

        TGroupInfo(TGroupId id,
                   Schema::Group::Generation::Type generation,
                   Schema::Group::Owner::Type owner,
                   Schema::Group::ErasureSpecies::Type erasureSpecies,
                   Schema::Group::DesiredPDiskCategory::Type desiredPDiskCategory,
                   Schema::Group::DesiredVDiskCategory::Type desiredVDiskCategory,
                   Schema::Group::EncryptionMode::Type encryptionMode,
                   Schema::Group::LifeCyclePhase::Type lifeCyclePhase,
                   Schema::Group::MainKeyId::Type mainKeyId,
                   Schema::Group::EncryptedGroupKey::Type encryptedGroupKey,
                   Schema::Group::GroupKeyNonce::Type groupKeyNonce,
                   Schema::Group::MainKeyVersion::Type mainKeyVersion,
                   Schema::Group::Down::Type down,
                   Schema::Group::SeenOperational::Type seenOperational,
                   TBoxStoragePoolId storagePoolId,
                   ui32 numFailRealms,
                   ui32 numFailDomainsPerFailRealm,
                   ui32 numVDisksPerFailDomain)
            : ID(id)
            , Generation(generation)
            , Owner(owner)
            , ErasureSpecies(erasureSpecies)
            , DesiredPDiskCategory(desiredPDiskCategory)
            , DesiredVDiskCategory(desiredVDiskCategory)
            , EncryptionMode(encryptionMode)
            , LifeCyclePhase(lifeCyclePhase)
            , MainKeyId(mainKeyId)
            , EncryptedGroupKey(encryptedGroupKey)
            , GroupKeyNonce(groupKeyNonce)
            , MainKeyVersion(mainKeyVersion)
            , PersistedDown(down)
            , SeenOperational(seenOperational)
            , Down(PersistedDown)
            , VDisksInGroup(numFailRealms * numFailDomainsPerFailRealm * numVDisksPerFailDomain)
            , StoragePoolId(storagePoolId)
            , NumFailRealms(numFailRealms)
            , NumFailDomainsPerFailRealm(numFailDomainsPerFailRealm)
            , NumVDisksPerFailDomain(numVDisksPerFailDomain)
            , Topology(std::make_shared<TBlobStorageGroupInfo::TTopology>(TBlobStorageGroupType(ErasureSpecies),
                NumFailRealms, NumFailDomainsPerFailRealm, NumVDisksPerFailDomain))
        {
            Topology->FinalizeConstruction();
            Y_ABORT_UNLESS(VDisksInGroup.size() == Topology->GetTotalVDisksNum());
        }

        bool Listable() const {
            return VDisksInGroup
                || !VirtualGroupState
                || *VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::WORKING
                || *VirtualGroupState == NKikimrBlobStorage::EVirtualGroupState::CREATE_FAILED;
        }

        void ClearVDisksInGroup() {
            std::fill(VDisksInGroup.begin(), VDisksInGroup.end(), nullptr);
        }

        void AddVSlot(const TVSlotInfo *vslot) {
            Y_ABORT_UNLESS(vslot->Group == this);
            const ui32 index = Topology->GetOrderNumber(vslot->GetShortVDiskId());
            Y_ABORT_UNLESS(!VDisksInGroup[index]);
            VDisksInGroup[index] = vslot;
        }

        void FinishVDisksInGroup() {
            for (const TVSlotInfo *slot : VDisksInGroup) {
                Y_ABORT_UNLESS(slot);
            }
        }

        TGroupSpecies GetGroupSpecies() const {
            return TGroupSpecies(ErasureSpecies, DesiredPDiskCategory, DesiredVDiskCategory);
        }

        TResourceRawValues GetResourceCurrentValues() const {
            TResourceRawValues values = {};
            for (const TVSlotInfo *vslot : VDisksInGroup) {
                values += vslot->GetResourceCurrentValues();
            }
            return values;
        }

        NPDisk::EDeviceType GetCommonDeviceType() const {
            if (VDisksInGroup) {
                const NPDisk::EDeviceType type = VDisksInGroup.front()->PDisk->Kind.Type();
                for (const TVSlotInfo *vslot : VDisksInGroup) {
                    if (type != vslot->PDisk->Kind.Type()) {
                        return NPDisk::DEVICE_TYPE_UNKNOWN;
                    }
                }
                return type;
            } else {
                return NPDisk::DEVICE_TYPE_UNKNOWN;
            }
        }

        void FillInGroupParameters(NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters *params) const {
            if (GroupMetrics) {
                params->MergeFrom(GroupMetrics->GetGroupParameters());
            } else {
                FillInResources(params->MutableAssuredResources(), true);
                FillInResources(params->MutableCurrentResources(), false);
                FillInVDiskResources(params);
            }
        }

        void FillInResources(NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters::TResources *pb, bool countMaxSlots) const {
            // count minimum params for each of slots assuming they are shared fairly between all the slots (expected or currently created)
            std::optional<ui64> size;
            std::optional<double> iops;
            std::optional<ui64> readThroughput;
            std::optional<ui64> writeThroughput;
            std::optional<double> occupancy;
            for (const TVSlotInfo *vslot : VDisksInGroup) {
                const TPDiskInfo *pdisk = vslot->PDisk;
                const auto& metrics = pdisk->Metrics;
                const ui32 shareFactor = countMaxSlots ? pdisk->ExpectedSlotCount : pdisk->NumActiveSlots;
                ui64 vdiskSlotSize = 0;
                if (metrics.HasEnforcedDynamicSlotSize()) {
                    vdiskSlotSize = metrics.GetEnforcedDynamicSlotSize();
                } else if (metrics.GetTotalSize()) {
                    vdiskSlotSize = metrics.GetTotalSize() / shareFactor;
                }
                if (vdiskSlotSize) {
                    size = Min(size.value_or(Max<ui64>()), vdiskSlotSize);
                }
                if (metrics.HasMaxIOPS()) {
                    iops = Min(iops.value_or(Max<double>()), metrics.GetMaxIOPS() * 100 / shareFactor * 0.01);
                }
                if (metrics.HasMaxReadThroughput()) {
                    readThroughput = Min(readThroughput.value_or(Max<ui64>()), metrics.GetMaxReadThroughput() / shareFactor);
                }
                if (metrics.HasMaxWriteThroughput()) {
                    writeThroughput = Min(writeThroughput.value_or(Max<ui64>()), metrics.GetMaxWriteThroughput() / shareFactor);
                }
                if (const auto& vm = vslot->Metrics; vm.HasOccupancy()) {
                    occupancy = Max(occupancy.value_or(0), vm.GetOccupancy());
                }
            }

            // and recalculate it to the total size of the group according to the erasure
            TBlobStorageGroupType type(ErasureSpecies);
            const double factor = (double)VDisksInGroup.size() * type.DataParts() / type.TotalPartCount();
            if (size) {
                pb->SetSpace(*size * factor);
            }
            if (iops) {
                pb->SetIOPS(*iops * VDisksInGroup.size() / type.TotalPartCount());
            }
            if (readThroughput) {
                pb->SetReadThroughput(*readThroughput * factor);
            }
            if (writeThroughput) {
                pb->SetWriteThroughput(*writeThroughput * factor);
            }
            if (occupancy) {
                pb->SetOccupancy(*occupancy);
            }
        }

        void FillInVDiskResources(NKikimrBlobStorage::TEvControllerSelectGroupsResult::TGroupParameters *pb) const {
            TBlobStorageGroupType type(ErasureSpecies);
            const double f = (double)VDisksInGroup.size() * type.DataParts() / type.TotalPartCount();
            for (const TVSlotInfo *vslot : VDisksInGroup) {
                const auto& m = vslot->Metrics;
                if (m.HasAvailableSize()) {
                    pb->SetAvailableSize(Min<ui64>(pb->HasAvailableSize() ? pb->GetAvailableSize() : Max<ui64>(), f * m.GetAvailableSize()));
                }
                if (m.HasAllocatedSize()) {
                    pb->SetAllocatedSize(Max<ui64>(pb->HasAllocatedSize() ? pb->GetAllocatedSize() : 0, f * m.GetAllocatedSize()));
                }
                if (m.HasSpaceColor()) {
                    pb->SetSpaceColor(pb->HasSpaceColor() ? Max(pb->GetSpaceColor(), m.GetSpaceColor()) : m.GetSpaceColor());
                }
            }
        }

        void UpdateSeenOperational() {
            TBlobStorageGroupInfo::TGroupFailDomains failed(Topology.get());
            for (const TVSlotInfo *slot : VDisksInGroup) {
                if (!slot->IsOperational()) {
                    failed |= {Topology.get(), {(ui8)slot->RingIdx, (ui8)slot->FailDomainIdx, (ui8)slot->VDiskIdx}};
                }
            }
            if (Topology->QuorumChecker->CheckFailModelForGroupDomains(failed)) {
                SeenOperational = true;
            }
        }

        TStorageStatusFlags GetStorageStatusFlags() const {
            TStorageStatusFlags res;
            for (const TVSlotInfo *slot : VDisksInGroup) {
                if (const auto& m = slot->Metrics; m.HasStatusFlags()) {
                    res.Merge(m.GetStatusFlags());
                }
            }
            return res;
        }

        bool IsPhysicalGroup() const {
            return !BlobDepotId && DecommitStatus == NKikimrBlobStorage::TGroupDecommitStatus::NONE;
        }

        bool IsDecommitted() const {
            return DecommitStatus != NKikimrBlobStorage::TGroupDecommitStatus::NONE;
        }

        void OnCommit();
    };

    using TGroups = TMap<TGroupId, THolder<TGroupInfo>>;

    class TNodeInfo {
    public:
        using Table = Schema::Node;

        TActorId ConnectedServerId; // the latest ServerId who sent RegisterNode
        TActorId InterconnectSessionId;
        Table::NextPDiskID::Type NextPDiskID;
        TInstant LastConnectTimestamp;
        TInstant LastDisconnectTimestamp;
        // in-mem only
        std::map<TString, NPDisk::TDriveData> KnownDrives;
        THashSet<TGroupId> WaitingForGroups;
        THashSet<TGroupId> GroupsRequested;
        bool DeclarativePDiskManagement = false;

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TNodeInfo,
                    Table::NextPDiskID,
                    Table::LastConnectTimestamp,
                    Table::LastDisconnectTimestamp
                > adapter(
                    &TNodeInfo::NextPDiskID,
                    &TNodeInfo::LastConnectTimestamp,
                    &TNodeInfo::LastDisconnectTimestamp
                );
            callback(&adapter);
        }

        TNodeInfo()
            : NextPDiskID(0)
        {}

        TNodeInfo(Table::NextPDiskID::Type nextPDiskID)
            : NextPDiskID(nextPDiskID)
        {}

    };

    TMap<ui32, TSet<ui32>> NodesAwaitingKeysForGroup;

    struct THostConfigInfo {
        struct TDriveKey {
            Schema::HostConfigDrive::HostConfigId::Type HostConfigId;
            Schema::HostConfigDrive::Path::Type Path;

            TDriveKey() = default;
            TDriveKey(const TDriveKey&) = default;
            TDriveKey(TDriveKey&&) = default;

            TDriveKey(Schema::HostConfigDrive::TKey::Type key) {
                std::tie(HostConfigId, Path) = std::move(key);
            }

            auto GetKey() const {
                return std::tie(HostConfigId, Path);
            }

            friend bool operator<(const TDriveKey &x, const TDriveKey &y) {
                return x.GetKey() < y.GetKey();
            }
        };

        struct TDriveInfo {
            using Table = Schema::HostConfigDrive;

            Table::TypeCol::Type Type;
            Table::SharedWithOs::Type SharedWithOs;
            Table::ReadCentric::Type ReadCentric;
            Table::Kind::Type Kind;
            TMaybe<Table::PDiskConfig::Type> PDiskConfig;

            template<typename T>
            static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
                static TTableAdapter<Table, TDriveInfo,
                        Table::TypeCol,
                        Table::SharedWithOs,
                        Table::ReadCentric,
                        Table::Kind,
                        Table::PDiskConfig
                    > adapter(
                        &TDriveInfo::Type,
                        &TDriveInfo::SharedWithOs,
                        &TDriveInfo::ReadCentric,
                        &TDriveInfo::Kind,
                        &TDriveInfo::PDiskConfig
                    );
                callback(&adapter);
            }
        };

        using TDrives = TMap<TDriveKey, TDriveInfo>;

        using Table = Schema::HostConfig;

        Table::Name::Type Name;
        TMaybe<Table::Generation::Type> Generation;
        TDrives Drives;

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, THostConfigInfo,
                    Table::Name,
                    Table::Generation,
                    TInlineTable<TDrives, Schema::HostConfigDrive>
                > adapter(
                    &THostConfigInfo::Name,
                    &THostConfigInfo::Generation,
                    &THostConfigInfo::Drives
                );
            callback(&adapter);
        }
    };

    struct TBoxInfo {
        struct THostKey {
            Schema::BoxHostV2::BoxId::Type BoxId;
            Schema::BoxHostV2::Fqdn::Type Fqdn;
            Schema::BoxHostV2::IcPort::Type IcPort;

            THostKey() = default;
            THostKey(const THostKey&) = default;
            THostKey(THostKey&&) = default;

            THostKey(Schema::BoxHostV2::TKey::Type key) {
                std::tie(BoxId, Fqdn, IcPort) = std::move(key);
            }

            auto GetKey() const {
                return std::tie(BoxId, Fqdn, IcPort);
            }

            friend bool operator<(const THostKey &x, const THostKey &y) {
                return x.GetKey() < y.GetKey();
            }

            operator THostId() const {
                return {Fqdn, IcPort};
            }
        };

        struct THostInfo {
            using Table = Schema::BoxHostV2;

            Schema::BoxHostV2::HostConfigId::Type HostConfigId;
            TMaybe<Schema::Node::ID::Type> EnforcedNodeId;

            template<typename T>
            static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
                static TTableAdapter<Table, THostInfo,
                        Table::HostConfigId,
                        Table::EnforcedNodeId
                    > adapter(
                        &THostInfo::HostConfigId,
                        &THostInfo::EnforcedNodeId
                    );
                callback(&adapter);
            }
        };

        using Table = Schema::Box;

        using TUserIds = TSet<Schema::BoxUser::TKey::Type>;
        using THosts = TMap<THostKey, THostInfo>;

        Table::Name::Type Name;
        TMaybe<Table::Generation::Type> Generation;
        TUserIds UserIds;
        THosts Hosts;

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TBoxInfo,
                    Table::Name,
                    Table::Generation,
                    TInlineTable<TUserIds, Schema::BoxUser>,
                    TInlineTable<THosts, Schema::BoxHostV2>
                > adapter(
                    &TBoxInfo::Name,
                    &TBoxInfo::Generation,
                    &TBoxInfo::UserIds,
                    &TBoxInfo::Hosts
                );
            callback(&adapter);
        }
    };

    struct TStoragePoolInfo {
        using Table = Schema::BoxStoragePool;

        struct TPDiskFilter {
            Schema::BoxStoragePoolPDiskFilter::BoxId::Type BoxId{};
            Schema::BoxStoragePoolPDiskFilter::StoragePoolId::Type StoragePoolId{};
            TMaybe<Schema::BoxStoragePoolPDiskFilter::TypeCol::Type> Type;
            TMaybe<Schema::BoxStoragePoolPDiskFilter::SharedWithOs::Type> SharedWithOs;
            TMaybe<Schema::BoxStoragePoolPDiskFilter::ReadCentric::Type> ReadCentric;
            TMaybe<Schema::BoxStoragePoolPDiskFilter::Kind::Type> Kind;

            template<typename TRowset>
            static TPDiskFilter CreateFromRowset(const TRowset &rowset) {
                using T = Schema::BoxStoragePoolPDiskFilter;
                TPDiskFilter result;
                result.BoxId = rowset.template GetValue<T::BoxId>();
                result.StoragePoolId = rowset.template GetValue<T::StoragePoolId>();
                if (rowset.template HaveValue<T::TypeCol>()) {
                    result.Type = rowset.template GetValue<T::TypeCol>();
                }
                if (rowset.template HaveValue<T::SharedWithOs>()) {
                    result.SharedWithOs = rowset.template GetValue<T::SharedWithOs>();
                }
                if (rowset.template HaveValue<T::ReadCentric>()) {
                    result.ReadCentric = rowset.template GetValue<T::ReadCentric>();
                }
                if (rowset.template HaveValue<T::Kind>()) {
                    result.Kind = rowset.template GetValue<T::Kind>();
                }
                return result;
            }

            bool MatchPDisk(ui64 category, TMaybe<bool> sharedWithOs, TMaybe<bool> readCentric) const {
                TPDiskCategory c(category);

                // determine disk type in terms of configuration
                Schema::BoxStoragePoolPDiskFilter::TypeCol::Type type = PDiskTypeToPDiskType(c.Type());

                // obtain disk kind
                Schema::BoxStoragePoolPDiskFilter::Kind::Type kind = c.Kind();

                return (!Type || *Type == type)
                    && (!SharedWithOs || *SharedWithOs == sharedWithOs)
                    && (!ReadCentric || *ReadCentric == readCentric)
                    && (!Kind || *Kind == kind);
            }

            bool MatchPDisk(const TPDiskInfo& pdisk) const {
                return MatchPDisk(pdisk.Kind, pdisk.SharedWithOs, pdisk.ReadCentric);
            }

            void Output(IOutputStream& s) const {
                bool first = true;
                auto comma = [&] { return std::exchange(first, false) ? "" : ","; };
                if (const auto& x = Type) {
                    s << comma() << "Type:" << NKikimrBlobStorage::EPDiskType_Name(*x);
                }
                if (const auto& x = SharedWithOs) {
                    s << comma() << "SharedWithOs:" << (int)*x;
                }
                if (const auto& x = ReadCentric) {
                    s << comma() << "ReadCentric:" << (int)*x;
                }
                if (const auto& x = Kind) {
                    s << comma() << "Kind:" << *x;
                }
            }

            static TString ToString(const TSet<TPDiskFilter>& filters) {
                TStringStream s;
                for (auto it = filters.begin(); it != filters.end(); ++it) {
                    if (it != filters.begin()) {
                        s << "|";
                    }
                    it->Output(s);
                }
                return s.Str();
            }

            auto GetKey() const {
                return std::tie(BoxId, StoragePoolId, Type, SharedWithOs, ReadCentric, Kind);
            }

            friend bool operator <(const TPDiskFilter &x, const TPDiskFilter &y) {
                return x.GetKey() < y.GetKey();
            }

            Y_SAVELOAD_DEFINE(Type, SharedWithOs, ReadCentric, Kind);
        };

        Table::Name::Type Name;
        Table::ErasureSpecies::Type ErasureSpecies;
        TMaybe<Table::RealmLevelBegin::Type> RealmLevelBegin;
        TMaybe<Table::RealmLevelEnd::Type> RealmLevelEnd;
        TMaybe<Table::DomainLevelBegin::Type> DomainLevelBegin;
        TMaybe<Table::DomainLevelEnd::Type> DomainLevelEnd;
        TMaybe<Table::NumFailRealms::Type> NumFailRealms;
        TMaybe<Table::NumFailDomainsPerFailRealm::Type> NumFailDomainsPerFailRealm;
        TMaybe<Table::NumVDisksPerFailDomain::Type> NumVDisksPerFailDomain;
        Table::VDiskKind::Type VDiskKind;
        TMaybe<Table::SpaceBytes::Type> SpaceBytes;
        TMaybe<Table::WriteIOPS::Type> WriteIOPS;
        TMaybe<Table::WriteBytesPerSecond::Type> WriteBytesPerSecond;
        TMaybe<Table::ReadIOPS::Type> ReadIOPS;
        TMaybe<Table::ReadBytesPerSecond::Type> ReadBytesPerSecond;
        TMaybe<Table::InMemCacheBytes::Type> InMemCacheBytes;
        Table::Kind::Type Kind;
        Table::NumGroups::Type NumGroups;
        TMaybe<Table::Generation::Type> Generation;
        TMaybe<Table::EncryptionMode::Type> EncryptionMode; // null on old versions
        TMaybe<ui64> SchemeshardId;
        TMaybe<ui64> PathItemId;
        bool RandomizeGroupMapping;

        bool IsSameGeometry(const TStoragePoolInfo& other) const {
            return ErasureSpecies == other.ErasureSpecies
                && RealmLevelBegin == other.RealmLevelBegin
                && RealmLevelEnd == other.RealmLevelEnd
                && DomainLevelBegin == other.DomainLevelBegin
                && DomainLevelEnd == other.DomainLevelEnd
                && NumFailRealms == other.NumFailRealms
                && NumFailDomainsPerFailRealm == other.NumFailDomainsPerFailRealm
                && NumVDisksPerFailDomain == other.NumVDisksPerFailDomain;
        }

        bool HasGroupGeometry() const {
            return RealmLevelBegin || RealmLevelEnd || DomainLevelBegin || DomainLevelEnd || NumFailRealms ||
                NumFailDomainsPerFailRealm || NumVDisksPerFailDomain;
        }

        NKikimrBlobStorage::TGroupGeometry GetGroupGeometry() const {
            NKikimrBlobStorage::TGroupGeometry res;
            if (RealmLevelBegin) {
                res.SetRealmLevelBegin(*RealmLevelBegin);
            }
            if (RealmLevelEnd) {
                res.SetRealmLevelEnd(*RealmLevelEnd);
            }
            if (DomainLevelBegin) {
                res.SetDomainLevelBegin(*DomainLevelBegin);
            }
            if (DomainLevelEnd) {
                res.SetDomainLevelEnd(*DomainLevelEnd);
            }
            if (NumFailRealms) {
                res.SetNumFailRealms(*NumFailRealms);
            }
            if (NumFailDomainsPerFailRealm) {
                res.SetNumFailDomainsPerFailRealm(*NumFailDomainsPerFailRealm);
            }
            if (NumVDisksPerFailDomain) {
                res.SetNumVDisksPerFailDomain(*NumVDisksPerFailDomain);
            }
            return res;
        }

        bool HasUsagePattern() const {
            return SpaceBytes || WriteIOPS || WriteBytesPerSecond || ReadIOPS || ReadBytesPerSecond || InMemCacheBytes;
        }

        NKikimrBlobStorage::TGroupUsagePattern GetUsagePattern() const {
            NKikimrBlobStorage::TGroupUsagePattern res;
            if (SpaceBytes) {
                res.SetSpaceBytes(*SpaceBytes);
            }
            if (WriteIOPS) {
                res.SetWriteIOPS(*WriteIOPS);
            }
            if (WriteBytesPerSecond) {
                res.SetWriteBytesPerSecond(*WriteBytesPerSecond);
            }
            if (ReadIOPS) {
                res.SetReadIOPS(*ReadIOPS);
            }
            if (ReadBytesPerSecond) {
                res.SetReadBytesPerSecond(*ReadBytesPerSecond);
            }
            if (InMemCacheBytes) {
                res.SetInMemCacheBytes(*InMemCacheBytes);
            }
            return res;
        }

        using TUserIds = TSet<Schema::BoxStoragePoolUser::TKey::Type>;
        using TPDiskFilters = TSet<TPDiskFilter>;

        TUserIds UserIds;
        TPDiskFilters PDiskFilters;

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TStoragePoolInfo,
                    Table::Name,
                    Table::ErasureSpecies,
                    Table::RealmLevelBegin,
                    Table::RealmLevelEnd,
                    Table::DomainLevelBegin,
                    Table::DomainLevelEnd,
                    Table::NumFailRealms,
                    Table::NumFailDomainsPerFailRealm,
                    Table::NumVDisksPerFailDomain,
                    Table::VDiskKind,
                    Table::SpaceBytes,
                    Table::WriteIOPS,
                    Table::WriteBytesPerSecond,
                    Table::ReadIOPS,
                    Table::ReadBytesPerSecond,
                    Table::InMemCacheBytes,
                    Table::Kind,
                    Table::NumGroups,
                    Table::Generation,
                    Table::EncryptionMode,
                    Table::SchemeshardId,
                    Table::PathItemId,
                    Table::RandomizeGroupMapping,
                    TInlineTable<TUserIds, Schema::BoxStoragePoolUser>,
                    TInlineTable<TPDiskFilters, Schema::BoxStoragePoolPDiskFilter>
                > adapter(
                    &TStoragePoolInfo::Name,
                    &TStoragePoolInfo::ErasureSpecies,
                    &TStoragePoolInfo::RealmLevelBegin,
                    &TStoragePoolInfo::RealmLevelEnd,
                    &TStoragePoolInfo::DomainLevelBegin,
                    &TStoragePoolInfo::DomainLevelEnd,
                    &TStoragePoolInfo::NumFailRealms,
                    &TStoragePoolInfo::NumFailDomainsPerFailRealm,
                    &TStoragePoolInfo::NumVDisksPerFailDomain,
                    &TStoragePoolInfo::VDiskKind,
                    &TStoragePoolInfo::SpaceBytes,
                    &TStoragePoolInfo::WriteIOPS,
                    &TStoragePoolInfo::WriteBytesPerSecond,
                    &TStoragePoolInfo::ReadIOPS,
                    &TStoragePoolInfo::ReadBytesPerSecond,
                    &TStoragePoolInfo::InMemCacheBytes,
                    &TStoragePoolInfo::Kind,
                    &TStoragePoolInfo::NumGroups,
                    &TStoragePoolInfo::Generation,
                    &TStoragePoolInfo::EncryptionMode,
                    &TStoragePoolInfo::SchemeshardId,
                    &TStoragePoolInfo::PathItemId,
                    &TStoragePoolInfo::RandomizeGroupMapping,
                    &TStoragePoolInfo::UserIds,
                    &TStoragePoolInfo::PDiskFilters
                );
            callback(&adapter);
        }
    };

    struct TSerial {
        TString Serial;

        TSerial(const TString& serial)
            : Serial(serial)
        {}

        auto GetKey() const {
            return std::tie(Serial);
        }

        operator TString() const {
            return Serial;
        }

        friend bool operator<(const TSerial &x, const TSerial &y) {
            return x.GetKey() < y.GetKey();
        }

        friend bool operator==(const TSerial &x, const TSerial &y) {
            return x.GetKey() == y.GetKey();
        }

        friend bool operator!=(const TSerial &x, const TSerial &y) {
            return !(x == y);
        }
    };

    struct TDriveSerialInfo {
        using Table = Schema::DriveSerial;

        Table::BoxId::Type BoxId;
        TMaybe<Table::NodeId::Type> NodeId;
        TMaybe<Table::PDiskId::Type> PDiskId;
        TMaybe<Table::Guid::Type> Guid;
        Table::LifeStage::Type LifeStage = NKikimrBlobStorage::TDriveLifeStage::FREE;
        Table::Kind::Type Kind = 0;
        Table::PDiskType::Type PDiskType = PDiskTypeToPDiskType(NPDisk::DEVICE_TYPE_UNKNOWN);
        TMaybe<Table::PDiskConfig::Type> PDiskConfig;
        TMaybe<Table::Path::Type> Path;

        TDriveSerialInfo() = default;
        TDriveSerialInfo(const TDriveSerialInfo&) = default;

        TDriveSerialInfo(Table::BoxId::Type boxId)
          : BoxId(boxId)
        {}

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TDriveSerialInfo,
                    Table::BoxId,
                    Table::NodeId,
                    Table::PDiskId,
                    Table::Guid,
                    Table::LifeStage,
                    Table::Kind,
                    Table::PDiskType,
                    Table::PDiskConfig,
                    Table::Path
                > adapter(
                    &TDriveSerialInfo::BoxId,
                    &TDriveSerialInfo::NodeId,
                    &TDriveSerialInfo::PDiskId,
                    &TDriveSerialInfo::Guid,
                    &TDriveSerialInfo::LifeStage,
                    &TDriveSerialInfo::Kind,
                    &TDriveSerialInfo::PDiskType,
                    &TDriveSerialInfo::PDiskConfig,
                    &TDriveSerialInfo::Path
                );
            callback(&adapter);
        }

        void OnCommit() {}
        void OnClone(const THolder<TDriveSerialInfo>&) {}
    };

    struct TBlobDepotDeleteQueueInfo {
        using Table = Schema::BlobDepotDeleteQueue;

        TMaybe<Table::HiveId::Type> HiveId;
        TMaybe<Table::BlobDepotId::Type> BlobDepotId;
        TActorId VirtualGroupSetupMachineId;

        TBlobDepotDeleteQueueInfo() = default;

        TBlobDepotDeleteQueueInfo(TMaybe<Table::HiveId::Type> hiveId, TMaybe<Table::BlobDepotId::Type> blobDepotId)
            : HiveId(std::move(hiveId))
            , BlobDepotId(std::move(blobDepotId))
        {}

        template<typename T>
        static void Apply(TBlobStorageController* /*controller*/, T&& callback) {
            static TTableAdapter<Table, TBlobDepotDeleteQueueInfo,
                    Table::HiveId,
                    Table::BlobDepotId
                > adapter(
                    &TBlobDepotDeleteQueueInfo::HiveId,
                    &TBlobDepotDeleteQueueInfo::BlobDepotId
                );
            callback(&adapter);
        }
    };

    struct THostRecord {
        TNodeId NodeId;
        TNodeLocation Location;

        THostRecord(TEvInterconnect::TNodeInfo&& nodeInfo)
            : NodeId(nodeInfo.NodeId)
            , Location(std::move(nodeInfo.Location))
        {}
    };

    class THostRecordMapImpl {
        THashMap<THostId, THostRecord> HostIdToRecord;
        THashMap<TNodeId, THostId> NodeIdToHostId;

    public:
        THostRecordMapImpl(TEvInterconnect::TEvNodesInfo *msg) {
            for (TEvInterconnect::TNodeInfo& nodeInfo : msg->Nodes) {
                const THostId hostId(nodeInfo.Host, nodeInfo.Port);
                NodeIdToHostId.emplace(nodeInfo.NodeId, hostId);
                HostIdToRecord.emplace(hostId, std::move(nodeInfo));
            }
        }

        const TNodeLocation& GetLocation(TNodeId nodeId) const {
            if (auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
                if (auto hostIt = HostIdToRecord.find(it->second); hostIt != HostIdToRecord.end()) {
                    return hostIt->second.Location;
                }
            }
            Y_ABORT();
        }

        TMaybe<TNodeId> ResolveNodeId(const THostId& hostId) const {
            if (const auto it = HostIdToRecord.find(hostId); it != HostIdToRecord.end()) {
                return it->second.NodeId;
            } else {
                return {};
            }
        }

        TMaybe<TNodeId> ResolveNodeId(const TBoxInfo::THostKey& key, const TBoxInfo::THostInfo& info) const {
            return info.EnforcedNodeId ? info.EnforcedNodeId : ResolveNodeId(key);
        }

        TMaybe<THostId> GetHostId(TNodeId nodeId) const {
            if (const auto it = NodeIdToHostId.find(nodeId); it != NodeIdToHostId.end()) {
                return it->second;
            } else {
                return {};
            }
        }

        auto begin() const {
            return HostIdToRecord.begin();
        }

        auto end() const {
            return HostIdToRecord.end();
        }
    };

    using THostRecordMap = std::shared_ptr<THostRecordMapImpl>;

private:
    TString InstanceId;
    std::shared_ptr<std::atomic_uint64_t> SelfHealUnreassignableGroups = std::make_shared<std::atomic_uint64_t>();
    TMaybe<TActorId> MigrationId;
    TVSlots VSlots; // ordering is important
    TPDisks PDisks; // ordering is important
    TMap<TSerial, THolder<TDriveSerialInfo>> DrivesSerials;
    TGroups GroupMap;
    THashMap<TGroupId, TGroupInfo*> GroupLookup;
    TMap<TGroupSpecies, TVector<TGroupId>> IndexGroupSpeciesToGroup;
    TMap<TNodeId, TNodeInfo> Nodes;
    Schema::Group::ID::Type NextGroupID = Schema::Group::ID::Type::Zero();
    Schema::Group::ID::Type NextVirtualGroupId = Schema::Group::ID::Type::Zero();
    Schema::State::NextStoragePoolId::Type NextStoragePoolId = 0;
    ui32 DefaultMaxSlots = 0;
    ui32 PDiskSpaceMarginPromille = 0;
    ui32 GroupReserveMin = 0;
    ui32 GroupReservePart = 0;
    ui32 MaxScrubbedDisksAtOnce = 0;
    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    TActorId ResponsivenessActorID;
    TTabletResponsivenessPinger* ResponsivenessPinger;
    TMap<THostConfigId, THostConfigInfo> HostConfigs;
    TMap<TBoxId, TBoxInfo> Boxes;
    TMap<TBoxStoragePoolId, TStoragePoolInfo> StoragePools;
    TMultiMap<TBoxStoragePoolId, TGroupId> StoragePoolGroups;
    TMap<TGroupId, TBlobDepotDeleteQueueInfo> BlobDepotDeleteQueue;
    ui64 NextOperationLogIndex = 1;
    TActorId StatProcessorActorId;
    TInstant LastMetricsCommit;
    bool SelfHealEnable = false;
    bool UseSelfHealLocalPolicy = false;
    bool TryToRelocateBrokenDisksLocallyFirst = false;
    bool DonorMode = false;
    TDuration ScrubPeriodicity;
    NKikimrBlobStorage::TStorageConfig StorageConfig;
    THashMap<TPDiskId, std::reference_wrapper<const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk>> StaticPDiskMap;
    THashMap<TPDiskId, ui32> StaticPDiskSlotUsage;
    std::unique_ptr<TStoragePoolStat> StoragePoolStat;
    bool StopGivingGroups = false;
    bool GroupLayoutSanitizerEnabled = false;
    bool AllowMultipleRealmsOccupation = true;
    bool StorageConfigObtained = false;
    bool Loaded = false;

    std::set<std::tuple<TGroupId, TNodeId>> GroupToNode;

    NKikimrBlobStorage::TSerialManagementStage::E SerialManagementStage
            = NKikimrBlobStorage::TSerialManagementStage::DISCOVER_SERIAL;

    NKikimrBlobStorage::TPDiskSpaceColor::E PDiskSpaceColorBorder
            = NKikimrBlobStorage::TPDiskSpaceColor::GREEN;

    TActorId SystemViewsCollectorId;

    // a set of VSlots with LastSeenReady != TInstant::Zero()
    std::unordered_set<TVSlotId, THash<TVSlotId>> NotReadyVSlotIds;

    friend class TConfigState;

    class TNodeWardenUpdateNotifier;

    struct TEvPrivate {
        enum EEv {
            EvUpdateSystemViews = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvUpdateSelfHealCounters,
            EvDropDonor,
            EvScrub,
            EvVSlotReadyUpdate,
            EvVSlotNotReadyHistogramUpdate,
            EvProcessIncomingEvent,
            EvUpdateHostRecords,
        };

        struct TEvUpdateSystemViews : public TEventLocal<TEvUpdateSystemViews, EvUpdateSystemViews> {};
        struct TEvUpdateSelfHealCounters : TEventLocal<TEvUpdateSelfHealCounters, EvUpdateSelfHealCounters> {};

        struct TEvDropDonor : TEventLocal<TEvDropDonor, EvDropDonor> {
            std::vector<TVSlotId> VSlotIds;
        };

        struct TEvScrub : TEventLocal<TEvScrub, EvScrub> {};
        struct TEvVSlotReadyUpdate : TEventLocal<TEvVSlotReadyUpdate, EvVSlotReadyUpdate> {};
        struct TEvVSlotNotReadyHistogramUpdate : TEventLocal<TEvVSlotNotReadyHistogramUpdate, EvVSlotNotReadyHistogramUpdate> {};

        struct TEvUpdateHostRecords : TEventLocal<TEvUpdateHostRecords, EvUpdateHostRecords> {
            THostRecordMap HostRecords;

            TEvUpdateHostRecords(THostRecordMap hostRecords)
                : HostRecords(std::move(hostRecords))
            {}
        };
    };

    static constexpr TDuration UpdateSystemViewsPeriod = TDuration::Seconds(5);

    std::unordered_set<TPDiskId, THash<TPDiskId>> SysViewChangedPDisks;
    std::unordered_set<TVSlotId, THash<TVSlotId>> SysViewChangedVSlots;
    std::unordered_set<TGroupId, THash<TGroupId>> SysViewChangedGroups;
    std::unordered_set<TBoxStoragePoolId, THash<TBoxStoragePoolId>> SysViewChangedStoragePools;
    bool SysViewChangedSettings = false;

    IActor* CreateSystemViewsCollector();
    void UpdateSystemViews();

    bool CommitConfigUpdates(TConfigState& state, bool suppressFailModelChecking, bool suppressDegradedGroupsChecking,
        bool suppressDisintegratedGroupsChecking, TTransactionContext& txc, TString *errorDescription,
        NKikimrBlobStorage::TConfigResponse *response = nullptr);

    void CommitSelfHealUpdates(TConfigState& state);
    void CommitScrubUpdates(TConfigState& state, TTransactionContext& txc);
    void CommitStoragePoolStatUpdates(TConfigState& state);
    void CommitSysViewUpdates(TConfigState& state);

    void InitializeSelfHealState();
    void FillInSelfHealGroups(TEvControllerUpdateSelfHealInfo& msg, TConfigState *state);
    void PushStaticGroupsToSelfHeal();
    void ProcessVDiskStatus(const google::protobuf::RepeatedPtrField<NKikimrBlobStorage::TVDiskStatus>& s);

    void UpdateSelfHealCounters();

    void ValidateInternalState();

    const TVSlotInfo* FindAcceptor(const TVSlotInfo& donor) {
        Y_ABORT_UNLESS(donor.Mood == TMood::Donor);
        TGroupInfo *group = FindGroup(donor.GroupId);
        Y_ABORT_UNLESS(group);
        const ui32 orderNumber = group->Topology->GetOrderNumber(donor.GetShortVDiskId());
        const TVSlotInfo *acceptor = group->VDisksInGroup[orderNumber];
        Y_ABORT_UNLESS(donor.GroupId == acceptor->GroupId && donor.GroupGeneration < acceptor->GroupGeneration &&
            donor.GetShortVDiskId() == acceptor->GetShortVDiskId());
        return acceptor;
    }

    TVSlotInfo* FindVSlot(TVSlotId id) {
        auto it = VSlots.find(id);
        return it != VSlots.end() ? it->second.Get() : nullptr;
    }

    TVSlotInfo* FindVSlot(TVDiskID id) { // GroupGeneration may be zero
        if (TGroupInfo *group = FindGroup(id.GroupID); group && !group->VDisksInGroup.empty()) {
            Y_ABORT_UNLESS(group->Topology->IsValidId(id));
            const ui32 index = group->Topology->GetOrderNumber(id);
            const TVSlotInfo *slot = group->VDisksInGroup[index];
            Y_ABORT_UNLESS(slot->GetShortVDiskId() == TVDiskIdShort(id)); // sanity check
            if (id.GroupGeneration == 0 || id.GroupGeneration == slot->GroupGeneration) {
                return const_cast<TVSlotInfo*>(slot);
            }
        }
        return nullptr;
    }

    bool HasScrubVSlot(TVSlotId id) {
        if (TVSlotInfo *slot = FindVSlot(id); slot && !slot->IsBeingDeleted()) {
            return true;
        } else {
            return StaticVSlots.count(id);
        }
    }

    template <typename... Types>
    TVSlotInfo& AddVSlot(TVSlotId id, Types... values) {
        SysViewChangedVSlots.insert(id);
        return *VSlots.emplace(id, MakeHolder<TVSlotInfo>(id, std::forward<Types>(values)...)).first->second;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // GROUP ACCESS

    TGroupInfo* FindGroup(TGroupId id) {
        if (const auto it = GroupLookup.find(id); it != GroupLookup.end()) {
            Y_DEBUG_ABORT_UNLESS(GroupMap[id].Get() == it->second && it->second);
            return it->second;
        } else {
            Y_DEBUG_ABORT_UNLESS(!GroupMap.count(id));
            return nullptr;
        }
    }

    template <typename... Types>
    TGroupInfo& AddGroup(TGroupId id, Types&&... values) {
        SysViewChangedGroups.insert(id);
        auto&& [it, inserted] = GroupMap.emplace(id, MakeHolder<TGroupInfo>(id, std::forward<Types>(values)...));
        Y_ABORT_UNLESS(inserted);
        GroupLookup.emplace(id, it->second.Get());
        return *it->second;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // NODE ACCESS

    TNodeInfo* FindNode(TNodeId id) {
        auto it = Nodes.find(id);
        if (it == Nodes.end())
            return nullptr;
        return &it->second;
    }

    TNodeInfo& GetNode(TNodeId id) {
        auto it = Nodes.find(id);
        if (it == Nodes.end()) {
            it = Nodes.emplace(id, TNodeInfo()).first;
        }
        return it->second;
    }

    bool AddNode(TNodeId id, const TNodeInfo& info) {
        return Nodes.emplace(id, info).second;
    }

    TPDiskInfo* FindPDisk(TPDiskId id) {
        auto it = PDisks.find(id);
        return it != PDisks.end() ? it->second.Get() : nullptr;
    }

    template<typename... Types>
    TPDiskInfo& AddPDisk(TPDiskId id, Types&&... values) {
        SysViewChangedPDisks.insert(id);
        auto&& [it, inserted] = PDisks.emplace(id, MakeHolder<TPDiskInfo>(std::forward<Types>(values)...));
        Y_ABORT_UNLESS(inserted);
        return *it->second;
    }

    //TGroupStatusTracker GroupStatusTracker;
    TDeque<TAutoPtr<IEventHandle>> InitQueue;
    THashMap<Schema::Group::Owner::Type, Schema::Group::ID::Type> OwnerIdIdxToGroup;

    void ReadGroups(TSet<ui32>& groupIDsToRead, bool discard, TEvBlobStorage::TEvControllerNodeServiceSetUpdate *result,
            TNodeId nodeId);

    void ReadPDisk(const TPDiskId& pdiskId, const TPDiskInfo& pdisk,
            TEvBlobStorage::TEvControllerNodeServiceSetUpdate *result,
            const NKikimrBlobStorage::EEntityStatus entityStatus);

    void ReadVSlot(const TVSlotInfo& vslot, TEvBlobStorage::TEvControllerNodeServiceSetUpdate *result);

    void DefaultSignalTabletActive(const TActorContext&) override
    {} // do nothing, we will signal tablet active explicitly after initialization

    void PassAway() override {
        if (ResponsivenessPinger) {
            ResponsivenessPinger->Detach(TActivationContext::ActorContextFor(ResponsivenessActorID));
            ResponsivenessPinger = nullptr;
        }
        for (TActorId *ptr : {&SelfHealId, &StatProcessorActorId, &SystemViewsCollectorId}) {
            if (const TActorId actorId = std::exchange(*ptr, {})) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            }
        }
        for (const auto& [id, info] : GroupMap) {
            if (const auto& actorId = info->VirtualGroupSetupMachineId) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            }
        }
        for (const auto& [groupId, info] : BlobDepotDeleteQueue) {
            if (const auto& actorId = info.VirtualGroupSetupMachineId) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            }
        }
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, GetNameserviceActorId(), SelfId(),
            nullptr, 0));
        TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0, MakeBlobStorageNodeWardenID(SelfId().NodeId()),
            SelfId(), nullptr, 0));
        return TActor::PassAway();
    }

    void OnDetach(const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC02, "OnDetach");
        PassAway();
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext&) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC03, "OnTabletDead", (Event, ev->Get()->ToString()));
        PassAway();
    }

    void Execute(TAutoPtr<ITransaction> tx) {
        TTabletExecutedFlat::Execute(tx, TActivationContext::AsActorContext());
    }

    void Execute(std::unique_ptr<ITransaction> tx) {
        TTabletExecutedFlat::Execute(tx.release(), TActivationContext::AsActorContext());
    }

    void OnActivateExecutor(const TActorContext&) override;

    void Handle(TEvNodeWardenStorageConfig::TPtr ev);
    void Handle(TEvents::TEvUndelivered::TPtr ev);
    void ApplyStorageConfig();
    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr ev);

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) override;
    void ProcessPostQuery(const NActorsProto::TRemoteHttpInfo& query, TActorId sender);

    void RenderResourceValues(IOutputStream& out, const TResourceRawValues& current);
    void RenderHeader(IOutputStream& out);
    void RenderFooter(IOutputStream& out);
    void RenderMonPage(IOutputStream& out);
    void RenderInternalTables(IOutputStream& out, const TString& table);
    void RenderVirtualGroups(IOutputStream& out);
    void RenderGroupDetail(IOutputStream &out, TGroupId groupId);
    void RenderGroupsInStoragePool(IOutputStream &out, const TBoxStoragePoolId& id);
    void RenderVSlotTable(IOutputStream& out, std::function<void()> callback);
    void RenderVSlotRow(IOutputStream& out, const TVSlotInfo& slot);
    void RenderGroupTable(IOutputStream& out, std::function<void()> callback);
    void RenderGroupRow(IOutputStream& out, const TGroupInfo& group);

    void Enqueue(STFUNC_SIG) override {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC04, "Enqueue", (TabletID, TabletID()), (Type, ev->GetTypeRewrite()),
            (Event, ev->ToString()));
        InitQueue.push_back(ev);
    }

    THostRecordMap HostRecords;
    void Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev);

public:
    // Self-heal actor's main purpose is to monitor FAULTY pdisks and to slightly move groups out of them; every move
    // should not render group unusable, also it should not exceed its fail model. It also takes into account replication
    // broker features such as only one vslot over PDisk is being replicated at a moment.
    //
    // It interacts with BS_CONTROLLER and group observer (which provides information about group state on a per-vdisk
    // basis). BS_CONTROLLER reports faulty PDisks and all involved groups in a push notification manner.
    IActor *CreateSelfHealActor();
    TActorId SelfHealId;

    bool IsGroupLayoutSanitizerEnabled() const {
        return GroupLayoutSanitizerEnabled;
    }

    // For test purposes, required for self heal actor
    void CreateEmptyHostRecordsMap() {
        TEvInterconnect::TEvNodesInfo nodes;
        HostRecords = std::make_shared<THostRecordMapImpl>(&nodes);
    }

    ui64 NextConfigTxSeqNo = 1;

private:
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Online state
    void Handle(TEvBlobStorage::TEvControllerRegisterNode::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerGetGroup::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerSelectGroups::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerUpdateDiskStatus::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerUpdateGroupStat::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerGroupMetricsExchange::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerUpdateNodeDrives::TPtr &ev);
    void Handle(TEvControllerCommitGroupLatencies::TPtr &ev);
    void Handle(TEvBlobStorage::TEvRequestControllerInfo::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerNodeReport::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerConfigRequest::TPtr &ev);
    void Handle(TEvBlobStorage::TEvControllerProposeGroupKey::TPtr &ev);
    void ForwardToSystemViewsCollector(STATEFN_SIG);
    void Handle(TEvPrivate::TEvUpdateSystemViews::TPtr &ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Scrub handling

    class TScrubState {
        friend class TBlobStorageController;
        friend class TTxScrubCommit;
        class TImpl;
        std::unique_ptr<TImpl> Impl;

    public:
        TScrubState(TBlobStorageController *self);
        ~TScrubState();
        void HandleTimer();
        void Clear();
        void AddItem(TVSlotId vslotId, std::optional<TString> state, TInstant scrubCycleStartTime,
            TInstant scrubCycleFinishTime, std::optional<bool> success);
        void OnDeletePDisk(TPDiskId pdiskId);
        void OnDeleteVSlot(TVSlotId vslotId, TTransactionContext& txc);
        void OnDeleteGroup(TGroupId groupId);
        void Render(IOutputStream& str);
        void OnNodeDisconnected(TNodeId nodeId);
        void OnScrubPeriodicityChange();
        void OnMaxScrubbedDisksAtOnceChange();
        void UpdateVDiskState(const TVSlotInfo *vslot);
    };

    TScrubState ScrubState;

    void Handle(TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TPtr ev);
    void Handle(TEvBlobStorage::TEvControllerScrubQuantumFinished::TPtr ev);
    void Handle(TEvBlobStorage::TEvControllerScrubReportQuantumInProgress::TPtr ev);

    void IssueInitialGroupContent();

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Metric collection

    struct TSelectGroupsQueueItem {
        TActorId RespondTo;
        ui64 Cookie;
        THolder<TEvBlobStorage::TEvControllerSelectGroupsResult> Event;
        TSet<TPDiskId> BlockedPDisks;

        TSelectGroupsQueueItem(TActorId respondTo, ui64 cookie, THolder<TEvBlobStorage::TEvControllerSelectGroupsResult> event)
            : RespondTo(respondTo)
            , Cookie(cookie)
            , Event(std::move(event))
        {}
    };

    struct TPDiskToQueueComp {
        using TIterator = TList<TSelectGroupsQueueItem>::iterator;
        using T = std::pair<TPDiskId, TIterator>;

        static void *IteratorToPtr(const TIterator& x) {
            return x == TIterator() ? nullptr : &*x;
        }

        bool operator ()(const TIterator& x, const TIterator& y) const {
            return IteratorToPtr(x) < IteratorToPtr(y);
        }

        bool operator ()(const T& x, const T& y) const {
            return x.first < y.first || (x.first == y.first && (*this)(x.second, y.second));
        }
    };

    TList<TSelectGroupsQueueItem> SelectGroupsQueue;
    TSet<std::pair<TPDiskId, TList<TSelectGroupsQueueItem>::iterator>, TPDiskToQueueComp> PDiskToQueue;

    void ProcessSelectGroupsQueueItem(TList<TSelectGroupsQueueItem>::iterator it);

    void NotifyNodesAwaitingKeysForGroups(ui32 groupId);
    ITransaction* CreateTxInitScheme();
    ITransaction* CreateTxMigrate();
    ITransaction* CreateTxLoadEverything();
    ITransaction* CreateTxUpdateSeenOperational(TVector<TGroupId> groups);

    struct TVDiskAvailabilityTiming {
        TVSlotId VSlotId;
        TInstant LastSeenReady;
        TInstant LastGotReplicating;
        TDuration ReplicationTime;

        TVDiskAvailabilityTiming() = default;
        TVDiskAvailabilityTiming(const TVDiskAvailabilityTiming&) = default;
        TVDiskAvailabilityTiming& operator=(const TVDiskAvailabilityTiming&) = default;

        TVDiskAvailabilityTiming(const TVSlotInfo& vslot)
            : VSlotId(vslot.VSlotId)
            , LastSeenReady(vslot.LastSeenReady)
            , LastGotReplicating(vslot.LastGotReplicating)
            , ReplicationTime(vslot.ReplicationTime)
        {}
    };
    ITransaction* CreateTxUpdateLastSeenReady(std::vector<TVDiskAvailabilityTiming> timingQ);

    void Handle(TEvPrivate::TEvDropDonor::TPtr ev);

    void AllocatePDiskWithSerial(TConfigState& state, ui32 nodeId, const TSerial& serial, TDriveSerialInfo *driveInfo);
    void ValidatePDiskWithSerial(TConfigState& state, ui32 nodeId, const TSerial& serial, const TDriveSerialInfo& driveInfo,
            std::function<TDriveSerialInfo*()> getMutableItem);
    void FitPDisksForUserConfig(TConfigState &state);
    void FitGroupsForUserConfig(TConfigState &state, ui32 availabilityDomainId,
        const NKikimrBlobStorage::TConfigRequest& cmd, std::deque<ui64> expectedSlotSize,
        NKikimrBlobStorage::TConfigResponse::TStatus& status);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_CONTROLLER_ACTOR;
    }

    TBlobStorageController(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , ResponsivenessPinger(nullptr)
        , ScrubState(this)
    {
        using namespace NBlobStorageController;
        TabletCountersPtr.Reset(new TProtobufTabletCounters<
            ESimpleCounters_descriptor,
            ECumulativeCounters_descriptor,
            EPercentileCounters_descriptor,
            ETxTypes_descriptor
        >());
        TabletCounters = TabletCountersPtr.Get();
    }

    STFUNC(StateInit) {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC05, "StateInit event", (Type, ev->GetTypeRewrite()),
            (Event, ev->ToString()));
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            default:
                StateInitImpl(ev, SelfId());
        }
    }

    std::multimap<ui32, std::unique_ptr<IEventHandle>> IncomingEventQ; // sorted by priority

    ui32 GetEventPriority(IEventHandle *ev);

    void EnqueueIncomingEvent(STATEFN_SIG) {
        const ui32 priority = GetEventPriority(ev.Get());
        if (IncomingEventQ.empty()) { // for the first event in the queue we have to push special event
            PushProcessIncomingEvent();
        }
        IncomingEventQ.emplace(priority, std::unique_ptr<IEventHandle>(ev.Release()));
    }

    void PushProcessIncomingEvent() {
        TActivationContext::Send(new IEventHandle(TEvPrivate::EvProcessIncomingEvent, 0, SelfId(), {}, nullptr, 0));
    }

    void ProcessIncomingEvent() {
        Y_ABORT_UNLESS(!IncomingEventQ.empty());
        const auto it = IncomingEventQ.begin();
        ProcessControllerEvent(it->second.release());
        IncomingEventQ.erase(it);
        if (!IncomingEventQ.empty()) {
            PushProcessIncomingEvent();
        }
    }

    bool ValidateIncomingNodeWardenEvent(const IEventHandle& ev) {
        auto makeError = [&](TString message) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSC16, "ValidateIncomingNodeWardenEvent error",
                (Sender, ev.Sender), (PipeServerId, ev.Recipient), (InterconnectSessionId, ev.InterconnectSession),
                (Type, ev.GetTypeRewrite()), (Message, message));
            return false;
        };

        switch (ev.GetTypeRewrite()) {
            case TEvBlobStorage::EvControllerRegisterNode: {
                if (const auto pipeIt = PipeServerToNode.find(ev.Recipient); pipeIt == PipeServerToNode.end()) {
                    return makeError("incorrect pipe server");
                } else if (pipeIt->second) {
                    return makeError("duplicate RegisterNode event");
                }
                break;
            }

            case TEvBlobStorage::EvControllerGetGroup:
                if (ev.Sender == SelfId()) {
                    break;
                } else if (ev.Cookie == Max<ui64>()) {
                    break; // for testing purposes
                }
                [[fallthrough]];
            case TEvBlobStorage::EvControllerUpdateDiskStatus:
            case TEvBlobStorage::EvControllerProposeGroupKey:
            case TEvBlobStorage::EvControllerUpdateGroupStat:
            case TEvBlobStorage::EvControllerScrubQueryStartQuantum:
            case TEvBlobStorage::EvControllerScrubQuantumFinished:
            case TEvBlobStorage::EvControllerScrubReportQuantumInProgress:
            case TEvBlobStorage::EvControllerUpdateNodeDrives:
            case TEvBlobStorage::EvControllerNodeReport: {
                if (const auto pipeIt = PipeServerToNode.find(ev.Recipient); pipeIt == PipeServerToNode.end()) {
                    return makeError("incorrect pipe server");
                } else if (const auto& nodeId = pipeIt->second; !nodeId) {
                    return makeError("no RegisterNode event received");
                } else if (*nodeId != ev.Sender.NodeId()) {
                    return makeError("NodeId mismatch");
                } else if (TNodeInfo *node = FindNode(*nodeId); !node) {
                    return makeError("no TNodeInfo record for node");
                } else if (node->InterconnectSessionId != ev.InterconnectSession) {
                    return makeError("InterconnectSession mismatch");
                } else if (node->ConnectedServerId != ev.Recipient) {
                    return makeError("pipe server mismatch");
                }
                break;
            }
        }

        return true;
    }

    void ProcessControllerEvent(TAutoPtr<IEventHandle> ev) {
        const ui32 type = ev->GetTypeRewrite();
        THPTimer timer;

        if (!ValidateIncomingNodeWardenEvent(*ev)) {
            return;
        }

        switch (type) {
            hFunc(TEvBlobStorage::TEvControllerRegisterNode, Handle);
            hFunc(TEvBlobStorage::TEvControllerGetGroup, Handle);
            hFunc(TEvBlobStorage::TEvControllerSelectGroups, Handle);
            hFunc(TEvBlobStorage::TEvControllerUpdateDiskStatus, Handle);
            hFunc(TEvBlobStorage::TEvControllerUpdateGroupStat, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupMetricsExchange, Handle);
            hFunc(TEvBlobStorage::TEvControllerUpdateNodeDrives, Handle);
            hFunc(TEvControllerCommitGroupLatencies, Handle);
            hFunc(TEvBlobStorage::TEvRequestControllerInfo, Handle);
            hFunc(TEvBlobStorage::TEvControllerNodeReport, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigRequest, Handle);
            hFunc(TEvBlobStorage::TEvControllerProposeGroupKey, Handle);
            hFunc(TEvPrivate::TEvUpdateSystemViews, Handle);
            cFunc(TEvPrivate::EvUpdateSelfHealCounters, UpdateSelfHealCounters);
            hFunc(TEvPrivate::TEvDropDonor, Handle);
            hFunc(TEvBlobStorage::TEvControllerScrubQueryStartQuantum, Handle);
            hFunc(TEvBlobStorage::TEvControllerScrubQuantumFinished, Handle);
            hFunc(TEvBlobStorage::TEvControllerScrubReportQuantumInProgress, Handle);
            hFunc(TEvBlobStorage::TEvControllerGroupDecommittedNotify, Handle);
            cFunc(TEvPrivate::EvScrub, ScrubState.HandleTimer);
            cFunc(TEvPrivate::EvVSlotReadyUpdate, VSlotReadyUpdate);
        }

        if (const TDuration time = TDuration::Seconds(timer.Passed()); time >= TDuration::MilliSeconds(100)) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSC07, "ProcessControllerEvent event processing took too much time", (Type, type),
                (Duration, time));
        }
    }

    STFUNC(StateWork) {
        const ui32 type = ev->GetTypeRewrite();
        THPTimer timer;

        switch (type) {
            fFunc(TEvBlobStorage::EvControllerRegisterNode, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerGetGroup, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerSelectGroups, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerUpdateDiskStatus, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerUpdateGroupStat, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerGroupMetricsExchange, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerUpdateNodeDrives, EnqueueIncomingEvent);
            fFunc(TEvControllerCommitGroupLatencies::EventType, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvRequestControllerInfo, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerNodeReport, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerConfigRequest, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerProposeGroupKey, EnqueueIncomingEvent);
            fFunc(NSysView::TEvSysView::EvGetPDisksRequest, ForwardToSystemViewsCollector);
            fFunc(NSysView::TEvSysView::EvGetVSlotsRequest, ForwardToSystemViewsCollector);
            fFunc(NSysView::TEvSysView::EvGetGroupsRequest, ForwardToSystemViewsCollector);
            fFunc(NSysView::TEvSysView::EvGetStoragePoolsRequest, ForwardToSystemViewsCollector);
            fFunc(NSysView::TEvSysView::EvGetStorageStatsRequest, ForwardToSystemViewsCollector);
            fFunc(TEvPrivate::EvUpdateSystemViews, EnqueueIncomingEvent);
            hFunc(TEvInterconnect::TEvNodesInfo, Handle);
            hFunc(TEvTabletPipe::TEvServerConnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            fFunc(TEvPrivate::EvUpdateSelfHealCounters, EnqueueIncomingEvent);
            fFunc(TEvPrivate::EvDropDonor, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerScrubQueryStartQuantum, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerScrubQuantumFinished, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerScrubReportQuantumInProgress, EnqueueIncomingEvent);
            fFunc(TEvBlobStorage::EvControllerGroupDecommittedNotify, EnqueueIncomingEvent);
            fFunc(TEvPrivate::EvScrub, EnqueueIncomingEvent);
            fFunc(TEvPrivate::EvVSlotReadyUpdate, EnqueueIncomingEvent);
            cFunc(TEvPrivate::EvVSlotNotReadyHistogramUpdate, VSlotNotReadyHistogramUpdate);
            cFunc(TEvPrivate::EvProcessIncomingEvent, ProcessIncomingEvent);
            hFunc(TEvNodeWardenStorageConfig, Handle);
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    STLOG(PRI_ERROR, BS_CONTROLLER, BSC06, "StateWork unexpected event", (Type, type),
                        (Event, ev->ToString()));
                }
            break;
        }

        if (const TDuration time = TDuration::Seconds(timer.Passed()); time >= TDuration::MilliSeconds(100)) {
            STLOG(PRI_ERROR, BS_CONTROLLER, BSC00, "StateWork event processing took too much time", (Type, type),
                (Duration, time));
        }
    }

    void LoadFinished() {
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC09, "LoadFinished");
        Become(&TThis::StateWork);

        ValidateInternalState();
        UpdatePDisksCounters();
        IssueInitialGroupContent();
        InitializeSelfHealState();
        UpdateSystemViews();
        UpdateSelfHealCounters();
        SignalTabletActive(TActivationContext::AsActorContext());
        Loaded = true;
        ApplyStorageConfig();

        for (const auto& [id, info] : GroupMap) {
            if (info->VirtualGroupState) {
                StartVirtualGroupSetupMachine(info.Get());
            }
        }
        for (auto& [groupId, info] : BlobDepotDeleteQueue) {
            StartVirtualGroupDeleteMachine(groupId, info);
        }

        for (; !InitQueue.empty(); InitQueue.pop_front()) {
            TAutoPtr<IEventHandle> &ev = InitQueue.front();
            STLOG(PRI_DEBUG, BS_CONTROLLER, BSC08, "Dequeue", (TabletID, TabletID()), (Type, ev->GetTypeRewrite()),
                (Event, ev->ToString()));
            StateWork(ev);
        }
    }

    void UpdatePDisksCounters() {
        ui32 numWithoutSlotCount = 0;
        ui32 numWithoutSerial = 0;
        for (const auto& [id, pdisk] : PDisks) {
            numWithoutSlotCount += !pdisk->HasExpectedSlotCount;
            numWithoutSerial += !pdisk->ExpectedSerial;
        }
        auto& counters = TabletCounters->Simple();
        counters[NBlobStorageController::COUNTER_PDISKS_WITHOUT_EXPECTED_SLOT_COUNT].Set(numWithoutSlotCount);
        counters[NBlobStorageController::COUNTER_PDISKS_WITHOUT_EXPECTED_SERIAL].Set(numWithoutSerial);

        ui32 numFree = 0;
        ui32 numAdded = 0;
        ui32 numRemoved = 0;
        for (const auto& [serial, driveInfo] : DrivesSerials) {
            switch (driveInfo->LifeStage) {
                case NKikimrBlobStorage::TDriveLifeStage::FREE:
                    ++numFree;
                    break;
                case NKikimrBlobStorage::TDriveLifeStage::ADDED_BY_DSTOOL:
                    ++numAdded;
                    break;
                case NKikimrBlobStorage::TDriveLifeStage::REMOVED_BY_DSTOOL:
                    ++numRemoved;
                    break;
                default:
                    break;
            }
        }

        counters[NBlobStorageController::COUNTER_DRIVE_SERIAL_FREE].Set(numFree);
        counters[NBlobStorageController::COUNTER_DRIVE_SERIAL_ADDED_BY_DSTOOL].Set(numAdded);
        counters[NBlobStorageController::COUNTER_DRIVE_SERIAL_REMOVED_BY_DSTOOL].Set(numRemoved);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // VIRTUAL GROUP MANAGEMENT

    class TVirtualGroupSetupMachine;

    void CommitVirtualGroupUpdates(TConfigState& state);

    void StartVirtualGroupSetupMachine(TGroupInfo *group);
    void StartVirtualGroupDeleteMachine(TGroupId groupId, TBlobDepotDeleteQueueInfo& info);

    void Handle(TEvBlobStorage::TEvControllerGroupDecommittedNotify::TPtr ev);

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // VSLOT READINESS EVALUATION

    TVSlotReadyTimestampQ VSlotReadyTimestampQ;
    bool VSlotReadyUpdateScheduled = false;

    void VSlotReadyUpdate() {
        std::vector<TVDiskAvailabilityTiming> timingQ;

        Y_ABORT_UNLESS(VSlotReadyUpdateScheduled);
        VSlotReadyUpdateScheduled = false;

        const TMonotonic now = TActivationContext::Monotonic();
        THashSet<TGroupInfo*> groups;

        std::vector<TEvControllerUpdateSelfHealInfo::TVDiskStatusUpdate> updates;
        for (auto it = VSlotReadyTimestampQ.begin(); it != VSlotReadyTimestampQ.end() && it->first <= now;
                it = VSlotReadyTimestampQ.erase(it)) {
            Y_DEBUG_ABORT_UNLESS(!it->second->IsReady);
            
            updates.push_back({
                .VDiskId = it->second->GetVDiskId(),
                .IsReady = true,
            });
            it->second->IsReady = true;
            it->second->ResetVSlotReadyTimestampIter();
            if (const TGroupInfo *group = it->second->Group) {
                groups.insert(const_cast<TGroupInfo*>(group));
                it->second->LastSeenReady = TInstant::Zero();
                timingQ.emplace_back(*it->second);
                NotReadyVSlotIds.erase(it->second->VSlotId);
            }
            ScrubState.UpdateVDiskState(&*it->second);
        }
        for (TGroupInfo *group : groups) {
            group->CalculateGroupStatus();
        }
        ScheduleVSlotReadyUpdate();
        if (!timingQ.empty()) {
            Execute(CreateTxUpdateLastSeenReady(std::move(timingQ)));
        }
        if (!updates.empty()) {
            Send(SelfHealId, new TEvControllerUpdateSelfHealInfo(std::move(updates)));
        }
    }

    void ScheduleVSlotReadyUpdate() {
        if (!VSlotReadyTimestampQ.empty() && !std::exchange(VSlotReadyUpdateScheduled, true)) {
            Schedule(VSlotReadyTimestampQ.front().first, new TEvPrivate::TEvVSlotReadyUpdate);
        }
    }

    void VSlotNotReadyHistogramUpdate() {
        const TInstant now = TActivationContext::Now();
        auto& histo = TabletCounters->Percentile()[NBlobStorageController::COUNTER_NUM_NOT_READY_VDISKS];
        auto& histoReplROT = TabletCounters->Percentile()[NBlobStorageController::COUNTER_NUM_REPLICATING_VDISKS_ROT];
        auto& histoReplOther = TabletCounters->Percentile()[NBlobStorageController::COUNTER_NUM_REPLICATING_VDISKS_OTHER];
        histo.Clear();
        histoReplROT.Clear();
        histoReplOther.Clear();
        for (const TVSlotId vslotId : NotReadyVSlotIds) {
            if (const TVSlotInfo *slot = FindVSlot(vslotId)) {
                Y_ABORT_UNLESS(slot->LastSeenReady != TInstant::Zero());
                const TDuration passed = now - slot->LastSeenReady;
                histo.IncrementFor(passed.Seconds());

                TDuration timeBeingReplicating = slot->ReplicationTime;
                if (slot->GetStatus() == NKikimrBlobStorage::EVDiskStatus::REPLICATING) {
                    timeBeingReplicating += now - slot->LastGotReplicating;
                }

                if (timeBeingReplicating != TDuration::Zero()) {
                    auto& hist = slot->PDisk->Kind.Type() == NPDisk::DEVICE_TYPE_ROT
                        ? histoReplROT
                        : histoReplOther;
                    hist.IncrementFor(timeBeingReplicating.Seconds());
                }
            } else {
                Y_DEBUG_ABORT_UNLESS(false);
            }
        }
        Schedule(TDuration::Seconds(15), new TEvPrivate::TEvVSlotNotReadyHistogramUpdate);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // STATIC GROUP OVERSEEING

    struct TStaticVSlotInfo {
        const TVDiskID VDiskId;
        const NKikimrBlobStorage::TVDiskKind::EVDiskKind VDiskKind;

        std::optional<NKikimrBlobStorage::TVDiskMetrics> VDiskMetrics;
        std::optional<NKikimrBlobStorage::EVDiskStatus> VDiskStatus;
        NHPTimer::STime VDiskStatusTimestamp = GetCycleCountFast();
        TMonotonic ReadySince = TMonotonic::Max(); // when IsReady becomes true for this disk; Max() in non-READY state

        TStaticVSlotInfo(const NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk& vdisk,
                std::map<TVSlotId, TStaticVSlotInfo>& prev)
            : VDiskId(VDiskIDFromVDiskID(vdisk.GetVDiskID()))
            , VDiskKind(vdisk.GetVDiskKind())
        {
            const auto& loc = vdisk.GetVDiskLocation();
            const TVSlotId vslotId(loc.GetNodeID(), loc.GetPDiskID(), loc.GetVDiskSlotID());
            if (const auto it = prev.find(vslotId); it != prev.end()) {
                TStaticVSlotInfo& item = it->second;
                VDiskMetrics = std::move(item.VDiskMetrics);
                VDiskStatus = item.VDiskStatus;
                VDiskStatusTimestamp = item.VDiskStatusTimestamp;
                ReadySince = item.ReadySince;
            }
        }
    };

    std::map<TVSlotId, TStaticVSlotInfo> StaticVSlots;
    std::map<TVDiskID, TVSlotId> StaticVDiskMap;

    struct TStaticPDiskInfo {
        const Schema::PDisk::NodeID::Type NodeId;
        const Schema::PDisk::PDiskID::Type PDiskId;
        const TString Path;
        const TPDiskCategory Category;
        const Schema::PDisk::Guid::Type Guid;
        Schema::PDisk::PDiskConfig::Type PDiskConfig;
        ui32 ExpectedSlotCount = 0;

        // runtime info
        ui32 StaticSlotUsage = 0;
        std::optional<NKikimrBlobStorage::TPDiskMetrics> PDiskMetrics;

        TStaticPDiskInfo(const NKikimrBlobStorage::TNodeWardenServiceSet::TPDisk& pdisk,
                std::map<TPDiskId, TStaticPDiskInfo>& prev)
            : NodeId(pdisk.GetNodeID())
            , PDiskId(pdisk.GetPDiskID())
            , Path(pdisk.GetPath())
            , Category(pdisk.GetPDiskCategory())
            , Guid(pdisk.GetPDiskGuid())
        {
            if (pdisk.HasPDiskConfig()) {
                const auto& cfg = pdisk.GetPDiskConfig();
                bool success = cfg.SerializeToString(&PDiskConfig);
                Y_ABORT_UNLESS(success);
                ExpectedSlotCount = cfg.GetExpectedSlotCount();
            }

            const TPDiskId pdiskId(NodeId, PDiskId);
            if (const auto it = prev.find(pdiskId); it != prev.end()) {
                TStaticPDiskInfo& item = it->second;
                PDiskMetrics = std::move(item.PDiskMetrics);
            }
        }
    };

    std::map<TPDiskId, TStaticPDiskInfo> StaticPDisks;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // NODE WARDEN PIPE LIFETIME MANAGEMENT

    THashMap<TActorId, std::optional<TNodeId>> PipeServerToNode;

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
    bool OnRegisterNode(const TActorId& serverId, TNodeId nodeId, TActorId interconnectSessionId);
    void OnWardenConnected(TNodeId nodeId, TActorId serverId, TActorId interconnectSessionId);
    void OnWardenDisconnected(TNodeId nodeId, TActorId serverId);
    void EraseKnownDrivesOnDisconnected(TNodeInfo *nodeInfo);
    void SendToWarden(TNodeId nodeId, std::unique_ptr<IEventBase> ev, ui64 cookie);
    void SendInReply(const IEventHandle& query, std::unique_ptr<IEventBase> ev);

    using TVSlotFinder = std::function<void(TVSlotId, const std::function<void(const TVSlotInfo&)>&)>;

    static void Serialize(NKikimrBlobStorage::TDefineHostConfig *pb, const THostConfigId &id, const THostConfigInfo &hostConfig);
    static void Serialize(NKikimrBlobStorage::TDefineBox *pb, const TBoxId &id, const TBoxInfo &box);
    static void Serialize(NKikimrBlobStorage::TDefineStoragePool *pb, const TBoxStoragePoolId &id, const TStoragePoolInfo &pool);
    static void Serialize(NKikimrBlobStorage::TPDiskFilter *pb, const TStoragePoolInfo::TPDiskFilter &filter);
    static void Serialize(NKikimrBlobStorage::TBaseConfig::TPDisk *pb, const TPDiskId &id, const TPDiskInfo &pdisk);
    static void Serialize(NKikimrBlobStorage::TVSlotId *pb, TVSlotId id);
    static void Serialize(NKikimrBlobStorage::TVDiskLocation *pb, const TVSlotInfo& vslot);
    static void Serialize(NKikimrBlobStorage::TVDiskLocation *pb, const TVSlotId& vslotId);
    static void Serialize(NKikimrBlobStorage::TBaseConfig::TVSlot *pb, const TVSlotInfo &vslot, const TVSlotFinder& finder);
    static void Serialize(NKikimrBlobStorage::TBaseConfig::TGroup *pb, const TGroupInfo &group);
    static void SerializeDonors(NKikimrBlobStorage::TNodeWardenServiceSet::TVDisk *vdisk, const TVSlotInfo& vslot,
        const TGroupInfo& group, const TVSlotFinder& finder);
    static void SerializeGroupInfo(NKikimrBlobStorage::TGroupInfo *group, const TGroupInfo& groupInfo,
        const TString& storagePoolName, const TMaybe<TKikimrScopeId>& scopeId);

    void SerializeSettings(NKikimrBlobStorage::TUpdateSettings *settings);

    static NKikimrBlobStorage::TGroupStatus::E DeriveStatus(const TBlobStorageGroupInfo::TTopology *topology,
        const TBlobStorageGroupInfo::TGroupVDisks& failed);
};

} //NBsController
} // NKikimr
