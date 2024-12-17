#include "impl.h"

namespace NKikimr::NBsController {

class TBlobStorageController::TTxScrubStart : public TTransactionBase<TBlobStorageController> {
    const TVSlotId VSlotId;
    const TInstant ScrubCycleStartTime;

public:
    TTxScrubStart(TBlobStorageController *controller, const TVSlotId& vslotId, TInstant scrubCycleStartTime)
        : TBase(controller)
        , VSlotId(vslotId)
        , ScrubCycleStartTime(scrubCycleStartTime)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_SCRUB_START; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Self->HasScrubVSlot(VSlotId)) {
            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::ScrubState;
            const T::TKey::Type key(VSlotId.NodeId, VSlotId.PDiskId, VSlotId.VSlotId);
            db.Table<T>().Key(key).Update<T::ScrubCycleStartTime>(ScrubCycleStartTime);
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TBlobStorageController::TTxScrubQuantumFinished : public TTransactionBase<TBlobStorageController> {
    const TVSlotId VSlotId;
    const std::optional<TString> State;
    const bool Success;
    const TInstant Timestamp;

public:
    TTxScrubQuantumFinished(TBlobStorageController *controller,
            const NKikimrBlobStorage::TEvControllerScrubQuantumFinished& r, const TInstant timestamp)
        : TBase(controller)
        , VSlotId(r.GetVSlotId())
        , State(r.HasState() ? std::make_optional(r.GetState()) : std::nullopt)
        , Success(r.GetSuccess())
        , Timestamp(timestamp)
    {}

    TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_SCRUB_QUANTUM_FINISHED; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        if (Self->HasScrubVSlot(VSlotId)) {
            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::ScrubState;
            const T::TKey::Type key(VSlotId.NodeId, VSlotId.PDiskId, VSlotId.VSlotId);
            if (State) {
                db.Table<T>().Key(key).Update<T::State>(*State);
            } else {
                db.Table<T>().Key(key).UpdateToNull<T::State>();
                db.Table<T>().Key(key).Update<T::ScrubCycleFinishTime, T::Success>(Timestamp, Success);
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {}
};

class TBlobStorageController::TScrubState::TImpl {
    static constexpr TDuration UnavailableResetTimeout = TDuration::Minutes(10);

    enum class EUserState : ui32 {
        WAITING_FOR_START = NBlobStorageController::COUNTER_DISK_SCRUB_WAITING_FOR_START,
        RUNNING = NBlobStorageController::COUNTER_DISK_SCRUB_RUNNING,
        IN_PROGRESS = NBlobStorageController::COUNTER_DISK_SCRUB_IN_PROGRESS,
        FINISHED_OK = NBlobStorageController::COUNTER_DISK_SCRUB_FINISHED_OK,
        FINISHED_ERR = NBlobStorageController::COUNTER_DISK_SCRUB_FINISHED_ERR,
    };

    // predicates for the reason of enqueued item being held out of queue
    struct TPredLockedByPDisk {};
    struct TPredLockedByGroup {};
    struct TPredLockedByTime {};
    struct TPredLockedByProhibitGroup {};
    struct TCandidates {};

    struct TVDiskItem
        : TIntrusiveListItem<TVDiskItem, TPredLockedByPDisk>
        , TIntrusiveListItem<TVDiskItem, TPredLockedByGroup>
        , TIntrusiveListItem<TVDiskItem, TPredLockedByTime>
        , TIntrusiveListItem<TVDiskItem, TPredLockedByProhibitGroup>
        , TIntrusiveListItem<TVDiskItem, TCandidates>
    {
        using Table = Schema::ScrubState;

        enum class EScrubState {
            IDLE, // disk did not ask for scrubbing yet
            ENQUEUED, // disk asked, but it was enqueued; must have valid position in the queue
            IN_PROGRESS, // disk is being scrubbed right now
        };

        const TVSlotId VSlotId;
        const TVDiskID VDiskId;
        std::optional<TString> State;
        TInstant ScrubCycleStartTime;
        TInstant ScrubCycleFinishTime;
        std::optional<bool> Success;
        EScrubState ScrubState = EScrubState::IDLE;
        ui64 Cookie = 0;
        ui64 QueueIndex = 0; // index inside the queue, or 0 if not in queue
        std::optional<EUserState> UserState;

        TVDiskItem(TVSlotId vslotId, TVDiskID vdiskId)
            : VSlotId(vslotId)
            , VDiskId(vdiskId)
        {}

        TVDiskItem(TVSlotId vslotId, TVDiskID vdiskId, std::optional<TString> state, TInstant scrubCycleStartTime,
                TInstant scrubCycleFinishTime, std::optional<bool> success)
            : VSlotId(vslotId)
            , VDiskId(vdiskId)
            , State(std::move(state))
            , ScrubCycleStartTime(scrubCycleStartTime)
            , ScrubCycleFinishTime(scrubCycleFinishTime)
            , Success(success)
        {}

        struct TCompare {
            bool operator ()(const TVDiskItem& x, const TVDiskItem& y) const { return x.VSlotId < y.VSlotId; }
            bool operator ()(const TVDiskItem& x, const TVSlotId& y) const { return x.VSlotId < y; }
            bool operator ()(const TVSlotId& x, const TVDiskItem& y) const { return x < y.VSlotId; }
            bool operator ()(const TVDiskItem& x, ui32 y) const { return x.VSlotId.NodeId < y; }
            using is_transparent = void;
        };
    };

    TBlobStorageController* const Self;
    std::set<TVDiskItem, TVDiskItem::TCompare> VDiskState;

    static TString ToString(TVDiskItem::EScrubState state) {
        switch (state) {
            case TVDiskItem::EScrubState::IDLE: return "IDLE";
            case TVDiskItem::EScrubState::ENQUEUED: return "ENQUEUED";
            case TVDiskItem::EScrubState::IN_PROGRESS: return "IN_PROGRESS";
        }
        Y_ABORT();
    }

    friend class TBlobStorageController;

public:
    TImpl(TBlobStorageController *self)
        : Self(self)
    {}

    TActorId SelfId() const {
        return Self->SelfId();
    }

    void Transition(TVDiskItem *scrub, std::optional<EUserState> state) {
        if (const auto prev = std::exchange(scrub->UserState, state); prev != state) {
            if (state) {
                auto& counter = Self->TabletCounters->Simple()[static_cast<ui32>(*state)];
                counter.Add(1);
            }
            if (prev) {
                auto& counter = Self->TabletCounters->Simple()[static_cast<ui32>(*prev)];
                counter.Set(counter.Get() - 1);
            }
        }
    }

    void AdjustUserState(TVDiskItem *scrub) {
        if (scrub->ScrubState == TVDiskItem::EScrubState::IN_PROGRESS) {
            Transition(scrub, EUserState::IN_PROGRESS);
        } else if (scrub->State) {
            Transition(scrub, EUserState::RUNNING);
        } else if (!scrub->Success) {
            Transition(scrub, EUserState::WAITING_FOR_START);
        } else if (*scrub->Success) {
            Transition(scrub, EUserState::FINISHED_OK);
        } else {
            Transition(scrub, EUserState::FINISHED_ERR);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TPtr ev, TInstant now) {
        const auto& r = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC10, "Handle(TEvControllerScrubQueryStartQuantum)", (Msg, r));
        if (TVDiskItem *scrub = GetVDiskState(r)) {
            scrub->Cookie = ev->Cookie; // store cookie to issue correct answer later
            switch (scrub->ScrubState) {
                case TVDiskItem::EScrubState::IN_PROGRESS:
                    // BSC thought that scrub was in progress, but it suddenly got restarted; we handle it like the IDLE
                    // state here, but first we have to drop InProgress state
                    RemoveFromInProgress(scrub, now);
                    [[fallthrough]];
                case TVDiskItem::EScrubState::IDLE:
                    // ordinary situation -- we assume this disk idle and it asks for scrubbing permission; try to issue
                    // that permission, enqueue otherwise
                    PutToQueue(scrub);
                    scrub->ScrubState = TVDiskItem::EScrubState::ENQUEUED;
                    ProcessQueue(now);
                    break;

                case TVDiskItem::EScrubState::ENQUEUED:
                    // item is enqueued, but the disk is asking again for this permission; maybe VDisk has just restarted
                    // anyway, keep this item enqueued (with cookie updated)
                    break;
            }
        }
    }

    void Handle(TEvBlobStorage::TEvControllerScrubQuantumFinished::TPtr ev, TInstant now) {
        const auto& r = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC11, "Handle(TEvControllerScrubQuantumFinished)", (Msg, r));
        if (TVDiskItem *scrub = GetVDiskState(r)) {
            switch (scrub->ScrubState) {
                case TVDiskItem::EScrubState::IDLE:
                    break;

                case TVDiskItem::EScrubState::ENQUEUED:
                    RemoveFromQueue(scrub);
                    break;

                case TVDiskItem::EScrubState::IN_PROGRESS:
                    RemoveFromInProgress(scrub, now);
                    break;
            }
            scrub->ScrubState = TVDiskItem::EScrubState::IDLE;

            do {
                if (r.HasState()) {
                    scrub->State = r.GetState();
                } else if (r.HasSuccess()) {
                    scrub->State = std::nullopt;
                    scrub->ScrubCycleFinishTime = now;
                    scrub->Success = r.GetSuccess();
                } else {
                    break; // quantum aborted
                }
                Self->Execute(new TTxScrubQuantumFinished(Self, r, now));
            } while (false);
            AdjustUserState(scrub);

            Self->TabletCounters->Cumulative()[NBlobStorageController::COUNTER_DISK_SCRUB_QUANTUM_FINISHED] += 1;
        }
    }

    void Handle(TEvBlobStorage::TEvControllerScrubReportQuantumInProgress::TPtr ev) {
        const auto& r = ev->Get()->Record;
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC12, "Handle(TEvControllerScrubReportQuantumInProgress)", (Msg, r));
        if (TVDiskItem *scrub = GetVDiskState(r)) {
            switch (scrub->ScrubState) {
                case TVDiskItem::EScrubState::ENQUEUED:
                    RemoveFromQueue(scrub);
                    [[fallthrough]];
                case TVDiskItem::EScrubState::IDLE:
                    SetInProgress(scrub);
                    break;
                case TVDiskItem::EScrubState::IN_PROGRESS:
                    break;
            }
            scrub->ScrubState = TVDiskItem::EScrubState::IN_PROGRESS;
            AdjustUserState(scrub);
        }
    }

    void OnNodeDisconnected(TNodeId nodeId, TInstant now) {
        for (auto it = VDiskState.lower_bound(nodeId); it != VDiskState.end() && it->VSlotId.NodeId == nodeId; ++it) {
            TVDiskItem *scrub = &const_cast<TVDiskItem&>(*it);
            switch (scrub->ScrubState) {
                case TVDiskItem::EScrubState::ENQUEUED:
                    RemoveFromQueue(scrub);
                    break;
                case TVDiskItem::EScrubState::IN_PROGRESS:
                    RemoveFromInProgress(scrub, now);
                    break;
                case TVDiskItem::EScrubState::IDLE:
                    break;
            }
            scrub->ScrubState = TVDiskItem::EScrubState::IDLE;
            AdjustUserState(scrub);
        }
    }

    TVDiskItem *GetVDiskState(TVSlotId vslotId) {
        // lookup for existing items
        auto it = VDiskState.lower_bound(vslotId);
        if (it != VDiskState.end() && it->VSlotId == vslotId) {
            return const_cast<TVDiskItem*>(&*it);
        }

        // find VDisk id for this vslot
        TVDiskID vdiskId;
        if (TVSlotInfo *slot = Self->FindVSlot(vslotId); slot && !slot->IsBeingDeleted()) {
            vdiskId = slot->GetVDiskId();
        } else if (const auto it = Self->StaticVSlots.find(vslotId); it != Self->StaticVSlots.end()) {
            vdiskId = it->second.VDiskId;
        } else {
            return nullptr;
        }
        vdiskId.GroupGeneration = 0;

        // create entry for static group
        auto *scrub = const_cast<TVDiskItem*>(&*VDiskState.emplace_hint(it, vslotId, vdiskId));
        AdjustUserState(scrub);
        return scrub;
    }

    template<typename TProto>
    TVDiskItem *GetVDiskState(const TProto& proto) {
        return GetVDiskState(TVSlotId(proto.GetVSlotId()));
    }

    void AddItem(TVSlotId vslotId, std::optional<TString> state, TInstant scrubCycleStartTime,
            TInstant scrubCycleFinishTime, std::optional<bool> success) {
        TVDiskID vdiskId;
        if (TVSlotInfo *slot = Self->FindVSlot(vslotId)) {
            if (slot->IsBeingDeleted()) {
                return;
            }
            vdiskId = slot->GetVDiskId();
        } else if (const auto it = Self->StaticVSlots.find(vslotId); it != Self->StaticVSlots.end()) {
            vdiskId = it->second.VDiskId;
        } else {
            STLOG(PRI_WARN, BS_CONTROLLER, BSC40,
                "TBlobStorageController::TScrubState::TImpl::AddItem no VDiskId found for the slot", (VSlotId, vslotId));
            return;  // To allow static group 'reconfiguration' instead of Y_FAIL_S("unexpected VSlotId# " << vslotId);
        }
        vdiskId.GroupGeneration = 0;

        auto [it, inserted] = VDiskState.emplace(vslotId, vdiskId, std::move(state), scrubCycleStartTime,
            scrubCycleFinishTime, success);
        Y_ABORT_UNLESS(inserted);
        AdjustUserState(&const_cast<TVDiskItem&>(*it));
    }

    void OnDeletePDisk(TPDiskId pdiskId) {
        CurrentlyScrubbedDisks.erase(pdiskId);
        Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_DISKS].Set(CurrentlyScrubbedDisks.size());
    }

    void OnDeleteVSlot(TVSlotId vslotId, TTransactionContext& txc, TInstant now) {
        if (const auto it = VDiskState.find(vslotId); it != VDiskState.end()) {
            TVDiskItem *scrub = &const_cast<TVDiskItem&>(*it);
            switch (scrub->ScrubState) {
                case TVDiskItem::EScrubState::IDLE:
                    break;

                case TVDiskItem::EScrubState::ENQUEUED:
                    RemoveFromQueue(scrub);
                    break;

                case TVDiskItem::EScrubState::IN_PROGRESS:
                    RemoveFromInProgress(scrub, now);
                    break;
            }

            NIceDb::TNiceDb db(txc.DB);
            using T = Schema::ScrubState;
            db.Table<T>().Key(vslotId.GetKey()).Delete();
            Transition(scrub, std::nullopt);
            VDiskState.erase(it);
            ProcessQueue(now);
        }
    }

    void OnDeleteGroup(TGroupId groupId) {
        CurrentlyScrubbedGroups.erase(groupId);
        ScrubProhibitedGroups.erase(groupId);
        Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_GROUPS].Set(CurrentlyScrubbedGroups.size());
    }

    void Render(IOutputStream& str) {
        HTML(str) {
            TAG(TH3) {
                str << "Scrub state"
                    << "<br/>"
                    << (Self->ScrubPeriodicity != TDuration::Zero() ? TString(TStringBuilder() << "every " <<
                        Self->ScrubPeriodicity) : "disabled");
            }
            auto dumpBlockedSlots = [&](const auto& value) {
                for (auto it = value.begin(); it != value.end(); ++it) {
                    if (it != value.begin()) {
                        str << "<br/>";
                    }
                    str << it->VSlotId;
                }
            };
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Scrubbed PDisks";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "PDiskId"; }
                                TABLEH() { str << "Count"; }
                                TABLEH() { str << "Blocked slots"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [key, value] : CurrentlyScrubbedDisks) {
                                TABLER() {
                                    TABLED() { str << key; }
                                    TABLED() { str << value.Count; }
                                    TABLED() { dumpBlockedSlots(value); }
                                }
                            }
                        }
                    }
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Scrubbed Groups";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "GroupId"; }
                                TABLEH() { str << "Count"; }
                                TABLEH() { str << "Blocked slots"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [key, value] : CurrentlyScrubbedGroups) {
                                TABLER() {
                                    TABLED() { str << key; }
                                    TABLED() { str << value.Count; }
                                    TABLED() { dumpBlockedSlots(value); }
                                }
                            }
                        }
                    }
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Groups prohibited from scrubbing";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "GroupId"; }
                                TABLEH() { str << "Blocked slots"; }
                            }
                        }
                        TABLEBODY() {
                            for (const auto& [key, value] : ScrubProhibitedGroups) {
                                TABLER() {
                                    TABLED() { str << key; }
                                    TABLED() { dumpBlockedSlots(value); }
                                }
                            }
                        }
                    }
                }
            }
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "VDisk queue";
                }
                DIV_CLASS("panel-body") {
                    TABLE_CLASS("table") {
                        TABLEHEAD() {
                            TABLER() {
                                TABLEH() { str << "VSlotId"; }
                                TABLEH() { str << "VDiskId"; }
                                TABLEH() { str << "IsReady"; }
                                TABLEH() { str << "State"; }
                                TABLEH() { str << "ScrubCycleFinishTime"; }
                                TABLEH() { str << "Success"; }
                                TABLEH() { str << "ScrubCycleStartTime"; }
                                TABLEH() { str << "Running"; }
                            }
                        }
                        TABLEBODY() {
                            TInstant minScrubCycleFinishTime = TInstant::Max();
                            for (const auto& item : VDiskState) {
                                if (item.ScrubCycleFinishTime != TInstant::Zero() &&
                                        item.ScrubCycleFinishTime < minScrubCycleFinishTime) {
                                    minScrubCycleFinishTime = item.ScrubCycleFinishTime;
                                }
                            }

                            for (const auto& item : VDiskState) {
                                TABLER() {
                                    TABLED() { str << item.VSlotId; }
                                    TABLED() { str << item.VDiskId; }
                                    TABLED() {
                                        if (TVSlotInfo *slot = Self->FindVSlot(item.VSlotId)) {
                                            str << (slot->IsReady ? "Y" : "N");
                                            if (CurrentlyScrubbedDisks.count(item.VSlotId.ComprisingPDiskId())) {
                                                str << "/N";
                                            }
                                        }
                                    }
                                    TABLED() {
                                        str << ToString(item.ScrubState);
                                        if (item.ScrubState == TVDiskItem::EScrubState::ENQUEUED) {
                                            str << ", " << item.QueueIndex;
                                        }
                                    }
                                    TABLED() {
                                        if (item.ScrubCycleFinishTime == minScrubCycleFinishTime) {
                                            str << "<b>";
                                        }
                                        if (item.ScrubCycleFinishTime != TInstant::Zero()) {
                                            str << item.ScrubCycleFinishTime;
                                        }
                                        if (item.ScrubCycleFinishTime == minScrubCycleFinishTime) {
                                            str << "</b>";
                                        }
                                    }
                                    TABLED() { str << (!item.Success ? "" : *item.Success ? "ok" : "error"); }
                                    TABLED() { str << (item.ScrubCycleStartTime != TInstant::Zero()
                                        ? item.ScrubCycleStartTime.ToString() : "did not start"); }
                                    TABLED() { str << (item.State ? "yes" : ""); }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    template<typename TPred>
    struct TScrubbedEntityInfo : TIntrusiveList<TVDiskItem, TPred> {
        ui32 Count = 0;
    };

    struct TProhibitInfo : TIntrusiveList<TVDiskItem, TPredLockedByProhibitGroup>
    {};

    std::unordered_map<TPDiskId, TScrubbedEntityInfo<TPredLockedByPDisk>, THash<TPDiskId>> CurrentlyScrubbedDisks;
    std::unordered_map<TGroupId, TScrubbedEntityInfo<TPredLockedByGroup>> CurrentlyScrubbedGroups;
    std::unordered_map<TGroupId, TProhibitInfo> ScrubProhibitedGroups;
    std::map<TInstant, TIntrusiveList<TVDiskItem, TPredLockedByTime>> LockedByTime;
    ui64 LastQueueIndex = 0;
    TIntrusiveList<TVDiskItem, TCandidates> Candidates;
    using TCandidateItem = TIntrusiveListItem<TVDiskItem, TCandidates>;
    size_t NumCandidates = 0;
    bool ScrubDisabled = false;

    void PutToQueue(TVDiskItem *scrub) {
        Y_ABORT_UNLESS(!scrub->QueueIndex);
        scrub->QueueIndex = ++LastQueueIndex;
        AddCandidate(scrub);
    }

    void RemoveFromQueue(TVDiskItem *scrub) {
        Y_ABORT_UNLESS(scrub->QueueIndex);
        scrub->QueueIndex = 0;
        static_cast<TIntrusiveListItem<TVDiskItem, TPredLockedByPDisk>*>(scrub)->Unlink();
        static_cast<TIntrusiveListItem<TVDiskItem, TPredLockedByGroup>*>(scrub)->Unlink();
        static_cast<TIntrusiveListItem<TVDiskItem, TPredLockedByTime>*>(scrub)->Unlink();
        static_cast<TIntrusiveListItem<TVDiskItem, TPredLockedByProhibitGroup>*>(scrub)->Unlink();
        if (!static_cast<TCandidateItem*>(scrub)->Empty()) {
            --NumCandidates;
        }
        static_cast<TCandidateItem*>(scrub)->Unlink();
    }

    bool CheckIfScrubPermitted(TVDiskItem *scrub, TInstant now) {
        Y_ABORT_UNLESS(scrub->QueueIndex);

        if (Self->ScrubPeriodicity == TDuration::Zero()) {
            ScrubDisabled = true;
            return false; // scrub disabled
        }

        const TPDiskId pdiskId = scrub->VSlotId.ComprisingPDiskId();
        const TGroupId groupId = GetGroupId(scrub);

        if (const auto it = CurrentlyScrubbedDisks.find(pdiskId); it != CurrentlyScrubbedDisks.end()) {
            it->second.PushBack(scrub);
        } else if (const auto it = CurrentlyScrubbedGroups.find(groupId); it != CurrentlyScrubbedGroups.end()) {
            it->second.PushBack(scrub);
        } else if (const auto it = ScrubProhibitedGroups.find(groupId); it != ScrubProhibitedGroups.end()) {
            it->second.PushBack(scrub);
        } else if (scrub->Success && *scrub->Success && !scrub->State && now < scrub->ScrubCycleStartTime + Self->ScrubPeriodicity) {
            LockedByTime[scrub->ScrubCycleStartTime].PushBack(scrub);
        } else {
            return true;
        }

        return false;
    }

    void IssueScrubStartQuantum(TVDiskItem *scrub, TInstant now) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerScrubStartQuantum>(scrub->VSlotId.NodeId,
            scrub->VSlotId.PDiskId, scrub->VSlotId.VSlotId, scrub->State);
        STLOG(PRI_DEBUG, BS_CONTROLLER, BSC13, "sending TEvControllerScrubStartQuantum", (Msg, ev->ToString()));
        TActivationContext::Send(new IEventHandle(MakeBlobStorageNodeWardenID(scrub->VSlotId.NodeId), Self->SelfId(),
            ev.release(), 0, scrub->Cookie));
        SetInProgress(scrub);
        if (!scrub->State) { // scrubbing has just started for this disk
            scrub->ScrubCycleStartTime = now;
            Self->Execute(new TTxScrubStart(Self, scrub->VSlotId, scrub->ScrubCycleStartTime));
        }
    }

    void OnTimer(TInstant now) {
        std::map<TInstant, TIntrusiveList<TVDiskItem, TPredLockedByTime>>::iterator it;
        for (it = LockedByTime.begin(); it != LockedByTime.end() && it->first + Self->ScrubPeriodicity <= now; ++it) {
            AddCandidates(it->second);
        }
        LockedByTime.erase(LockedByTime.begin(), it);
        ProcessQueue(now);
    }

    void OnScrubPeriodicityChange(TInstant now) {
        if (Self->ScrubPeriodicity != TDuration::Zero() && ScrubDisabled) {
            for (const TVDiskItem& item : VDiskState) {
                TVDiskItem *scrub = const_cast<TVDiskItem*>(&item);
                if (scrub->QueueIndex && CheckIfScrubPermitted(scrub, now)) {
                    AddCandidate(scrub);
                }
            }
            ScrubDisabled = false;
        }
        OnTimer(now);
    }

    void OnMaxScrubbedDisksAtOnceChange(TInstant now) {
        ProcessQueue(now);
    }

    void ProcessQueue(TInstant now) {
        if (CurrentlyScrubbedDisks.size() >= Self->MaxScrubbedDisksAtOnce) {
            return;
        }

        // prepare a heap to traverse Candidates
        std::vector<TVDiskItem*> heap;
        heap.reserve(NumCandidates);
        for (auto& item : Candidates) {
            heap.push_back(&item);
        }
        auto comp = [](TVDiskItem *x, TVDiskItem *y) { return x->QueueIndex > y->QueueIndex; };
        std::make_heap(heap.begin(), heap.end(), comp); // make a min-heap

        while (!heap.empty()) {
            if (CurrentlyScrubbedDisks.size() >= Self->MaxScrubbedDisksAtOnce) {
                break;
            }

            // extract item with the least QueueIndex from the heap
            std::pop_heap(heap.begin(), heap.end());
            TVDiskItem *scrub = heap.back();
            heap.pop_back();

            // remove item from the candidates list -- we will either run it, or it will be blocked by some predicate
            static_cast<TCandidateItem*>(scrub)->Unlink();
            --NumCandidates;

            // some sanity checks
            auto it = VDiskState.find(scrub->VSlotId);
            Y_ABORT_UNLESS(it != VDiskState.end());
            Y_ABORT_UNLESS(scrub == &*it);
            Y_ABORT_UNLESS(scrub->ScrubState == TVDiskItem::EScrubState::ENQUEUED);

            // run scrubbing for this item if it is allowed
            if (CheckIfScrubPermitted(scrub, now)) {
                RemoveFromQueue(scrub);
                IssueScrubStartQuantum(scrub, now);
                scrub->ScrubState = TVDiskItem::EScrubState::IN_PROGRESS;
                AdjustUserState(scrub);
            }
        }
    }

    template<typename TPred>
    void AddCandidates(TIntrusiveList<TVDiskItem, TPred>& list) {
        list.ForEach([&](TVDiskItem *item) { AddCandidate(item); });
    }

    void AddCandidate(TVDiskItem *scrub) {
        if (static_cast<TCandidateItem*>(scrub)->Empty()) {
            Candidates.PushBack(scrub);
            ++NumCandidates;
        }
    }

    void RemoveFromInProgress(TVDiskItem *scrub, TInstant now) {
        const TPDiskId pdiskId = scrub->VSlotId.ComprisingPDiskId();
        const auto pdiskIt = CurrentlyScrubbedDisks.find(pdiskId);
        Y_ABORT_UNLESS(pdiskIt != CurrentlyScrubbedDisks.end());
        if (!--pdiskIt->second.Count) {
            AddCandidates(pdiskIt->second);
            CurrentlyScrubbedDisks.erase(pdiskIt);
            UpdateGroupProhibition(pdiskId);
            Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_DISKS].Set(CurrentlyScrubbedDisks.size());
        }

        const TGroupId groupId = GetGroupId(scrub);
        const auto groupIt = CurrentlyScrubbedGroups.find(groupId);
        Y_ABORT_UNLESS(groupIt != CurrentlyScrubbedGroups.end());
        if (!--groupIt->second.Count) {
            AddCandidates(groupIt->second);
            CurrentlyScrubbedGroups.erase(groupIt);
            Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_GROUPS].Set(CurrentlyScrubbedGroups.size());
        }

        ProcessQueue(now);
    }

    void SetInProgress(TVDiskItem *scrub) {
        // mark PDisk as the one being scrubbed and pop out all pending items from the queue on the same PDisk
        const TPDiskId pdiskId = scrub->VSlotId.ComprisingPDiskId();
        ++CurrentlyScrubbedDisks[pdiskId].Count;
        UpdateGroupProhibition(pdiskId);
        Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_DISKS].Set(CurrentlyScrubbedDisks.size());

        // do the same thing for the group
        const TGroupId groupId = GetGroupId(scrub);
        ++CurrentlyScrubbedGroups[groupId].Count;
        Self->TabletCounters->Simple()[NBlobStorageController::COUNTER_DISK_SCRUB_CUR_GROUPS].Set(CurrentlyScrubbedGroups.size());
    }

    TGroupId GetGroupId(TVDiskItem *scrub) {
        return scrub->VDiskId.GroupID;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Group prohibition logic. Any VSlot in group should not be scrubbed if there is any chance that group will become
    // DEGRADED. We presume that every scrubbing VDisk may render PDisk unusable due to read errors and related hardware
    // issues. So we check readiness and conjunction of IsReady property of every VDisk as reported to BSC (READY state
    // is being applied only after specific amount of time) and the fact that the underlying PDisk is not being scrubbed.
    // When this property may change for the group, UpdateGroupProhibition should be called. When this property changes
    // for the whole PDisk (CurrentlyScrubbedDisks are updated), then UpdateGroupProhibition overload is called in which
    // it enumerates every slot available on the PDisk.

    bool CheckGroupProhibition(const TGroupInfo *group) {
        TBlobStorageGroupInfo::TGroupVDisks working(&*group->Topology);
        for (const TVSlotInfo *slot : group->VDisksInGroup) {
            if (slot->IsReady && !CurrentlyScrubbedDisks.count(slot->VSlotId.ComprisingPDiskId())) {
                working += {&*group->Topology, slot->GetShortVDiskId()};
            }
        }
        return group->Topology->QuorumChecker->OneStepFromDegradedOrWorse(~working);
    }

    void UpdateVDiskState(const TVSlotInfo *slot, TInstant now) {
        if (slot->Group) {
            UpdateGroupProhibition(slot->Group);
        }
        ProcessQueue(now);
    }

    void UpdateGroupProhibition(const TGroupInfo *group) {
        if (CheckGroupProhibition(group)) {
            ScrubProhibitedGroups.try_emplace(group->ID);
        } else if (const auto it = ScrubProhibitedGroups.find(group->ID); it != ScrubProhibitedGroups.end()) {
            AddCandidates(it->second);
            ScrubProhibitedGroups.erase(it);
        }
    }

    void UpdateGroupProhibition(TPDiskId pdiskId) {
        if (TPDiskInfo *pdisk = Self->FindPDisk(pdiskId)) {
            for (const auto& [id, slot] : pdisk->VSlotsOnPDisk) {
                if (slot->Group) {
                    UpdateGroupProhibition(slot->Group);
                }
            }
        }
    }
};

TBlobStorageController::TScrubState::TScrubState(TBlobStorageController *self)
    : Impl(new TImpl(self))
{}

TBlobStorageController::TScrubState::~TScrubState()
{}

void TBlobStorageController::TScrubState::HandleTimer() {
    Impl->OnTimer(TActivationContext::Now());
    TActivationContext::Schedule(TDuration::Minutes(1), new IEventHandle(Impl->SelfId(), {}, new TEvPrivate::TEvScrub));
}

void TBlobStorageController::TScrubState::Clear() {
    Impl.reset(new TImpl(Impl->Self));
}

void TBlobStorageController::TScrubState::AddItem(TVSlotId vslotId, std::optional<TString> state,
        TInstant scrubCycleStartTime, TInstant scrubCycleFinishTime, std::optional<bool> success) {
    Impl->AddItem(vslotId, std::move(state), scrubCycleStartTime, scrubCycleFinishTime, success);
}

void TBlobStorageController::TScrubState::OnDeletePDisk(TPDiskId pdiskId) {
    Impl->OnDeletePDisk(pdiskId);
}

void TBlobStorageController::TScrubState::OnDeleteVSlot(TVSlotId vslotId, TTransactionContext& txc) {
    Impl->OnDeleteVSlot(vslotId, txc, TActivationContext::Now());
}

void TBlobStorageController::TScrubState::OnDeleteGroup(TGroupId groupId) {
    Impl->OnDeleteGroup(groupId);
}

void TBlobStorageController::TScrubState::Render(IOutputStream& str) {
    Impl->Render(str);
}

void TBlobStorageController::TScrubState::OnNodeDisconnected(TNodeId nodeId) {
    Impl->OnNodeDisconnected(nodeId, TActivationContext::Now());
}

void TBlobStorageController::TScrubState::OnScrubPeriodicityChange() {
    Impl->OnScrubPeriodicityChange(TActivationContext::Now());
}

void TBlobStorageController::TScrubState::OnMaxScrubbedDisksAtOnceChange() {
    Impl->OnMaxScrubbedDisksAtOnceChange(TActivationContext::Now());
}

void TBlobStorageController::TScrubState::UpdateVDiskState(const TVSlotInfo *slot) {
    Impl->UpdateVDiskState(slot, TActivationContext::Now());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerScrubQueryStartQuantum::TPtr ev) {
    ScrubState.Impl->Handle(ev, TActivationContext::Now());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerScrubQuantumFinished::TPtr ev) {
    ScrubState.Impl->Handle(ev, TActivationContext::Now());
}

void TBlobStorageController::Handle(TEvBlobStorage::TEvControllerScrubReportQuantumInProgress::TPtr ev) {
    ScrubState.Impl->Handle(ev);
}

}
