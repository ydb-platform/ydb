#include "impl.h"
#include "config.h"

namespace NKikimr::NBsController {

    class TBlobStorageController::TTxUpdateShred : public TTransactionBase<TBlobStorageController> {
        TBlobStorageController::TShredState::TImpl *Impl;
        const NKikimrBlobStorage::TEvControllerShredRequest Request;
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId InterconnectSession;
        bool Action = false;

    public:
        TTxUpdateShred(TShredState::TImpl *impl, TEvBlobStorage::TEvControllerShredRequest::TPtr ev);
        TTxType GetTxType() const override { return NBlobStorageController::TXTYPE_UPDATE_SHRED; }
        bool Execute(TTransactionContext& txc, const TActorContext&) override;
        void Complete(const TActorContext&) override;
    };

    class TBlobStorageController::TShredState::TImpl {
        friend class TTxUpdateShred;
        TBlobStorageController* const Self;
        NKikimrBlobStorage::TShredState ShredState;
        THashMap<TGroupId, ui32> GroupShredForbidden;
        THashMap<TPDiskId, ui32> PDiskShredForbidden;
        THashSet<TPDiskId> PDiskShredComplete;
        bool Ready = false;

    public:
        TImpl(TBlobStorageController *self)
            : Self(self)
        {}

        void Handle(TEvBlobStorage::TEvControllerShredRequest::TPtr ev) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSC00, "received TEvControllerShredRequest", (Record, ev->Get()->Record));
            Self->Execute(new TTxUpdateShred(this, ev));
        }

        void OnLoad(const TString& buffer) {
            const bool success = ShredState.ParseFromString(buffer);
            Y_ABORT_UNLESS(success);
        }

        void Initialize() {
            for (const auto& [pdiskId, pdiskInfo] : Self->PDisks) {
                if (pdiskInfo->ShredComplete) {
                    PDiskShredComplete.insert(pdiskId);
                }
            }
            TActivationContext::Schedule(TDuration::Seconds(20), new IEventHandle(TEvPrivate::EvUpdateShredState, 0,
                Self->SelfId(), TActorId(), nullptr, 0));
        }

        void HandleUpdateShredState() {
            Ready = true;
            UpdateShredState(0);
        }

        void UpdateShredState(TNodeId nodeId) {
            if (!ShredState.HasGeneration() || ShredState.GetCompleted() || !Ready) {
                return; // nothing to do or everything is already done
            }

            THashMap<TNodeId, std::unique_ptr<TEvBlobStorage::TEvControllerNodeServiceSetUpdate>> outbox;

            auto& pdisks = Self->PDisks;
            const TPDiskId from(nodeId, Min<Schema::PDisk::PDiskID::Type>());
            const TPDiskId to(nodeId ? nodeId : Max<Schema::PDisk::NodeID::Type>(), Max<Schema::PDisk::PDiskID::Type>());
            for (auto it = pdisks.lower_bound(from); it != pdisks.end() && !(to < it->first); ++it) {
                const auto& [pdiskId, pdiskInfo] = *it;

                if (pdiskInfo->ShredInProgress || pdiskInfo->ShredComplete || PDiskShredForbidden.contains(pdiskId)) {
                    continue;
                }

                if (TNodeInfo *node = Self->FindNode(pdiskId.NodeId); !node || !node->ConnectedServerId) {
                    continue;
                }

                // disallow shredding for PDisks sharing same groups as this one
                for (const auto& [id, vslot] : pdiskInfo->VSlotsOnPDisk) {
                    if (vslot->Group) {
                        ++GroupShredForbidden[vslot->Group->ID];
                        for (const auto& groupSlot : vslot->Group->VDisksInGroup) {
                            ++PDiskShredForbidden[groupSlot->VSlotId.ComprisingPDiskId()];
                        }
                    }
                }

                pdiskInfo->ShredInProgress = true;

                auto& ptr = outbox[pdiskId.NodeId];
                if (!ptr) {
                    ptr.reset(new TEvBlobStorage::TEvControllerNodeServiceSetUpdate);
                }
                auto& record = ptr->Record;
                record.SetNodeID(pdiskId.NodeId);
                auto *shredRequest = record.MutableShredRequest();
                shredRequest->SetShredGeneration(ShredState.GetGeneration());
                shredRequest->AddPDiskIds(pdiskId.PDiskId);
            }

            for (auto& [nodeId, ev] : outbox) {
                STLOG(PRI_DEBUG, BS_SHRED, BSSC01, "issuing TEvControllerNodeServiceSetUpdate", (NodeId, nodeId),
                    (Record, ev->Record));
                Self->Send(MakeBlobStorageNodeWardenID(nodeId), ev.release());
            }
        }

        std::optional<ui64> GetCurrentGeneration() const {
            return ShredState.HasGeneration() ? std::make_optional(ShredState.GetGeneration()) : std::nullopt;
        }

        bool ShouldShred(TPDiskId /*pdiskId*/, const TPDiskInfo& pdiskInfo) const {
            if (!ShredState.HasGeneration() || pdiskInfo.ShredComplete) {
                return false; // PDisk is already shredded
            } else if (pdiskInfo.ShredInProgress) {
                return true;
            } else {
                return false;
            }
        }

        void OnShredFinished(TPDiskId pdiskId, TPDiskInfo& pdiskInfo, ui64 generation, TTransactionContext& txc) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSC02, "shred finished", (PDiskId, pdiskId), (Generation, generation),
                (ShredInProgress, pdiskInfo.ShredInProgress), (ShredComplete, pdiskInfo.ShredComplete),
                (CurrentGeneration, GetCurrentGeneration()));

            if (pdiskInfo.ShredInProgress && !pdiskInfo.ShredComplete && generation == GetCurrentGeneration()) {
                pdiskInfo.ShredComplete = true;

                const auto [it, inserted] = PDiskShredComplete.insert(pdiskId);
                Y_ABORT_UNLESS(inserted);

                // recalculate progress
                const ui32 complete = PDiskShredComplete.size();
                const ui32 total = Self->PDisks.size();
                Y_ABORT_UNLESS(complete <= total);

                ShredState.SetProgress10k(complete * 10'000ULL / total);
                ShredState.SetCompleted(complete == total);
                NIceDb::TNiceDb db(txc.DB);
                TString buffer;
                const bool success = ShredState.SerializeToString(&buffer);
                Y_ABORT_UNLESS(success);
                db.Table<Schema::State>().Key(true).Update<Schema::State::ShredState>(buffer);
            }

            EndShredForPDisk(pdiskId, pdiskInfo);
        }

        void OnShredAborted(TPDiskId pdiskId, TPDiskInfo& pdiskInfo) {
            STLOG(PRI_DEBUG, BS_SHRED, BSSC03, "shred aborted", (PDiskId, pdiskId),
                (ShredInProgress, pdiskInfo.ShredInProgress), (ShredComplete, pdiskInfo.ShredComplete));

            EndShredForPDisk(pdiskId, pdiskInfo);
        }

        void OnNodeReportTxComplete() {
            UpdateShredState(0); // update all nodes as this report may have unblocked some PDisks from shredding
        }

        void EndShredForPDisk(TPDiskId /*pdiskId*/, TPDiskInfo& pdiskInfo) {
            if (pdiskInfo.ShredInProgress) {
                pdiskInfo.ShredInProgress = false;

                for (const auto& [vdiskSlotId, vslot] : pdiskInfo.VSlotsOnPDisk) {
                    if (vslot->Group) {
                        const auto it = GroupShredForbidden.find(vslot->Group->ID);
                        Y_ABORT_UNLESS(it != GroupShredForbidden.end());
                        if (!--it->second) {
                            GroupShredForbidden.erase(it);
                        }

                        for (const auto& groupSlot : vslot->Group->VDisksInGroup) {
                            const auto pdiskIt = PDiskShredForbidden.find(groupSlot->VSlotId.ComprisingPDiskId());
                            Y_ABORT_UNLESS(pdiskIt != PDiskShredForbidden.end());
                            if (!--pdiskIt->second) {
                                PDiskShredForbidden.erase(pdiskIt);
                            }
                        }
                    }
                }
            }
        }

        void OnRegisterNode(TPDiskId pdiskId, std::optional<ui64> shredCompleteGeneration, bool shredInProgress,
                TTransactionContext& txc) {
            const auto it = Self->PDisks.find(pdiskId);
            if (it == Self->PDisks.end()) {
                return; // may be some kind of race or something
            }
            TPDiskInfo& pdiskInfo = *it->second;

            if (shredCompleteGeneration) {
                OnShredFinished(pdiskId, pdiskInfo, *shredCompleteGeneration, txc);
            }

            if (pdiskInfo.ShredComplete) {
                return;
            }

            if (!shredInProgress) {
                OnShredAborted(pdiskId, pdiskInfo);
            } else if (!pdiskInfo.ShredInProgress) {
                for (const auto& [id, vslot] : pdiskInfo.VSlotsOnPDisk) {
                    if (vslot->Group) {
                        ++GroupShredForbidden[vslot->Group->ID];
                        for (const auto& groupSlot : vslot->Group->VDisksInGroup) {
                            ++PDiskShredForbidden[groupSlot->VSlotId.ComprisingPDiskId()];
                        }
                    }
                }
                pdiskInfo.ShredInProgress = true;
            }
        }

        void OnWardenConnected(TNodeId nodeId) {
            UpdateShredState(nodeId); // try to issue new shreds specifically for this node
        }

        void Render(IOutputStream& str, const TCgiParameters& cgi) {
            HTML(str) {
                TAG(TH3) {
                    str << "Shred state";
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Start new shredding iteration";
                    }
                    DIV_CLASS("panel-body") {
						FORM_CLASS("form-horizontal") {
							DIV_CLASS("control-group") {
								LABEL_CLASS_FOR("control-label", "generation") { str << "Shred generation"; }
								DIV_CLASS("controls") {
									str << "<input id='generation' name='generation' type='number'/>";
                                    for (const auto& [key, value] : cgi) {
                                        constexpr char apos = '\'';
                                        auto escape = [&](const TString& x) {
                                            for (char ch : x) {
                                                switch (ch) {
                                                    case apos: str << "&#39;"; break;
                                                    case '&':  str << "&amp;"; break;
                                                    case '<':  str << "&lt;" ; break;
                                                    case '>':  str << "&gt;" ; break;
                                                    default:   str << ch     ; break;
                                                }
                                            }
                                        };
                                        str << "<input type='hidden' name=" << apos;
                                        escape(key);
                                        str << apos << " value=" << apos;
                                        escape(value);
                                        str << apos << "/>";
                                    }
								}
							}
							DIV_CLASS("control-group") {
								DIV_CLASS("controls") {
									str << "<button type='submit' name='startshred' class='btn btn-default'>Start</button>";
								}
							}
						}

                    }
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Settings";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "Parameter"; }
                                    TABLEH() { str << "Value"; }
                                }
                            }
                            TABLEBODY() {
                                TABLER() {
                                    TABLED() { str << "Generation"; }
                                    TABLED() { str << (ShredState.HasGeneration() ? ToString(ShredState.GetGeneration()) : "none"); }
                                }
                                TABLER() {
                                    TABLED() { str << "Progress"; }
                                    TABLED() {
                                        if (ShredState.HasProgress10k()) {
                                            const ui32 progress = ShredState.GetProgress10k();
                                            str << Sprintf("%d.%02d %%", progress / 100, progress % 100);
                                        } else {
                                            str << "none";
                                        }
                                    }
                                }
                                TABLER() {
                                    TABLED() { str << "Completed"; }
                                    TABLED() { str << (!ShredState.HasCompleted() || ShredState.GetCompleted() ? "yes" : "no"); }
                                }
                            }
                        }
                    }
                }
                DIV_CLASS("panel panel-info") {
                    DIV_CLASS("panel-heading") {
                        str << "Groups";
                    }
                    DIV_CLASS("panel-body") {
                        TABLE_CLASS("table") {
                            TABLEHEAD() {
                                TABLER() {
                                    TABLEH() { str << "GroupId"; }
                                    TABLEH() { str << "Shred in progress"; }
                                    TABLEH() { str << "PDisks"; }
                                }
                            }
                            TABLEBODY() {
                                THashSet<TPDiskId> listedPDisks;

                                auto renderPDisk = [&](TPDiskId pdiskId, const TPDiskInfo& pdiskInfo) {
                                    str << "<font color='" << (pdiskInfo.ShredComplete ? "green" :
                                        pdiskInfo.ShredInProgress ? "blue" : "red") << "'>" << pdiskId << "</font>";
                                };

                                for (const auto& [groupId, groupInfo] : Self->GroupMap) {
                                    TABLER() {
                                        TABLED() { str << groupId; }

                                        bool shredInProgress = false;
                                        for (const auto& vslot : groupInfo->VDisksInGroup) {
                                            shredInProgress = shredInProgress || vslot->PDisk->ShredInProgress;
                                        }
                                        TABLED() { str << (shredInProgress ? "yes" : "no"); }

                                        TABLED() {
                                            bool first = true;
                                            for (const auto& vslot : groupInfo->VDisksInGroup) {
                                                if (first) {
                                                    first = false;
                                                } else {
                                                    str << ' ';
                                                }
                                                renderPDisk(vslot->VSlotId.ComprisingPDiskId(), *vslot->PDisk);
                                                listedPDisks.insert(vslot->VSlotId.ComprisingPDiskId());
                                            }
                                        }
                                    }
                                }

                                TABLER() {
                                    TABLED() { str << "other ones"; }
                                    TABLED() {}
                                    TABLED() {
                                        for (const auto& [pdiskId, pdiskInfo] : Self->PDisks) {
                                            if (listedPDisks.contains(pdiskId)) {
                                                continue;
                                            }
                                            renderPDisk(pdiskId, *pdiskInfo);
                                            str << "<br/>";
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        void CommitShredUpdates(TConfigState& state) {
            for (const auto& [base, overlay] : state.VSlots.Diff()) {
                auto hasGroupOfInterest = [&](const auto& item) {
                    if constexpr (requires { static_cast<bool>(item); }) {
                        if (!item) {
                            return false;
                        }
                    }
                    return item->second && item->second->Group && GroupShredForbidden.contains(item->second->Group->ID);
                };
                if (int diff = hasGroupOfInterest(overlay) - hasGroupOfInterest(base)) {
                    const auto it = PDiskShredForbidden.find(overlay->first.ComprisingPDiskId());
                    Y_ABORT_UNLESS(it != PDiskShredForbidden.end());
                    it->second += diff;
                    if (!it->second) {
                        PDiskShredForbidden.erase(it);
                    }
                }
            }

            for (const auto& [base, overlay] : state.Groups.Diff()) {
                if (base && !overlay->second) { // group was deleted
                    GroupShredForbidden.erase(base->first);
                }
            }

            for (const auto& [base, overlay] : state.PDisks.Diff()) {
                if (base && !overlay->second) {
                    Y_ABORT_UNLESS(!PDiskShredForbidden.contains(base->first));
                }
            }
        }
    };

    TBlobStorageController::TTxUpdateShred::TTxUpdateShred(TShredState::TImpl *impl, TEvBlobStorage::TEvControllerShredRequest::TPtr ev)
        : TTransactionBase(impl->Self)
        , Impl(impl)
        , Request(std::move(ev->Get()->Record))
        , Sender(ev->Sender)
        , Cookie(ev->Cookie)
        , InterconnectSession(ev->InterconnectSession)
    {}

    bool TBlobStorageController::TTxUpdateShred::Execute(TTransactionContext& txc, const TActorContext&) {
        auto& current = Impl->ShredState;
        if (Request.HasGeneration() && (!current.HasGeneration() || current.GetGeneration() < Request.GetGeneration())) {
            // reset shred state to initial one with newly provided generation
            current.SetGeneration(Request.GetGeneration());
            current.SetCompleted(false);
            current.SetProgress10k(0);

            // serialize it to string and update database
            TString buffer;
            const bool success = current.SerializeToString(&buffer);
            Y_ABORT_UNLESS(success);
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::State>().Key(true).Update<Schema::State::ShredState>(buffer);

            // reset shred competion flag for all shredded PDisks
            for (auto& [pdiskId, pdiskInfo] : Self->PDisks) {
                if (pdiskInfo->ShredComplete) {
                    db.Table<Schema::PDisk>().Key(pdiskId.GetKey()).Update<Schema::PDisk::ShredComplete>(false);
                    pdiskInfo->ShredComplete = false;
                    Impl->PDiskShredComplete.erase(pdiskId);
                    Action = true;
                }
            }
        }

        return true;
    }

    void TBlobStorageController::TTxUpdateShred::Complete(const TActorContext&) {
        auto ev = std::make_unique<TEvBlobStorage::TEvControllerShredResponse>();
        auto& r = ev->Record;
        const auto& current = Impl->ShredState;
        if (current.HasGeneration()) {
            r.SetCurrentGeneration(current.GetGeneration());
            r.SetCompleted(current.GetCompleted());
            r.SetProgress10k(current.GetProgress10k());
        }

        auto h = std::make_unique<IEventHandle>(Sender, Impl->Self->SelfId(), ev.release(), 0, Cookie);
        if (InterconnectSession) {
            h->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
        }
        TActivationContext::Send(h.release());
        if (Action) {
            Impl->UpdateShredState(0);
        }
    }

    TBlobStorageController::TShredState::TShredState(TBlobStorageController *self)
        : Impl(std::make_unique<TImpl>(self))
    {}

    TBlobStorageController::TShredState::~TShredState() = default;

    void TBlobStorageController::TShredState::Handle(TEvBlobStorage::TEvControllerShredRequest::TPtr ev) {
        Impl->Handle(ev);
    }

    void TBlobStorageController::TShredState::OnLoad(const TString& buffer) {
        Impl->OnLoad(buffer);
    }

    void TBlobStorageController::TShredState::Initialize() {
        Impl->Initialize();
    }

    void TBlobStorageController::TShredState::HandleUpdateShredState() {
        Impl->HandleUpdateShredState();
    }

    std::optional<ui64> TBlobStorageController::TShredState::GetCurrentGeneration() const {
        return Impl->GetCurrentGeneration();
    }

    bool TBlobStorageController::TShredState::ShouldShred(TPDiskId pdiskId, const TPDiskInfo& pdiskInfo) const {
        return Impl->ShouldShred(pdiskId, pdiskInfo);
    }

    void TBlobStorageController::TShredState::OnShredFinished(TPDiskId pdiskId, TPDiskInfo& pdiskInfo, ui64 generation,
            TTransactionContext& txc) {
        Impl->OnShredFinished(pdiskId, pdiskInfo, generation, txc);
    }

    void TBlobStorageController::TShredState::OnShredAborted(TPDiskId pdiskId, TPDiskInfo& pdiskInfo) {
        Impl->OnShredAborted(pdiskId, pdiskInfo);
    }

    void TBlobStorageController::TShredState::OnNodeReportTxComplete() {
        Impl->OnNodeReportTxComplete();
    }

    void TBlobStorageController::TShredState::OnRegisterNode(TPDiskId pdiskId, std::optional<ui64> shredCompleteGeneration,
            bool shredInProgress, TTransactionContext& txc) {
        Impl->OnRegisterNode(pdiskId, shredCompleteGeneration, shredInProgress, txc);
    }

    void TBlobStorageController::TShredState::OnWardenConnected(TNodeId nodeId) {
        Impl->OnWardenConnected(nodeId);
    }

    void TBlobStorageController::TShredState::Render(IOutputStream& str, const TCgiParameters& cgi) {
        Impl->Render(str, cgi);
    }

    void TBlobStorageController::CommitShredUpdates(TConfigState& state) {
        ShredState.Impl->CommitShredUpdates(state);
    }

} // NKikimr::NBsController
