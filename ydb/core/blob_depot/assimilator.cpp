#include "blob_depot_tablet.h"
#include "assimilator_fetch_machine.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        enum {
            EvReconnectToController = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        const ui32 GroupId;
        const NKikimrBlobDepot::TBlobDepotConfig Config;
        TActorId BlobDepotId;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;

    public:
        TGroupAssimilator(ui32 groupId, const NKikimrBlobDepot::TBlobDepotConfig& config)
            : GroupId(groupId)
            , Config(config)
        {
            Y_VERIFY(Config.GetOperationMode() == NKikimrBlobDepot::EOperationMode::VirtualGroup);
        }

        void Bootstrap(TActorId parentId) {
            BlobDepotId = parentId;
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT31, "TGroupAssimilator::Bootstrap", (GroupId, GroupId));
            QueryGroupConfiguration();
        }

        void PassAway() override {
            FetchMachine->OnPassAway();
            TActorBootstrapped::PassAway();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // BSC interaction

        TActorId ControllerPipeId;

        void QueryGroupConfiguration() {
            TGroupID groupId(GroupId);
            if (groupId.ConfigurationType() != EGroupConfigurationType::Dynamic) {
                AbortWithError("group configuration type is not dynamic");
            }

            const ui64 controllerId = MakeBSControllerID(groupId.AvailabilityDomainID());
            ControllerPipeId = Register(NTabletPipe::CreateClient(SelfId(), controllerId));
            Become(&TThis::StateQueryController);
        }

        STRICT_STFUNC(StateQueryController,
            cFunc(TEvents::TSystem::Poison, PassAway);

            cFunc(EvReconnectToController, QueryGroupConfiguration);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            hFunc(TEvBlobStorage::TEvControllerNodeServiceSetUpdate, Handle);
        );

        void Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT00, "TGroupAssimilator::TEvClientConnected", (GroupId, GroupId));
            Y_VERIFY(ev->Get()->ClientId == ControllerPipeId);
            if (ev->Get()->Status == NKikimrProto::OK) {
                NTabletPipe::SendData(SelfId(), ControllerPipeId, new TEvBlobStorage::TEvControllerGetGroup(0, GroupId));
            } else {
                Reconnect();
            }
        }

        void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT32, "TGroupAssimilator::TEvClientDestroyed", (GroupId, GroupId));
            Y_VERIFY(ev->Get()->ClientId == ControllerPipeId);
            Reconnect();
        }

        void Handle(TEvBlobStorage::TEvControllerNodeServiceSetUpdate::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT33, "TGroupAssimilator::TEvControllerNodeServiceSetUpdate", (GroupId, GroupId),
                (Msg, ev->Get()->ToString()));
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeId);

            auto& record = ev->Get()->Record;
            if (record.HasStatus() && record.GetStatus() == NKikimrProto::OK && record.HasServiceSet()) {
                const auto& ss = record.GetServiceSet();
                for (const auto& group : ss.GetGroups()) {
                    if (group.GetGroupID() == GroupId) {
                        if (group.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                            return AbortWithError("the group being decommitted was destroyed");
                        } else if (!group.HasAssimilatorGroupId()) {
                            return AbortWithError("the group being decommitted is not in assimilation mode");
                        } else if (group.HasBlobDepotId()) {
                            const TString msg = "the group being decommitted is a virtual one";
                            Y_VERIFY_DEBUG(false, "%s", msg.data());
                            STLOG(PRI_CRIT, BLOB_DEPOT, BDT35, msg, (GroupId, GroupId));
                            return AbortWithError(msg);
                        } else {
                            Info = TBlobStorageGroupInfo::Parse(group, nullptr, nullptr);
                            StartAssimilation();
                            return;
                        }
                    }
                }
            }

            // retry operation in some time
            Reconnect();
        }

        void Reconnect() {
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeId);
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(EvReconnectToController, 0,
                SelfId(), {}, nullptr, 0));
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        std::unique_ptr<TGroupAssimilatorFetchMachine> FetchMachine;

        void StartAssimilation() {
            Become(&TThis::StateAssimilate);
            FetchMachine = std::make_unique<TGroupAssimilatorFetchMachine>(SelfId(), Info, BlobDepotId);
        }

        void StateAssimilate(STFUNC_SIG) {
            Y_UNUSED(ctx);

            switch (ev->GetTypeRewrite()) {
                cFunc(TEvents::TSystem::Poison, PassAway);
                IgnoreFunc(TEvTabletPipe::TEvClientDestroyed);

                default:
                    return FetchMachine->Handle(ev);
            }
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        void AbortWithError(TString error) {
            STLOG(PRI_ERROR, BLOB_DEPOT, BDT34, "failed to assimilate group", (GroupId, GroupId), (Error, error));
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, BlobDepotId, SelfId(), nullptr, GroupId));
            PassAway();
        }
    };

    void TBlobDepot::StartGroupAssimilators() {
        for (const ui32 groupId : Config.GetDecommittingGroups()) {
            if (Config.GetOperationMode() != NKikimrBlobDepot::EOperationMode::VirtualGroup) {
                STLOG(PRI_CRIT, BLOB_DEPOT, BDT36, "incorrect operating mode of BlobDepot", (TabletId, TabletID()));
                Y_VERIFY_DEBUG(false, "incorrect operating mode of BlobDepot");
                return;
            }

            StartGroupAssimilator(groupId);
        }
    }

    void TBlobDepot::StartGroupAssimilator(ui32 groupId) {
        if (RunningGroupAssimilators.contains(groupId)) {
            return;
        }

        RunningGroupAssimilators[groupId] = Register(new TGroupAssimilator(groupId, Config));
    }

    void TBlobDepot::HandleGone(TAutoPtr<IEventHandle> ev) {
        if (const auto it = RunningGroupAssimilators.find(ev->Cookie); it != RunningGroupAssimilators.end()) {
            Y_VERIFY(it->second == ev->Sender);
            RunningGroupAssimilators.erase(it);
        } else {
            Y_FAIL("unexpected event");
        }
    }

    void TBlobDepot::Handle(TEvAssimilatedData::TPtr /*ev*/) {
    }

} // NKikimr::NBlobDepot
