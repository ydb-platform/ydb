#include "blob_depot_tablet.h"
#include "assimilator_fetch_machine.h"
#include "schema.h"
#include "blocks.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TGroupAssimilator : public TActorBootstrapped<TGroupAssimilator> {
        enum {
            EvReconnectToController = EventSpaceBegin(TEvents::ES_PRIVATE),
        };

        const ui32 GroupId;
        const NKikimrBlobDepot::TBlobDepotConfig Config;
        const ui64 TabletId;
        TActorId BlobDepotId;
        TIntrusivePtr<TBlobStorageGroupInfo> Info;

    public:
        TGroupAssimilator(const NKikimrBlobDepot::TBlobDepotConfig& config, ui64 tabletId)
            : GroupId(config.GetDecommitGroupId())
            , Config(config)
            , TabletId(tabletId)
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
                (Msg, ev->Get()->Record));
            NTabletPipe::CloseAndForgetClient(SelfId(), ControllerPipeId);

            auto& record = ev->Get()->Record;
            if (record.HasStatus() && record.GetStatus() == NKikimrProto::OK && record.HasServiceSet()) {
                const auto& ss = record.GetServiceSet();
                for (const auto& group : ss.GetGroups()) {
                    if (group.GetGroupID() == GroupId) {
                        if (group.GetEntityStatus() == NKikimrBlobStorage::EEntityStatus::DESTROY) {
                            return AbortWithError("the group being decommitted was destroyed");
                        } else if (!group.HasBlobDepotId() || group.GetBlobDepotId() != TabletId) {
                            return AbortWithError("inconsistent decommission state");
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
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Gone, 0, BlobDepotId, SelfId(), nullptr, 0));
            PassAway();
        }
    };

    void TBlobDepot::StartGroupAssimilator() {
        if (!RunningGroupAssimilator && Config.HasDecommitGroupId()) {
            RunningGroupAssimilator = Register(new TGroupAssimilator(Config, TabletID()));
        }
    }

    void TBlobDepot::HandleGone(TAutoPtr<IEventHandle> ev) {
        if (ev->Sender == RunningGroupAssimilator) {
            RunningGroupAssimilator = {};
        } else {
            Y_FAIL("unexpected event");
        }
    }

    void TBlobDepot::Handle(TEvAssimilatedData::TPtr ev) {
        class TTxPutAssimilatedData : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::unique_ptr<TEvAssimilatedData> Ev;

        public:
            TTxPutAssimilatedData(TBlobDepot *self, TEvAssimilatedData::TPtr ev)
                : TTransactionBase(self)
                , Ev(ev->Release().Release())
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);

                for (const auto& block : Ev->Blocks) {
                    Self->BlocksManager->AddBlockOnLoad(block.TabletId, block.BlockedGeneration, 0);
                    db.Table<Schema::Blocks>().Key(block.TabletId).Update(
                        NIceDb::TUpdate<Schema::Blocks::BlockedGeneration>(block.BlockedGeneration),
                        NIceDb::TUpdate<Schema::Blocks::IssuerGuid>(0)
                    );
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                if (!std::exchange(Self->DecommitBlocksFinished, Ev->BlocksFinished)) {
                    STLOG(PRI_INFO, BLOB_DEPOT, BDTxx, "blocks assimilation complete", (TabletId, Self->TabletID()),
                        (DecommitGroupId, Self->Config.GetDecommitGroupId()));
                    Self->ProcessRegisterAgentQ();
                }
            }
        };

        Execute(std::make_unique<TTxPutAssimilatedData>(this, ev));
    }

} // NKikimr::NBlobDepot
