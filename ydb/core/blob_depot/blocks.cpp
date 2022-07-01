#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBlocksManager {
        TBlobDepot* const Self;
        THashMap<ui64, ui32> Blocks;

    private:
        class TTxUpdateBlock : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const ui64 TabletId;
            const ui32 BlockedGeneration;
            const ui32 NodeId;
            const TInstant Timestamp;
            std::unique_ptr<IEventHandle> Response;
            bool RaceDetected;

        public:
            TTxUpdateBlock(TBlobDepot *self, ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, TInstant timestamp,
                    std::unique_ptr<IEventHandle> response)
                : TTransactionBase(self)
                , TabletId(tabletId)
                , BlockedGeneration(blockedGeneration)
                , NodeId(nodeId)
                , Timestamp(timestamp)
                , Response(std::move(response))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                const auto [it, inserted] = Self->BlocksManager->Blocks.emplace(TabletId, BlockedGeneration);
                RaceDetected = !inserted && BlockedGeneration <= it->second;
                if (RaceDetected) {
                    Response->Get<TEvBlobDepot::TEvBlockResult>()->Record.SetStatus(NKikimrProto::ALREADY);
                } else {
                    NIceDb::TNiceDb db(txc.DB);
                    db.Table<Schema::Blocks>().Key(TabletId).Update(
                        NIceDb::TUpdate<Schema::Blocks::BlockedGeneration>(BlockedGeneration),
                        NIceDb::TUpdate<Schema::Blocks::IssuedByNode>(NodeId),
                        NIceDb::TUpdate<Schema::Blocks::IssueTimestamp>(Timestamp)
                    );
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                if (RaceDetected) {
                    TActivationContext::Send(Response.release());
                } else {
                    Self->BlocksManager->OnBlockCommitted(TabletId, BlockedGeneration, std::move(Response));
                }
            }
        };

        class TBlockProcessorActor : public TActorBootstrapped<TBlockProcessorActor> {
            TBlobDepot* const Self;
            const ui64 TabletId;
            const ui32 BlockedGeneration;
            std::unique_ptr<IEventHandle> Response;
            ui32 BlocksPending = 0;
            ui32 RetryCount = 0;
            const ui64 IssuerGuid = RandomNumber<ui64>() | 1;

        public:
            TBlockProcessorActor(TBlobDepot *self, ui64 tabletId, ui32 blockedGeneration,
                    std::unique_ptr<IEventHandle> response)
                : Self(self)
                , TabletId(tabletId)
                , BlockedGeneration(blockedGeneration)
                , Response(std::move(response))
            {}

            void Bootstrap() {
                TTabletStorageInfo *info = Self->Info();
                for (const auto& [_, kind] : Self->ChannelKinds) {
                    for (const ui8 channel : kind.IndexToChannel) {
                        const ui32 groupId = info->GroupFor(channel, Self->Executor()->Generation());
                        SendBlock(groupId);
                        ++BlocksPending;
                        RetryCount += 2;
                    }
                }

                Become(&TThis::StateFunc);
            }

            void SendBlock(ui32 groupId) {
                SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvBlock(TabletId, BlockedGeneration,
                    TInstant::Max(), IssuerGuid), groupId);
            }

            void Handle(TEvBlobStorage::TEvBlockResult::TPtr ev) {
                switch (ev->Get()->Status) {
                    case NKikimrProto::OK:
                        if (!--BlocksPending) {
                            TActivationContext::Send(Response.release());
                            PassAway();
                        }
                        break;

                    case NKikimrProto::ALREADY:
                        // race, but this is not possible in current implementation
                        Y_FAIL();

                    case NKikimrProto::ERROR:
                    default:
                        if (!--RetryCount) {
                            auto& r = Response->Get<TEvBlobDepot::TEvBlockResult>()->Record;
                            r.SetStatus(NKikimrProto::ERROR);
                            r.SetErrorReason(ev->Get()->ErrorReason);
                        } else {
                            SendBlock(ev->Cookie);
                        }
                        break;
                }
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvBlobStorage::TEvBlockResult, Handle);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };

    public:
        TBlocksManager(TBlobDepot *self)
            : Self(self)
        {}

        void AddBlockOnLoad(ui64 tabletId, ui32 generation) {
            Blocks.emplace(tabletId, generation);
        }

        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration,
                std::unique_ptr<IEventHandle> response) {
            Self->RegisterWithSameMailbox(new TBlockProcessorActor(Self, tabletId, blockedGeneration, std::move(response)));
        }

        void Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
            const auto& record = ev->Get()->Record;
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev, Self->SelfId(), NKikimrProto::OK, std::nullopt);

            if (!record.HasTabletId() || !record.HasBlockedGeneration()) {
                responseRecord->SetStatus(NKikimrProto::ERROR);
                responseRecord->SetErrorReason("incorrect protobuf");
            } else {
                const ui64 tabletId = record.GetTabletId();
                const ui32 blockedGeneration = record.GetBlockedGeneration();
                if (const auto it = Blocks.find(tabletId); it != Blocks.end() && blockedGeneration <= it->second) {
                    responseRecord->SetStatus(NKikimrProto::ERROR);
                } else {
                    TAgentInfo& agent = Self->GetAgent(ev->Recipient);
                    Self->Execute(std::make_unique<TTxUpdateBlock>(Self, tabletId, blockedGeneration,
                        agent.ConnectedNodeId, TActivationContext::Now(), std::move(response)));
                }
            }

            TActivationContext::Send(response.release()); // not sent if the request got processed and response now is nullptr
        }

        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev) {
            const auto& record = ev->Get()->Record;
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev, Self->SelfId());
            responseRecord->SetTimeToLiveMs(15000); // FIXME

            for (const ui64 tabletId : record.GetTabletIds()) {
                const auto it = Blocks.find(tabletId);
                responseRecord->AddBlockedGenerations(it != Blocks.end() ? it->second : 0);
            }

            TActivationContext::Send(response.release());
        }
    };

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlocksManager wrapper

    TBlobDepot::TBlocksManagerPtr TBlobDepot::CreateBlocksManager() {
        return {new TBlocksManager{this}, std::default_delete<TBlocksManager>{}};
    }

    void TBlobDepot::AddBlockOnLoad(ui64 tabletId, ui32 generation) {
        BlocksManager->AddBlockOnLoad(tabletId, generation);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
        return BlocksManager->Handle(ev);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev) {
        return BlocksManager->Handle(ev);
    }

} // NKikimr::NBlobDepot
