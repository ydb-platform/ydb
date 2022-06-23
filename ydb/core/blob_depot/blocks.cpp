#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBlocksManager::TImpl {
        TBlobDepot *Self;
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
                TImpl& impl = *Self->BlocksManager.Impl;
                const auto [it, inserted] = impl.Blocks.emplace(TabletId, BlockedGeneration);
                RaceDetected = !inserted && BlockedGeneration <= it->second;
                if (RaceDetected) {
                    Response->Get<TEvBlobDepot::TEvBlockResult>()->Record.SetStatus(NKikimrProto::RACE);
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
                    Self->BlocksManager.Impl->OnBlockCommitted(TabletId, BlockedGeneration, std::move(Response));
                }
            }
        };

    public:
        TImpl(TBlobDepot *self)
            : Self(self)
        {}

        void AddBlockOnLoad(ui64 tabletId, ui32 generation) {
            Blocks.emplace(tabletId, generation);
        }

        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, std::unique_ptr<IEventHandle> response) {
            (void)tabletId, (void)blockedGeneration, (void)response;
        }

        void OnAgentConnect(TAgentInfo& agent) {
            (void)agent;
        }

        void OnAgentDisconnect(TAgentInfo& agent) {
            (void)agent;
        }

        void Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
            const auto& record = ev->Get()->Record;
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(ev, Self->SelfId(), NKikimrProto::OK, std::nullopt);

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
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(ev, Self->SelfId());
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

    TBlobDepot::TBlocksManager::TBlocksManager(TBlobDepot *self)
        : Impl(std::make_unique<TImpl>(self))
    {}

    TBlobDepot::TBlocksManager::~TBlocksManager()
    {}

    void TBlobDepot::TBlocksManager::AddBlockOnLoad(ui64 tabletId, ui32 generation) {
        Impl->AddBlockOnLoad(tabletId, generation);
    }

    void TBlobDepot::TBlocksManager::OnAgentConnect(TAgentInfo& agent) {
        Impl->OnAgentConnect(agent);
    }

    void TBlobDepot::TBlocksManager::OnAgentDisconnect(TAgentInfo& agent) {
        Impl->OnAgentDisconnect(agent);
    }

    void TBlobDepot::TBlocksManager::Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
        return Impl->Handle(ev);
    }

    void TBlobDepot::TBlocksManager::Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev) {
        return Impl->Handle(ev);
    }

} // NKikimr::NBlobDepot
