#include "blob_depot_tablet.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBlocksManager {
        // wait duration before issuing blocks via storage
        static constexpr TDuration AgentsWaitTime = TDuration::Seconds(1);

        // TTL for block lease
        static constexpr TDuration BlockLeaseTime = TDuration::Seconds(60);

        struct TBlock {
            struct TPerAgentInfo {
                TMonotonic ExpirationTimestamp = TMonotonic::Zero();
            };

            ui32 BlockedGeneration = 0;
            THashMap<ui32, TPerAgentInfo> PerAgentInfo;
        };

        TBlobDepot* const Self;
        THashMap<ui64, TBlock> Blocks;

    private:
        class TTxUpdateBlock : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const ui64 TabletId;
            const ui32 BlockedGeneration;
            const ui32 NodeId;
            const TInstant Timestamp;
            std::unique_ptr<IEventHandle> Response;

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
                auto& block = Self->BlocksManager->Blocks[TabletId];
                if (BlockedGeneration <= block.BlockedGeneration) {
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
                if (Response->Get<TEvBlobDepot::TEvBlockResult>()->Record.GetStatus() != NKikimrProto::OK) {
                    TActivationContext::Send(Response.release());
                } else {
                    // update block value in memory
                    auto& block = Self->BlocksManager->Blocks[TabletId];
                    Y_VERIFY(block.BlockedGeneration < BlockedGeneration);
                    block.BlockedGeneration = BlockedGeneration;

                    Self->BlocksManager->OnBlockCommitted(TabletId, BlockedGeneration, NodeId, std::move(Response));
                }
            }
        };

        class TBlockProcessorActor : public TActorBootstrapped<TBlockProcessorActor> {
            TBlobDepot* const Self;
            const ui64 TabletId;
            const ui32 BlockedGeneration;
            const ui32 NodeId;
            std::unique_ptr<IEventHandle> Response;
            ui32 BlocksPending = 0;
            ui32 RetryCount = 0;
            const ui64 IssuerGuid = RandomNumber<ui64>() | 1;
            THashSet<ui32> NodesWaitingForPushResult;

        public:
            TBlockProcessorActor(TBlobDepot *self, ui64 tabletId, ui32 blockedGeneration, ui32 nodeId,
                    std::unique_ptr<IEventHandle> response)
                : Self(self)
                , TabletId(tabletId)
                , BlockedGeneration(blockedGeneration)
                , NodeId(nodeId)
                , Response(std::move(response))
            {}

            void Bootstrap() {
                IssueNotificationsToAgents();
                Become(&TThis::StateFunc, AgentsWaitTime, new TEvents::TEvWakeup);
            }

            void IssueNotificationsToAgents() {
                const TMonotonic now = TActivationContext::Monotonic();
                auto& block = Self->BlocksManager->Blocks[TabletId];
                for (const auto& [agentId, info] : block.PerAgentInfo) {
                    if (agentId == NodeId) {
                        // skip the origin agent
                        continue;
                    }
                    if (info.ExpirationTimestamp <= now) {
                        SendPushToAgent(agentId);
                    }
                }
            }

            void SendPushToAgent(ui32 agentId) {
                auto ev = std::make_unique<TEvBlobDepot::TEvPushNotify>();
                auto *item = ev->Record.AddBlockedTablets();
                item->SetTabletId(TabletId);
                item->SetBlockedGeneration(BlockedGeneration);

                TAgentInfo& agent = Self->GetAgent(agentId);
                if (const auto& actorId = agent.ConnectedAgent) {
                    Send(*actorId, ev.release(), IEventHandle::FlagTrackDelivery, IssuerGuid);
                }
                NodesWaitingForPushResult.insert(agentId);
            }

            void Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
                const ui32 agentId = ev->Sender.NodeId();
                const size_t numErased = NodesWaitingForPushResult.erase(agentId);
                Y_VERIFY(numErased == 1 && ev->Cookie == IssuerGuid);

                // mark lease as successfully revoked one
                auto& block = Self->BlocksManager->Blocks[TabletId];
                block.PerAgentInfo.erase(agentId);

                if (NodesWaitingForPushResult.empty()) {
                    Finish();
                }
            }

            void Handle(TEvents::TEvUndelivered::TPtr /*ev*/) {
                // can't reach an agent to notify it about blocked generation change -- we can't do anything here
            }

            void IssueBlocksToStorage() {
                TTabletStorageInfo *info = Self->Info();
                for (const auto& [_, kind] : Self->ChannelKinds) {
                    for (const ui8 channel : kind.IndexToChannel) {
                        const ui32 groupId = info->GroupFor(channel, Self->Executor()->Generation());
                        SendBlock(groupId);
                        ++BlocksPending;
                        RetryCount += 2;
                    }
                }
            }

            void SendBlock(ui32 groupId) {
                SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvBlock(TabletId, BlockedGeneration,
                    TInstant::Max(), IssuerGuid), groupId);
            }

            void Handle(TEvBlobStorage::TEvBlockResult::TPtr ev) {
                switch (ev->Get()->Status) {
                    case NKikimrProto::OK:
                        if (!--BlocksPending) {
                            Finish();
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
                            Finish();
                        } else {
                            SendBlock(ev->Cookie);
                        }
                        break;
                }
            }

            void Finish() {
                TActivationContext::Send(Response.release());
                PassAway();
            }

            STRICT_STFUNC(StateFunc,
                hFunc(TEvBlobStorage::TEvBlockResult, Handle);
                hFunc(TEvBlobDepot::TEvPushNotifyResult, Handle);
                hFunc(TEvents::TEvUndelivered, Handle);
                cFunc(TEvents::TSystem::Wakeup, IssueBlocksToStorage);
                cFunc(TEvents::TSystem::Poison, PassAway);
            )
        };

    public:
        TBlocksManager(TBlobDepot *self)
            : Self(self)
        {}

        void AddBlockOnLoad(ui64 tabletId, ui32 generation) {
            Blocks[tabletId].BlockedGeneration = generation;
        }

        void OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, std::unique_ptr<IEventHandle> response) {
            Self->RegisterWithSameMailbox(new TBlockProcessorActor(Self, tabletId, blockedGeneration, nodeId,
                std::move(response)));
        }

        void Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
            const auto& record = ev->Get()->Record;
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev, Self->SelfId(), NKikimrProto::OK,
                std::nullopt, BlockLeaseTime.MilliSeconds());

            if (!record.HasTabletId() || !record.HasBlockedGeneration()) {
                responseRecord->SetStatus(NKikimrProto::ERROR);
                responseRecord->SetErrorReason("incorrect protobuf");
            } else {
                const ui64 tabletId = record.GetTabletId();
                const ui32 blockedGeneration = record.GetBlockedGeneration();
                if (const auto it = Blocks.find(tabletId); it != Blocks.end() && blockedGeneration <= it->second.BlockedGeneration) {
                    responseRecord->SetStatus(NKikimrProto::ALREADY);
                } else {
                    TAgentInfo& agent = Self->GetAgent(ev->Recipient);
                    Self->Execute(std::make_unique<TTxUpdateBlock>(Self, tabletId, blockedGeneration,
                        agent.ConnectedNodeId, TActivationContext::Now(), std::move(response)));
                }
            }

            TActivationContext::Send(response.release()); // not sent if the request got processed and response now is nullptr
        }

        void Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev) {
            TAgentInfo& agent = Self->GetAgent(ev->Recipient);
            const ui32 agentId = agent.ConnectedNodeId;
            Y_VERIFY(agentId);

            const TMonotonic now = TActivationContext::Monotonic();

            const auto& record = ev->Get()->Record;
            auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev, Self->SelfId());
            responseRecord->SetTimeToLiveMs(BlockLeaseTime.MilliSeconds());

            for (const ui64 tabletId : record.GetTabletIds()) {
                auto& block = Blocks[tabletId];
                responseRecord->AddBlockedGenerations(block.BlockedGeneration);
                block.PerAgentInfo[agentId].ExpirationTimestamp = now + BlockLeaseTime;
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
