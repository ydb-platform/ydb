#include "blocks.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBlocksManager::TTxUpdateBlock : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        const ui64 TabletId;
        const ui32 BlockedGeneration;
        const ui32 NodeId;
        const ui64 IssuerGuid;
        const TInstant Timestamp;
        std::unique_ptr<IEventHandle> Response;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_UPDATE_BLOCK; }

        TTxUpdateBlock(TBlobDepot *self, ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, ui64 issuerGuid,
                TInstant timestamp, std::unique_ptr<IEventHandle> response)
            : TTransactionBase(self)
            , TabletId(tabletId)
            , BlockedGeneration(blockedGeneration)
            , NodeId(nodeId)
            , IssuerGuid(issuerGuid)
            , Timestamp(timestamp)
            , Response(std::move(response))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            auto& block = Self->BlocksManager->Blocks[TabletId];
            if (BlockedGeneration <= block.BlockedGeneration) {
                Response->Get<TEvBlobDepot::TEvBlockResult>()->Record.SetStatus(NKikimrProto::ALREADY);
            } else {
                // update block value in memory
                auto& block = Self->BlocksManager->Blocks[TabletId];
                block.BlockedGeneration = BlockedGeneration;
                block.IssuerGuid = IssuerGuid;

                // and persist it
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Blocks>().Key(TabletId).Update(
                    NIceDb::TUpdate<Schema::Blocks::BlockedGeneration>(BlockedGeneration),
                    NIceDb::TUpdate<Schema::Blocks::IssuerGuid>(IssuerGuid),
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
                Self->BlocksManager->OnBlockCommitted(TabletId, BlockedGeneration, NodeId, IssuerGuid, std::move(Response));
            }
        }
    };

    class TBlobDepot::TBlocksManager::TBlockProcessorActor : public TActorBootstrapped<TBlockProcessorActor> {
        struct TEvPrivate {
            enum {
                EvCheckWaitingNode = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

        TBlobDepot* const Self;
        const ui64 TabletId;
        const ui32 BlockedGeneration;
        const ui32 NodeId;
        const ui64 IssuerGuid;
        std::unique_ptr<IEventHandle> Response;
        ui32 BlocksPending = 0;
        ui32 RetryCount = 0;
        THashSet<ui32> NodesWaitingForPushResult;
        std::weak_ptr<TToken> Token;

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BLOB_DEPOT_BLOCKS_PROCESSOR_ACTOR;
        }

        TBlockProcessorActor(TBlobDepot *self, ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, ui64 issuerGuid,
                std::unique_ptr<IEventHandle> response)
            : Self(self)
            , TabletId(tabletId)
            , BlockedGeneration(blockedGeneration)
            , NodeId(nodeId)
            , IssuerGuid(issuerGuid)
            , Response(std::move(response))
            , Token(Self->Token)
        {}

        bool CheckIfObsolete() {
            if (Token.expired()) {
                return true; // tablet is dead
            }
            auto& block = Self->BlocksManager->Blocks[TabletId];
            if (block.BlockedGeneration == BlockedGeneration && block.IssuerGuid == IssuerGuid) {
                return false;
            } else {
                auto& r = Response->Get<TEvBlobDepot::TEvBlockResult>()->Record;
                r.SetStatus(NKikimrProto::ALREADY);
                Finish();
                return true;
            }
        }

        void Bootstrap() {
            if (CheckIfObsolete()) {
                return;
            }
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
                if (now < info.ExpirationTimestamp) { // includes case when agent is connected right now
                    TAgent& agent = Self->GetAgent(agentId);

                    // enqueue push notification
                    const auto [it, inserted] = agent.BlockToDeliver.try_emplace(TabletId, BlockedGeneration, IssuerGuid, SelfId());
                    if (!inserted) {
                        const auto& [currentBlockedGeneration, _1, _2] = it->second;
                        Y_ABORT_UNLESS(currentBlockedGeneration < BlockedGeneration);
                        it->second = {BlockedGeneration, IssuerGuid, SelfId()};
                    }

                    // add node to wait list; also start timer to remove this node from the wait queue
                    NodesWaitingForPushResult.insert(agentId);
                    TActivationContext::Schedule(info.ExpirationTimestamp, new IEventHandle(TEvPrivate::EvCheckWaitingNode,
                        0, SelfId(), {}, nullptr, agentId));

                    // issue message to the connected node, if it is
                    if (const auto& connection = agent.Connection) {
                        auto ev = std::make_unique<TEvBlobDepot::TEvPushNotify>();
                        auto *item = ev->Record.AddBlockedTablets();
                        item->SetTabletId(TabletId);
                        item->SetBlockedGeneration(BlockedGeneration);
                        item->SetIssuerGuid(IssuerGuid);

                        const ui64 id = ++agent.LastRequestId;
                        agent.PushCallbacks.emplace(id, [selfId = SelfId()](TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
                            TActivationContext::Send(ev->Forward(selfId));
                        });
                        TActivationContext::Send(new IEventHandle(connection->AgentId, connection->PipeServerId, ev.release(), 0, id));
                    }
                }
            }
        }

        void HandleCheckWaitingNode(TAutoPtr<IEventHandle> ev) {
            const ui32 agentId = ev->Cookie;
            if (NodesWaitingForPushResult.contains(agentId)) {
                const TMonotonic now = TActivationContext::Monotonic();
                const auto& info = Self->BlocksManager->Blocks[TabletId].PerAgentInfo[agentId];
                if (now < info.ExpirationTimestamp) { // node still can write data for this tablet, reschedule timer
                    TActivationContext::Schedule(info.ExpirationTimestamp, new IEventHandle(TEvPrivate::EvCheckWaitingNode,
                        0, SelfId(), {}, nullptr, agentId));
                } else {
                    NodesWaitingForPushResult.erase(agentId);
                    if (NodesWaitingForPushResult.empty()) {
                        Finish();
                    }
                }
            }
        }

        void Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
            const ui32 agentId = ev->Sender.NodeId();
            if (NodesWaitingForPushResult.erase(agentId)) {
                // mark lease as successfully revoked one
                auto& block = Self->BlocksManager->Blocks[TabletId];
                block.PerAgentInfo.erase(agentId);

                TAgent& agent = Self->GetAgent(agentId);
                const auto it = agent.BlockToDeliver.find(TabletId);
                Y_ABORT_UNLESS(it != agent.BlockToDeliver.end() && it->second == std::make_tuple(BlockedGeneration, IssuerGuid, SelfId()));
                agent.BlockToDeliver.erase(it);

                if (NodesWaitingForPushResult.empty()) {
                    Finish();
                }
            }
        }

        void IssueBlocksToStorage() {
            THashSet<ui32> processedGroups;
            for (const auto& [_, kind] : Self->ChannelKinds) {
                for (const auto& [channel, groupId] : kind.ChannelGroups) {
                    // FIXME: consider previous group generations (because agent can write in obsolete tablet generation)
                    // !!!!!!!!!!!
                    if (const auto [it, inserted] = processedGroups.insert(groupId); inserted) {
                        SendBlock(groupId);
                        ++BlocksPending;
                        RetryCount += 2;
                    }
                }
            }
        }

        void SendBlock(ui32 groupId) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT08, "issing TEvBlock", (Id, Self->GetLogId()), (BlockedTabletId,
                TabletId), (BlockedGeneration, BlockedGeneration), (GroupId, groupId), (IssuerGuid, IssuerGuid));
            SendToBSProxy(SelfId(), groupId, new TEvBlobStorage::TEvBlock(TabletId, BlockedGeneration, TInstant::Max(),
                IssuerGuid), groupId);
        }

        void Handle(TEvBlobStorage::TEvBlockResult::TPtr ev) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT09, "TEvBlockResult", (Id, Self->GetLogId()), (Msg, ev->Get()->ToString()),
                (BlockedTabletId, TabletId), (BlockedGeneration, BlockedGeneration), (GroupId, ev->Cookie));
            switch (ev->Get()->Status) {
                case NKikimrProto::OK:
                    if (!--BlocksPending) {
                        Finish();
                    }
                    break;

                case NKikimrProto::ALREADY: {
                    auto& r = Response->Get<TEvBlobDepot::TEvBlockResult>()->Record;
                    r.SetStatus(NKikimrProto::ALREADY);
                    Finish();
                    break;
                }

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

        STATEFN(StateFunc) {
            if (CheckIfObsolete()) {
                return;
            }
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvBlockResult, Handle);
                hFunc(TEvBlobDepot::TEvPushNotifyResult, Handle);
                fFunc(TEvPrivate::EvCheckWaitingNode, HandleCheckWaitingNode);
                cFunc(TEvents::TSystem::Wakeup, IssueBlocksToStorage);
                cFunc(TEvents::TSystem::Poison, PassAway);
            }
        }
    };

    void TBlobDepot::TBlocksManager::AddBlockOnLoad(ui64 tabletId, ui32 blockedGeneration, ui64 issuerGuid) {
        Blocks[tabletId] = {
            .BlockedGeneration = blockedGeneration,
            .IssuerGuid = issuerGuid,
        };
    }

    void TBlobDepot::TBlocksManager::AddBlockOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlock& block,
            NTabletFlatExecutor::TTransactionContext& txc) {
        AddBlockOnLoad(block.TabletId, block.BlockedGeneration, 0);

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Blocks>().Key(block.TabletId).Update(
            NIceDb::TUpdate<Schema::Blocks::BlockedGeneration>(block.BlockedGeneration),
            NIceDb::TUpdate<Schema::Blocks::IssuerGuid>(0)
        );

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT44, "adding block through decommission", (Id, Self->GetLogId()), (Block, block));
    }

    void TBlobDepot::TBlocksManager::OnBlockCommitted(ui64 tabletId, ui32 blockedGeneration, ui32 nodeId, ui64 issuerGuid,
            std::unique_ptr<IEventHandle> response) {
        Self->RegisterWithSameMailbox(new TBlockProcessorActor(Self, tabletId, blockedGeneration, nodeId, issuerGuid,
            std::move(response)));
    }

    void TBlobDepot::TBlocksManager::Handle(TEvBlobDepot::TEvBlock::TPtr ev) {
        const auto& record = ev->Get()->Record;
        auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev, NKikimrProto::OK,
            std::nullopt, BlockLeaseTime.MilliSeconds());

        if (!record.HasTabletId() || !record.HasBlockedGeneration()) {
            responseRecord->SetStatus(NKikimrProto::ERROR);
            responseRecord->SetErrorReason("incorrect protobuf");
        } else {
            const ui64 tabletId = record.GetTabletId();
            const ui32 blockedGeneration = record.GetBlockedGeneration();
            const ui64 issuerGuid = record.GetIssuerGuid();
            const auto it = Blocks.find(tabletId);
            if (it == Blocks.end() || it->second.CanSetNewBlock(blockedGeneration, issuerGuid)) {
                TAgent& agent = Self->GetAgent(ev->Recipient);
                Self->Execute(std::make_unique<TTxUpdateBlock>(Self, tabletId, blockedGeneration,
                    agent.Connection->NodeId, record.GetIssuerGuid(), TActivationContext::Now(), std::move(response)));
            } else {
                responseRecord->SetStatus(NKikimrProto::ALREADY);
            }
        }

        TActivationContext::Send(response.release()); // not sent if the request got processed and response now is nullptr
    }

    void TBlobDepot::TBlocksManager::Handle(TEvBlobDepot::TEvQueryBlocks::TPtr ev) {
        TAgent& agent = Self->GetAgent(ev->Recipient);
        const ui32 agentId = agent.Connection->NodeId;

        const TMonotonic now = TActivationContext::Monotonic();

        const auto& record = ev->Get()->Record;
        auto [response, responseRecord] = TEvBlobDepot::MakeResponseFor(*ev);
        responseRecord->SetTimeToLiveMs(BlockLeaseTime.MilliSeconds());

        for (const ui64 tabletId : record.GetTabletIds()) {
            auto& block = Blocks[tabletId];
            responseRecord->AddBlockedGenerations(block.BlockedGeneration);
            block.PerAgentInfo[agentId].ExpirationTimestamp = now + BlockLeaseTime;
        }

        TActivationContext::Send(response.release());
    }

    bool TBlobDepot::TBlocksManager::CheckBlock(ui64 tabletId, ui32 generation) const {
        const auto it = Blocks.find(tabletId);
        return it == Blocks.end() || it->second.BlockedGeneration < generation;
    }

} // NKikimr::NBlobDepot
