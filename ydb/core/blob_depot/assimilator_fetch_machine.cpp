#include "assimilator_fetch_machine.h"

namespace NKikimr::NBlobDepot {

    using TFetchMachine = TBlobDepot::TGroupAssimilatorFetchMachine;

    TFetchMachine::TGroupAssimilatorFetchMachine(TActorIdentity self, TIntrusivePtr<TBlobStorageGroupInfo> info,
            TActorId blobDepotId)
        : Self(self)
        , Info(std::move(info))
        , BlobDepotId(blobDepotId)
    {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT38, "TGroupAssimilatorFetchMachine start", (GroupId, Info->GroupID));
        PerDiskState.resize(Info->GetTotalVDisksNum());
        VDisksInHeap.emplace(&Info->GetTopology());
        for (ui32 i = 0; i < PerDiskState.size(); ++i) {
            const TActorId actorId = Info->GetActorId(i);
            const ui32 nodeId = actorId.NodeId();
            TNodeInfo& node = Nodes[nodeId];
            node.OrderNumbers.push_back(i);
        }
        for (const auto& [nodeId, node] : Nodes) {
            if (nodeId != Self.NodeId()) {
                TActivationContext::Send(new IEventHandle(TEvInterconnect::EvConnectNode, 0,
                    TActivationContext::InterconnectProxy(nodeId), Self, nullptr, 0));
            } else {
                for (const ui32 orderNumber : node.OrderNumbers) {
                    IssueAssimilateCmdToVDisk(orderNumber);
                }
            }
        }
    }

    void TFetchMachine::Handle(TAutoPtr<IEventHandle>& ev) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvVAssimilateResult, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
            hFunc(TEvInterconnect::TEvNodeConnected, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);

            default:
                Y_VERIFY_DEBUG(false, "unexpected event Type# %08" PRIx32, type);
        }
    }

    void TFetchMachine::OnPassAway() {
        for (const auto& [nodeId, node] : Nodes) {
            if (nodeId != Self.NodeId()) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Unsubscribe, 0,
                   TActivationContext::InterconnectProxy(nodeId), Self, nullptr, 0));
            }
        }
    }

    void TFetchMachine::IssueAssimilateCmdToVDisk(ui32 orderNumber) {
        const TActorId actorId = Info->GetActorId(orderNumber);

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT39, "IssueAssimilateCmdToVDisk", (GroupId, Info->GroupID),
            (OrderNumber, orderNumber), (ActorId, actorId));

        TPerDiskState& state = PerDiskState[orderNumber];
        Y_VERIFY(!state.Finished);

        auto ev = std::make_unique<TEvBlobStorage::TEvVAssimilate>(Info->GetVDiskId(orderNumber));
        auto& record = ev->Record;
        if (state.LastBlock) {
            record.SetSkipBlocksUpTo(*state.LastBlock);
        }
        if (state.LastBarrier) {
            auto *x = record.MutableSkipBarriersUpTo();
            x->SetTabletId(std::get<0>(*state.LastBarrier));
            x->SetChannel(std::get<1>(*state.LastBarrier));
        }
        if (state.LastBlob) {
            LogoBlobIDFromLogoBlobID(*state.LastBlob, record.MutableSkipBlobsUpTo());
        }

        const ui64 id = ++LastRequestId;
        Self.Send(actorId, ev.release(), IEventHandle::FlagTrackDelivery, id);

        const auto [it, inserted] = RequestsInFlight.emplace(id, TRequestInFlight{orderNumber});
        Y_VERIFY(inserted);

        const ui32 nodeId = actorId.NodeId();
        Nodes[nodeId].RequestsInFlight.insert(&*it);
    }

    void TFetchMachine::Handle(TEvInterconnect::TEvNodeConnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT40, "NodeConnected", (GroupId, Info->GroupID), (NodeId, nodeId));
        TNodeInfo& node = Nodes[nodeId];
        for (const ui32 orderNumber : node.OrderNumbers) {
            auto& state = PerDiskState[orderNumber];
            if (!state.Finished) {
                IssueAssimilateCmdToVDisk(orderNumber);
            }
        }
    }

    void TFetchMachine::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr ev) {
        const ui32 nodeId = ev->Get()->NodeId;
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT41, "NodeDisconnected", (GroupId, Info->GroupID), (NodeId, nodeId));
        TNodeInfo& node = Nodes[nodeId];
        for (const auto *kv : std::exchange(node.RequestsInFlight, {})) {
            const size_t num = RequestsInFlight.erase(kv->first);
            Y_VERIFY(num == 1);
        }
    }

    void TFetchMachine::Handle(TEvents::TEvUndelivered::TPtr ev) {
        if (ev->Get()->SourceType == TEvBlobStorage::EvVAssimilate) {
            // TODO: undelivery may be caused by moving VDisk actor out, handle it
            EndRequest(ev->Cookie);
        }
    }

    ui32 TFetchMachine::EndRequest(ui64 id) {
        const auto it = RequestsInFlight.find(id);
        Y_VERIFY(it != RequestsInFlight.end());
        const ui32 orderNumber = it->second.OrderNumber;
        const TActorId actorId = Info->GetActorId(orderNumber);
        const ui32 nodeId = actorId.NodeId();
        TNodeInfo& node = Nodes[nodeId];
        const size_t num = node.RequestsInFlight.erase(&*it);
        Y_VERIFY(num == 1);
        RequestsInFlight.erase(it);
        return orderNumber;
    }

    void TFetchMachine::Handle(TEvBlobStorage::TEvVAssimilateResult::TPtr ev) {
        const ui32 orderNumber = EndRequest(ev->Cookie);
        const auto& record = ev->Get()->Record;

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT42, "EvVAssimilate", (GroupId, Info->GroupID), (Id, ev->Cookie),
            (OrderNumber, orderNumber), (Status, record.GetStatus()), (Blocks.size, record.BlocksSize()),
            (Barriers.size, record.BarriersSize()), (Blobs.size, record.BlobsSize()));

        TPerDiskState& state = PerDiskState[orderNumber];

        const bool wasExhausted = state.Exhausted();

        for (const auto& item : record.GetBlocks()) {
            const ui64 tabletId = item.GetTabletId();
            state.LastBlock.emplace(tabletId);
            if (CurrentBlock <= *state.LastBlock) {
                state.Blocks.emplace_back(item);
            }
        }
        for (const auto& item : record.GetBarriers()) {
            const ui64 tabletId = item.GetTabletId();
            const ui8 channel = item.GetChannel();
            state.LastBarrier.emplace(tabletId, channel);
            if (CurrentBarrier <= *state.LastBarrier) {
                state.Barriers.emplace_back(item);
            }
        }
        ui64 raw[3] = {0, 0, 0};
        for (const auto& item : record.GetBlobs()) {
            if (item.HasRawX1()) {
                raw[0] = item.GetRawX1();
            } else if (item.HasDiffX1()) {
                raw[0] += item.GetDiffX1();
            }
            if (item.HasRawX2()) {
                raw[1] = item.GetRawX2();
            } else if (item.HasDiffX2()) {
                raw[1] += item.GetDiffX2();
            }
            if (item.HasRawX3()) {
                raw[2] = item.GetRawX3();
            } else if (item.HasDiffX3()) {
                raw[2] += item.GetDiffX3();
            }
            const TLogoBlobID id(raw);
            state.LastBlob.emplace(id);
            if (CurrentBlob <= *state.LastBlob) {
                state.Blobs.emplace_back(item, id);
            }
        }

        if (wasExhausted && !state.Exhausted()) {
            Heap.push_back(&state);
            std::push_heap(Heap.begin(), Heap.end(), TPerDiskState::THeapCompare());
            *VDisksInHeap |= {&Info->GetTopology(), Info->GetVDiskId(orderNumber)};
        }

        if (record.BlocksSize() + record.BarriersSize() + record.BlobsSize() == 0 && record.GetStatus() == NKikimrProto::OK) {
            state.Finished = true;
        } else if (state.Exhausted()) { // still no records; for example, when all were skipped
            return IssueAssimilateCmdToVDisk(orderNumber);
        }

        Merge();
    }

    void TFetchMachine::Merge() {
        auto ev = std::make_unique<TEvAssimilatedData>(Info->GroupID);

        bool quorumCorrect = Info->GetQuorumChecker().CheckQuorumForGroup(*VDisksInHeap);
        while (quorumCorrect) {
            if (Heap.empty()) {
                CurrentBlob = Max<TLogoBlobID>();
                break;
            }

            std::optional<TBlock> block;
            std::optional<TBarrier> barrier;
            std::optional<TBlob> blob;

            auto callback = [&](auto&& value, ui32 orderNumber) {
                using T = std::decay_t<decltype(value)>;
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT37, "AssimilatedItem", (GroupId, Info->GroupID), (OrderNumber, orderNumber),
                    (Value, value));
                if constexpr (std::is_same_v<T, TBlock>) {
                    Y_VERIFY(CurrentBlock <= value.TabletId);
                    CurrentBlock = value.TabletId;
                    if (block) {
                        block->Merge(value);
                    } else {
                        block.emplace(std::move(value));
                    }
                } else if constexpr (std::is_same_v<T, TBarrier>) {
                    CurrentBlock = Max<ui64>();
                    Y_VERIFY(CurrentBarrier <= std::make_tuple(value.TabletId, value.Channel));
                    CurrentBarrier = {value.TabletId, value.Channel};
                    if (barrier) {
                        barrier->Merge(value);
                    } else {
                        barrier.emplace(std::move(value));
                    }
                } else if constexpr (std::is_same_v<T, TBlob>) {
                    CurrentBarrier = {Max<ui64>(), Max<ui8>()};
                    Y_VERIFY(CurrentBlob <= value.Id);
                    CurrentBlob = value.Id;
                    if (blob) {
                        blob->Merge(value);
                    } else {
                        blob.emplace(std::move(value));
                    }
                    // special handling for part layout here -- we can't just merge the blobs
                } else {
                    static_assert(TDependentFalse<T>, "incorrect case");
                }
            };

            TPerDiskState& head = *Heap.front();
            auto key = head.FirstKey();
            while (!Heap.empty() && Heap.front()->FirstKey() == key) {
                std::pop_heap(Heap.begin(), Heap.end(), TPerDiskState::THeapCompare());
                const ui32 orderNumber = Heap.back() - PerDiskState.data();
                TPerDiskState& item = PerDiskState[orderNumber];
                item.PopFirstItem(std::bind(callback, std::placeholders::_1, orderNumber));
                if (item.Exhausted()) {
                    if (!item.Finished) {
                        // data not yet received -- ask for it
                        IssueAssimilateCmdToVDisk(orderNumber);
                        // mark disk temporarily unavailable
                        *VDisksInHeap -= {&Info->GetTopology(), Info->GetVDiskId(orderNumber)};
                        quorumCorrect = Info->GetQuorumChecker().CheckQuorumForGroup(*VDisksInHeap);
                    }
                    // remove item from the heap -- it has no valid data to process
                    Heap.pop_back();
                } else {
                    // more items to do
                    std::push_heap(Heap.begin(), Heap.end(), TPerDiskState::THeapCompare());
                }
            }

            if (block) {
                ev->Blocks.push_back(std::move(*block));
            } else if (barrier) {
                ev->Barriers.push_back(std::move(*barrier));
            } else if (blob) {
                ev->Blobs.push_back(std::move(*blob));
            } else {
                Y_FAIL();
            }
        }

        ev->BlobsFinished = CurrentBlob == Max<TLogoBlobID>();
        ev->BarriersFinished = ev->BlobsFinished || CurrentBarrier == std::make_tuple(Max<ui64>(), Max<ui8>());
        ev->BlocksFinished = ev->BarriersFinished || CurrentBlock == Max<ui64>();
        Self.Send(BlobDepotId, ev.release());
    }

} // NKikimr::NBlobDepot
