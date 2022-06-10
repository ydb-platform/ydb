#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDAC01, "TEvClientConnected", (VirtualGroupId, VirtualGroupId),
            (Msg, ev->Get()->ToString()));
    }

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC02, "TEvClientDestroyed", (VirtualGroupId, VirtualGroupId),
            (Msg, ev->Get()->ToString()));
        PipeId = {};
        OnDisconnect();
        ConnectToBlobDepot();
    }

    void TBlobDepotAgent::ConnectToBlobDepot() {
        PipeId = Register(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        const ui64 id = NextRequestId++;
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvRegisterAgent(VirtualGroupId), id);
        RegisterRequest(id, nullptr, [this](IEventBase *ev) {
            if (ev) {
                auto& msg = *static_cast<TEvBlobDepot::TEvRegisterAgentResult*>(ev);
                STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC06, "TEvRegisterAgentResult", (VirtualGroupId, VirtualGroupId),
                    (Msg, msg.Record));
                Registered = true;
                BlobDepotGeneration = msg.Record.GetGeneration();
                auto& channelGroups = msg.Record.GetChannelGroups();
                BlobDepotChannelGroups = {channelGroups.begin(), channelGroups.end()};
            }
            return true;
        });
        IssueAllocateIdsIfNeeded();
    }

    void TBlobDepotAgent::IssueAllocateIdsIfNeeded() {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC09, "IssueAllocateIdsIfNeeded", (VirtualGroupId, VirtualGroupId),
            (IdAllocInFlight, IdAllocInFlight), (IdQ.size, IdQ.size()), (PreallocatedIdCount, PreallocatedIdCount),
            (PipeId, PipeId));
        if (!IdAllocInFlight && IdQ.size() < PreallocatedIdCount && PipeId) {
            const ui64 id = NextRequestId++;
            NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvAllocateIds, id);
            IdAllocInFlight = true;

            RegisterRequest(id, nullptr, [this](IEventBase *ev) {
                Y_VERIFY(IdAllocInFlight);
                IdAllocInFlight = false;

                if (ev) {
                    auto& msg = *static_cast<TEvBlobDepot::TEvAllocateIdsResult*>(ev);
                    STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC07, "TEvAllocateIdsResult", (VirtualGroupId, VirtualGroupId),
                        (Msg, msg.Record));
                    Y_VERIFY(msg.Record.GetGeneration() == BlobDepotGeneration);
                    IdQ.push_back(TAllocatedId{BlobDepotGeneration, msg.Record.GetRangeBegin(), msg.Record.GetRangeEnd()});

                    // FIXME notify waiting requests about new ids

                    // ask for more ids if needed
                    IssueAllocateIdsIfNeeded();
                }

                return true; // request complete, remove from queue
            });
        }
    }

    void TBlobDepotAgent::OnDisconnect() {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC04, "OnDisconnect", (VirtualGroupId, VirtualGroupId));

        for (auto& [id, item] : std::exchange(RequestInFlight, {})) {
            if (item.Sender) {
                item.Sender->OnRequestComplete(id);
            }
            const bool done = item.Callback(nullptr);
            Y_VERIFY(done);
        }

        Registered = false;
    }

    void TBlobDepotAgent::RegisterRequest(ui64 id, TRequestSender *sender, TRequestCompleteCallback callback) {
        const auto [_, inserted] = RequestInFlight.emplace(id, TRequestInFlight{sender, std::move(callback)});
        Y_VERIFY(inserted);
        if (sender) {
            sender->RegisterRequest(id);
        }
    }

    template<typename TEvent>
    void TBlobDepotAgent::HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDAC05, "HandleTabletResponse", (VirtualGroupId, VirtualGroupId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()));
        if (const auto it = RequestInFlight.find(ev->Cookie); it != RequestInFlight.end()) {
            auto& [id, item] = *it;
            if (item.Sender) {
                item.Sender->OnRequestComplete(id);
            }
            if (item.Callback(ev->Get())) {
                RequestInFlight.erase(it);
            }
        } else {
            Y_FAIL(); // don't know how this can happen without logic error
        }
    }

    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvRegisterAgentResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvAllocateIdsResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvBlockResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvQueryBlocksResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvCommitBlobSeqResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvResolveResult::TPtr ev);

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestCompleteCallback callback) {
        auto ev = std::make_unique<TEvBlobDepot::TEvBlock>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(callback));
    }

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestCompleteCallback callback) {
        auto ev = std::make_unique<TEvBlobDepot::TEvResolve>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(callback));
    }

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestCompleteCallback callback) {
        auto ev = std::make_unique<TEvBlobDepot::TEvQueryBlocks>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(callback));
    }

    void TBlobDepotAgent::Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestCompleteCallback callback) {
        const ui64 id = NextRequestId++;
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDAC03, "Issue", (VirtualGroupId, VirtualGroupId), (Id, id), (Msg, ev->ToString()));
        NTabletPipe::SendData(SelfId(), PipeId, ev.release(), id);
        RegisterRequest(id, sender, std::move(callback));
    }

    NKikimrProto::EReplyStatus TBlobDepotAgent::CheckBlockForTablet(ui64 tabletId, ui32 generation, TExecutingQuery *query) {
        auto& block = Blocks[tabletId];
        const TMonotonic issueTime = TActivationContext::Monotonic();
        if (generation <= block.BlockedGeneration) {
            return NKikimrProto::RACE;
        } else if (issueTime< block.ExpirationTimestamp) {
            return NKikimrProto::OK;
        } else if (!block.RefreshInFlight) {
            NKikimrBlobDepot::TEvQueryBlocks queryBlocks;
            queryBlocks.AddTabletIds(tabletId);
            Issue(std::move(queryBlocks), nullptr, [=](IEventBase *ev) {
                if (ev) {
                    auto& msg = *static_cast<TEvBlobDepot::TEvQueryBlocksResult*>(ev);
                    const ui64 tabletId_ = tabletId;
                    STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDAC08, "TEvQueryBlocksResult", (VirtualGroupId, VirtualGroupId),
                        (Msg, msg.Record), (TabletId, tabletId_));
                    Y_VERIFY(msg.Record.BlockedGenerationsSize() == 1);
                    auto& block = Blocks[tabletId];
                    Y_VERIFY(block.BlockedGeneration < generation);
                    block.BlockedGeneration = msg.Record.GetBlockedGenerations(1);
                    block.ExpirationTimestamp = issueTime + TDuration::MilliSeconds(msg.Record.GetTimeToLiveMs());
                    for (auto& query : block.PendingBlockChecks) {
                        query.OnUpdateBlock();
                    }
                }
                return true;
            });
            block.RefreshInFlight = true;
            block.PendingBlockChecks.PushBack(query);
        }
        return NKikimrProto::NOTREADY;
    }

} // NKikimr::NBlobDepot
