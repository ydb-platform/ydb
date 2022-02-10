#include "pipe_tracker.h"

namespace NKikimr {

std::unordered_set<ui64> TPipeTrackerBase::EmptySet;
std::unordered_set<std::pair<ui64, ui64>> TPipeTrackerBase::EmptyPairSet;

void TPipeTrackerBase::AttachTablet(ui64 txid, ui64 tabletid, ui64 cookie) {
    auto txIt = TxToTablet.find(txid);
    if (txIt == TxToTablet.end()) {
        txIt = TxToTablet.emplace(txid, std::unordered_set<std::pair<ui64, ui64>>()).first;
    }

    auto& tabletSet = txIt->second;
    auto tabIt = tabletSet.find(std::make_pair(cookie, tabletid)); 
    if (tabIt != tabletSet.end())
        return;

    tabletSet.insert(std::make_pair(cookie, tabletid)); 
    TabletToTx[tabletid].insert(txid);
    TxTablets[txid].insert(tabletid);
}

bool TPipeTrackerBase::DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie) {
    auto txIt = TxToTablet.find(txid);
    if (txIt == TxToTablet.end())
        return false;

    auto& tabletSet = txIt->second;
    auto tabIt = tabletSet.find(std::make_pair(cookie, tabletid)); 
    if (tabIt == tabletSet.end())
        return false;

    tabletSet.erase(tabIt);
    auto multiIt = TxTablets.find(txid);
    Y_VERIFY(multiIt != TxTablets.end());
    auto& tablets = multiIt->second;
    auto currIt = tablets.find(tabletid);
    Y_VERIFY(currIt != tablets.end());
    auto nextIt = currIt;
    ++nextIt;
    tablets.erase(currIt);
    if (nextIt == tablets.end() || *nextIt != tabletid) {
        if (tabletSet.empty()) {
            TxToTablet.erase(txIt);
            Y_VERIFY(tablets.empty());
            TxTablets.erase(multiIt);
        }

        auto it = TabletToTx.find(tabletid);
        Y_VERIFY(it != TabletToTx.end());
        it->second.erase(txid);
        if (it->second.empty()) {
            TabletToTx.erase(it);
            return true;
        }
    }

    return false;
}

bool TPipeTrackerBase::IsTxAlive(ui64 txid) const {
    auto txIt = TxToTablet.find(txid);
    return (txIt != TxToTablet.end());
}

const std::unordered_set<ui64> &TPipeTrackerBase::FindTx(ui64 tabletid) const {
    auto it = TabletToTx.find(tabletid);
    if (it == TabletToTx.end())
        return EmptySet;

    return it->second;
}

const std::unordered_set<std::pair<ui64, ui64> > &TPipeTrackerBase::FindTablets(ui64 txid) const {
    auto it = TxToTablet.find(txid);
    if (it == TxToTablet.end())
        return EmptyPairSet;

    return it->second;
}

TPipeTracker::TPipeTracker(NTabletPipe::IClientCache& clientCache)
    : ClientCache(clientCache)
{
}

void TPipeTracker::DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie, const TActorContext& ctx) {
    if (TPipeTrackerBase::DetachTablet(txid, tabletid, cookie)) {
       ClientCache.Shutdown(ctx, tabletid);
    }
}

void TPendingPipeTrackerCommands::AttachTablet(ui64 txid, ui64 tabletid, ui64 cookie) {
    Attach.push_back(TCommand(txid, tabletid, cookie));
}

void TPendingPipeTrackerCommands::DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie) {
    Detach.push_back(TCommand(txid, tabletid, cookie));
}

void TPendingPipeTrackerCommands::Clear() {
    Attach.clear();
    Detach.clear();
}

void TPendingPipeTrackerCommands::Apply(TPipeTracker& tracker, const TActorContext& ctx) {
    for (auto c : Attach) {
        tracker.AttachTablet(c.TxId, c.TabletId, c.Cookie);
    }

    for (auto c : Detach) {
        tracker.DetachTablet(c.TxId, c.TabletId, c.Cookie, ctx);
    }

    Clear();
}


}
