#include "pipe_tracker.h"

namespace NKikimr {

std::unordered_set<ui64> TPipeTrackerBase::EmptySet;
std::unordered_set<std::pair<ui64, ui64>> TPipeTrackerBase::EmptyPairSet;

void TPipeTrackerBase::AttachTablet(ui64 txid, ui64 tabletid, ui64 cookie) {
    auto& tabletSet = TxToTablet[txid];
    auto tabIt = tabletSet.find(std::make_pair(cookie, tabletid));
    if (tabIt != tabletSet.end())
        return;

    tabletSet.insert(std::make_pair(cookie, tabletid));
    TabletToTx[tabletid].insert(txid);
    TxTablets[txid][tabletid].insert(cookie);
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

    auto itTxTablets = TxTablets.find(txid);
    Y_ABORT_UNLESS(itTxTablets != TxTablets.end());
    auto itCookies = itTxTablets->second.find(tabletid);
    Y_ABORT_UNLESS(itCookies != itTxTablets->second.end());
    auto itCookie = itCookies->second.find(cookie);
    Y_ABORT_UNLESS(itCookie != itCookies->second.end());
    itCookies->second.erase(itCookie);

    // Cookies are empty when there are no more links between txid and tabletid
    if (itCookies->second.empty()) {
        itTxTablets->second.erase(itCookies);

        // Check if txid has no more tablets
        if (itTxTablets->second.empty()) {
            Y_ABORT_UNLESS(tabletSet.empty());
            TxTablets.erase(itTxTablets);
            TxToTablet.erase(txIt);
        }

        // Unlink txid from tabletid
        auto it = TabletToTx.find(tabletid);
        Y_ABORT_UNLESS(it != TabletToTx.end());
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

const std::unordered_set<ui64> &TPipeTrackerBase::FindCookies(ui64 txid, ui64 tabletid) const {
    auto it = TxTablets.find(txid);
    if (it == TxTablets.end())
        return EmptySet;

    auto itCookies = it->second.find(tabletid);
    if (itCookies == it->second.end())
        return EmptySet;

    return itCookies->second;
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
