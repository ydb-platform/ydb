#pragma once
#include "defs.h"
#include <ydb/core/tablet/tablet_pipe_client_cache.h>
#include <ydb/core/util/tuples.h>
#include <functional>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr {

class TPipeTrackerBase {
public:
    // pair of tabletid:cookie should be unique if tabletid should be processed several times
    void AttachTablet(ui64 txid, ui64 tabletid, ui64 cookie = 0);
    // returns false if tabletid is not used anymore
    bool DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie = 0);
    bool IsTxAlive(ui64 txid) const;
    const std::unordered_set<ui64>& FindTx(ui64 tabletid) const;
    const std::unordered_set<std::pair<ui64, ui64>>& FindTablets(ui64 txid) const;
    const std::unordered_set<ui64>& FindCookies(ui64 txid, ui64 tabletid) const;

private:
    std::unordered_map<ui64, std::unordered_set<ui64>> TabletToTx; // tabletid -> txid
    std::unordered_map<ui64, std::unordered_set<std::pair<ui64, ui64>>> TxToTablet; // txid -> cookie:tabletid
    std::unordered_map<ui64, std::unordered_map<ui64, std::unordered_set<ui64>>> TxTablets; // txid -> tabletid -> cookie
    static std::unordered_set<ui64> EmptySet;
    static std::unordered_set<std::pair<ui64, ui64>> EmptyPairSet;
};

class TPipeTracker : public TPipeTrackerBase {
public:
    TPipeTracker(NTabletPipe::IClientCache& clientCache);
    void DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie, const TActorContext& ctx);

private:
    NTabletPipe::IClientCache& ClientCache;
};

class TPendingPipeTrackerCommands {
public:
    void AttachTablet(ui64 txid, ui64 tabletid, ui64 cookie = 0);
    void DetachTablet(ui64 txid, ui64 tabletid, ui64 cookie = 0);
    void Clear();
    void Apply(TPipeTracker& tracker, const TActorContext& ctx);

private:
    struct TCommand {
        ui64 TxId;
        ui64 TabletId;
        ui64 Cookie;

        TCommand(ui64 txid, ui64 tabletid, ui64 cookie)
            : TxId(txid)
            , TabletId(tabletid)
            , Cookie(cookie)
        {}
    };

    TVector<TCommand> Attach;
    TVector<TCommand> Detach;
};

}
