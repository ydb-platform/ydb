#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData::TResolveResultAccumulator {
        const TActorId Sender;
        const ui64 Cookie;
        const TActorId InterconnectSession;
        std::deque<NKikimrBlobDepot::TEvResolveResult::TResolvedKey> Items;

    public:
        TResolveResultAccumulator(TEventHandle<TEvBlobDepot::TEvResolve>& ev);

        void AddItem(NKikimrBlobDepot::TEvResolveResult::TResolvedKey&& item);
        void Send(TActorIdentity selfId, NKikimrProto::EReplyStatus status, std::optional<TString> errorReason,
            const std::unordered_set<TKey> *filter = nullptr, const NKikimrBlobDepot::TBlobDepotConfig *config = nullptr);
        std::deque<NKikimrBlobDepot::TEvResolveResult::TResolvedKey> ReleaseItems();
    };

} // NKikimr::NBlobDepot
