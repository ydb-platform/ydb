#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData::TResolveResultAccumulator {
        const TActorId Sender;
        const TActorId Recipient;
        const ui64 Cookie;
        const TActorId InterconnectSession;
        std::deque<NKikimrBlobDepot::TEvResolveResult::TResolvedKey> Items;
        std::unordered_map<TKey, size_t> KeyToIndex;
        std::vector<bool> KeysToFilterOut;

    public:
        TResolveResultAccumulator(TEventHandle<TEvBlobDepot::TEvResolve>& ev);

        void AddItem(NKikimrBlobDepot::TEvResolveResult::TResolvedKey&& item, const NKikimrBlobDepot::TBlobDepotConfig& config);
        void Send(NKikimrProto::EReplyStatus status, std::optional<TString> errorReason);
        std::deque<NKikimrBlobDepot::TEvResolveResult::TResolvedKey> ReleaseItems();

        void AddKeyWithNoData(const TKey& key);
        void AddKeyWithError(const TKey& key, const TString& errorReason);

        const TActorId& GetSender() const { return Sender; }
        ui64 GetCookie() const { return Cookie; }
    };

} // NKikimr::NBlobDepot
