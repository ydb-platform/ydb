#pragma once

#include "defs.h"

#include "data.h"
#include "data_resolve.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData::TUncertaintyResolver {
        TBlobDepot* const Self;

        struct TPendingUncertainKey {};

        struct TResolveOnHold : TSimpleRefCount<TResolveOnHold> {
            TResolveResultAccumulator Result;
            ui32 NumUncertainKeys = 0;
            std::unordered_set<TKey> KeysToBeFilteredOut;

            TResolveOnHold(TResolveResultAccumulator&& result)
                : Result(std::move(result))
            {}
        };

        struct TKeyContext {
            // requests dependent on this key
            std::vector<TIntrusivePtr<TResolveOnHold>> DependentRequests;

            // blob queries involved
            std::set<TLogoBlobID> BlobQueriesInFlight;

            // found blobs
            std::set<TLogoBlobID> ConfirmedBlobs;
        };

        using TKeys = std::map<TKey, TKeyContext>;

        struct TBlobContext {
            // keys referring to this blob
            std::unordered_set<TKeys::value_type*> ReferringKeys;
        };

        TKeys Keys;
        std::unordered_map<TLogoBlobID, TBlobContext> Blobs;

    public:
        TUncertaintyResolver(TBlobDepot *self);
        void PushResultWithUncertainties(TResolveResultAccumulator&& result, std::deque<TKey>&& uncertainties);
        void MakeKeyCertain(const TKey& key);
        void DropBlobs(const std::vector<TLogoBlobID>& blobIds);
        void DropKey(const TKey& key);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);

    private:
        void IssueIndexRestoreGetQuery(TKeys::value_type *keyRecord, TLogoBlobID id);
        void FinishBlob(TLogoBlobID id, bool success);
        void CheckAndFinishKeyIfPossible(TKeys::value_type *keyRecord);
        void FinishKey(const TKey& key, bool success);
    };

} // NKikimr::NBlobDepot
