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

        enum class EKeyBlobState {
            INITIAL, // just created blob, no activity
            QUERY_IN_FLIGHT, // blob should have BlobContext referring to this key too
            CONFIRMED, // we got OK for this blob
            WASNT_WRITTEN, // we got NODATA for this blob, this key needs to be deleted if possible
            ERROR, // we got ERROR or any other reply for this blob
        };

        struct TKeyContext {
            // requests dependent on this key
            std::vector<TIntrusivePtr<TResolveOnHold>> DependentRequests;

            // blob queries issued and replied
            std::unordered_map<TLogoBlobID, EKeyBlobState> BlobState;
        };

        using TKeys = std::map<TKey, TKeyContext>;

        struct TBlobContext {
            // keys referring to this blob
            std::unordered_set<TKeys::value_type*> ReferringKeys;
        };

        TKeys Keys;
        std::unordered_map<TLogoBlobID, TBlobContext> Blobs;

        ui64 NumKeysQueried = 0;
        ui64 NumGetsIssued = 0;
        ui64 NumKeysResolved = 0;
        ui64 NumKeysUnresolved = 0;
        ui64 NumKeysDropped = 0;

    public:
        TUncertaintyResolver(TBlobDepot *self);
        void PushResultWithUncertainties(TResolveResultAccumulator&& result, std::deque<TKey>&& uncertainties);
        void MakeKeyCertain(const TKey& key);
        void DropBlobs(const std::vector<TLogoBlobID>& blobIds);
        void DropKey(const TKey& key);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);

        void RenderMainPage(IOutputStream& s);

    private:
        void FinishBlob(TLogoBlobID id, EKeyBlobState state);
        void CheckAndFinishKeyIfPossible(TKeys::value_type *keyRecord);
        void FinishKey(const TKey& key, bool success);
    };

} // NKikimr::NBlobDepot
