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
            THashMap<TLogoBlobID, std::tuple<EKeyBlobState, TString>> BlobState;
        };

        using TKeys = THashMap<TKey, TKeyContext>;

        struct TBlobContext {
            THashSet<TKeys::value_type*> KeysWaitingForThisBlob;
        };

        TKeys Keys;
        THashMap<TLogoBlobID, TBlobContext> Blobs;

        ui64 NumKeysQueried = 0;
        ui64 NumGetsIssued = 0;
        ui64 NumKeysResolved = 0;
        ui64 NumKeysUnresolved = 0;
        ui64 NumKeysDropped = 0;

        friend void Out<EKeyBlobState>(IOutputStream& s, EKeyBlobState value) {
            switch (value) {
                case EKeyBlobState::INITIAL:            s << "INITIAL";             break;
                case EKeyBlobState::QUERY_IN_FLIGHT:    s << "QUERY_IN_FLIGHT";     break;
                case EKeyBlobState::CONFIRMED:          s << "CONFIRMED";           break;
                case EKeyBlobState::WASNT_WRITTEN:      s << "WASNT_WRITTEN";       break;
                case EKeyBlobState::ERROR:              s << "ERROR";               break;
            }
        }

    public:
        TUncertaintyResolver(TBlobDepot *self);
        void PushResultWithUncertainties(TResolveResultAccumulator&& result, std::deque<TKey>&& uncertainties);
        void MakeKeyCertain(const TKey& key);
        void DropBlobs(const std::vector<TLogoBlobID>& blobIds);
        void DropKey(const TKey& key);
        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev);

        void RenderMainPage(IOutputStream& s);

    private:
        void FinishBlob(TLogoBlobID id, EKeyBlobState state, const TString& errorReason);
        void CheckAndFinishKeyIfPossible(TKeys::value_type *keyRecord);
        void FinishKey(const TKey& key, NKikimrProto::EReplyStatus status, const TString& errorReason);
    };

} // NKikimr::NBlobDepot
