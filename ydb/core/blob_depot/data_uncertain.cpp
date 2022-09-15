#include "data.h"
#include "data_uncertain.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    // FIXME(alexvru): make sure that all situations where ValueChain gets changed during the resolution process are
    // handled correctly

    TData::TUncertaintyResolver::TUncertaintyResolver(TBlobDepot *self)
        : Self(self)
    {}

    void TData::TUncertaintyResolver::PushResultWithUncertainties(TResolveResultAccumulator&& result,
            std::deque<TKey>&& uncertainties) {
        Y_VERIFY(!uncertainties.empty());

        auto entry = MakeIntrusive<TResolveOnHold>(std::move(result));
        for (const TKey& key : uncertainties) {
            if (const TValue *value = Self->Data->FindKey(key); value && value->UncertainWrite && !value->ValueChain.empty()) {
                const auto [it, _] = Keys.try_emplace(key);
                it->second.DependentRequests.push_back(entry);
                ++entry->NumUncertainKeys;
                CheckAndFinishKeyIfPossible(&*it);
            } else {
                // this value is not uncertainly written anymore, we can issue response
                // FIXME: handle race when underlying value gets changed here and we reply with old value chain
            }
        }

        if (entry->NumUncertainKeys == 0) {
            // we had no more uncertain keys to resolve
            entry->Result.Send(Self->SelfId(), NKikimrProto::OK, std::nullopt);
        }
    }

    void TData::TUncertaintyResolver::MakeKeyCertain(const TKey& key) {
        FinishKey(key, true);
    }

    void TData::TUncertaintyResolver::DropBlobs(const std::vector<TLogoBlobID>& blobIds) {
        for (const TLogoBlobID& id : blobIds) {
            FinishBlob(id, false);
        }
    }

    void TData::TUncertaintyResolver::DropKey(const TKey& key) {
        FinishKey(key, false);
    }

    void TData::TUncertaintyResolver::IssueIndexRestoreGetQuery(TKeys::value_type *keyRecord, TLogoBlobID id) {
        const auto [it, inserted] = Blobs.try_emplace(id);
        TBlobContext& blobContext = it->second;

        const bool inserted1 = blobContext.ReferringKeys.insert(keyRecord).second;
        Y_VERIFY(inserted1);

        const bool inserted2 = keyRecord->second.BlobQueriesInFlight.insert(id).second;
        Y_VERIFY(inserted2);

        if (inserted) {
            const ui32 groupId = Self->Info()->GroupFor(id.Channel(), id.Generation());
            SendToBSProxy(Self->SelfId(), groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
        }
    }

    void TData::TUncertaintyResolver::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        Y_VERIFY(msg.ResponseSz == 1);
        auto& resp = msg.Responses[0];
        FinishBlob(resp.Id, resp.Status == NKikimrProto::OK);
    }

    void TData::TUncertaintyResolver::FinishBlob(TLogoBlobID id, bool success) {
        const auto blobIt = Blobs.find(id);
        if (blobIt == Blobs.end()) {
            return;
        }
        auto blob = Blobs.extract(blobIt);
        TBlobContext& blobContext = blob.mapped();

        for (TKeys::value_type *keyRecord : blobContext.ReferringKeys) {
            auto& [key, keyContext] = *keyRecord;

            const auto blobInFlightIt = keyContext.BlobQueriesInFlight.find(id);
            Y_VERIFY(blobInFlightIt != keyContext.BlobQueriesInFlight.end());
            auto blobInFlight = keyContext.BlobQueriesInFlight.extract(blobInFlightIt);
            if (success) {
                keyContext.ConfirmedBlobs.insert(std::move(blobInFlight));
            }

            CheckAndFinishKeyIfPossible(keyRecord);
        }
    }

    void TData::TUncertaintyResolver::CheckAndFinishKeyIfPossible(TKeys::value_type *keyRecord) {
        auto& [key, keyContext] = *keyRecord;

        bool okay = true;

        if (const TValue *value = Self->Data->FindKey(key); value && !value->ValueChain.empty()) {
            Y_VERIFY(value->UncertainWrite); // otherwise we must have already received push notification

            EnumerateBlobsForValueChain(value->ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                auto& [key, keyContext] = *keyRecord;

                if (keyContext.ConfirmedBlobs.contains(id)) {
                    // this blob is in, okay
                } else if (keyContext.BlobQueriesInFlight.contains(id)) {
                    // still have to wait for this one
                    okay = false;
                } else {
                    // have to additionally query this blob and wait for it
                    okay = false;
                    IssueIndexRestoreGetQuery(keyRecord, id);
                }
            });
        } else { // key has been deleted, we have to drop it from the response
            okay = false;
            FinishKey(key, false);
        }

        if (okay) {
            Self->Data->MakeKeyCertain(key);
        }
    }

    void TData::TUncertaintyResolver::FinishKey(const TKey& key, bool success) {
        const auto keyIt = Keys.find(key);
        if (keyIt == Keys.end()) {
            return;
        }

        auto item = Keys.extract(keyIt);
        auto& keyContext = item.mapped();

        for (auto& request : keyContext.DependentRequests) {
            if (!success) {
                request->KeysToBeFilteredOut.insert(key);
            }
            if (--request->NumUncertainKeys == 0) { // we can finish the request
                request->Result.Send(Self->SelfId(), NKikimrProto::OK, std::nullopt, &request->KeysToBeFilteredOut,
                    &Self->Config);
            }
        }

        for (const TLogoBlobID& id : keyContext.BlobQueriesInFlight) {
            const auto blobIt = Blobs.find(id);
            Y_VERIFY(blobIt != Blobs.end());
            TBlobContext& blobContext = blobIt->second;
            const size_t numErased = blobContext.ReferringKeys.erase(&*keyIt);
            Y_VERIFY(numErased == 1);
            if (blobContext.ReferringKeys.empty()) {
                Blobs.erase(blobIt);
            }
        }
    }

} // NKikimr::NBlobDepot
