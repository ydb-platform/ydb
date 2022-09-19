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
            FinishBlob(id, EKeyBlobState::WASNT_WRITTEN);
        }
    }

    void TData::TUncertaintyResolver::DropKey(const TKey& key) {
        FinishKey(key, false);
    }

    void TData::TUncertaintyResolver::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        Y_VERIFY(msg.ResponseSz == 1);
        auto& resp = msg.Responses[0];
        FinishBlob(resp.Id, resp.Status == NKikimrProto::OK ? EKeyBlobState::CONFIRMED :
                resp.Status == NKikimrProto::NODATA ? EKeyBlobState::WASNT_WRITTEN :
                EKeyBlobState::ERROR);
    }

    void TData::TUncertaintyResolver::FinishBlob(TLogoBlobID id, EKeyBlobState state) {
        const auto blobIt = Blobs.find(id);
        if (blobIt == Blobs.end()) {
            return;
        }
        auto blob = Blobs.extract(blobIt);
        TBlobContext& blobContext = blob.mapped();

        for (TKeys::value_type *keyRecord : blobContext.ReferringKeys) {
            auto& [key, keyContext] = *keyRecord;

            const auto blobStateIt = keyContext.BlobState.find(id);
            Y_VERIFY(blobStateIt != keyContext.BlobState.end());
            blobStateIt->second = state;

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
                switch (EKeyBlobState& state = keyContext.BlobState[id]) {
                    case EKeyBlobState::INITIAL: {
                        // have to additionally query this blob and wait for it
                        TBlobContext& blobContext = Blobs[id];
                        const bool inserted = blobContext.ReferringKeys.insert(keyRecord).second;
                        Y_VERIFY(inserted);
                        if (blobContext.ReferringKeys.size() == 1) {
                            const ui32 groupId = Self->Info()->GroupFor(id.Channel(), id.Generation());
                            SendToBSProxy(Self->SelfId(), groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                                NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
                        }

                        okay = false;
                        state = EKeyBlobState::QUERY_IN_FLIGHT;
                        break;
                    }

                    case EKeyBlobState::QUERY_IN_FLIGHT:
                        // still have to wait for this one
                        okay = false;
                        break;

                    case EKeyBlobState::CONFIRMED:
                        // blob was found and it is ok
                        break;

                    case EKeyBlobState::WASNT_WRITTEN:
                        // the blob hasn't been written completely; this may also be a race when it is being written
                        // right now, but we are asking for the data too early (like in scan request)
                        break;

                    case EKeyBlobState::ERROR:
                        // we can't figure out this blob's state
                        break;
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

        for (const auto& [id, state] : keyContext.BlobState) {
            if (state == EKeyBlobState::QUERY_IN_FLIGHT) {
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
    }

} // NKikimr::NBlobDepot
