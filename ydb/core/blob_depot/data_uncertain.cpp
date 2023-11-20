#include "data.h"
#include "data_uncertain.h"
#include "mon_main.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    // FIXME(alexvru): make sure that all situations where ValueChain gets changed during the resolution process are
    // handled correctly

    TData::TUncertaintyResolver::TUncertaintyResolver(TBlobDepot *self)
        : Self(self)
    {}

    void TData::TUncertaintyResolver::PushResultWithUncertainties(TResolveResultAccumulator&& result,
            std::deque<TKey>&& uncertainties) {
        Y_ABORT_UNLESS(!uncertainties.empty());

        auto entry = MakeIntrusive<TResolveOnHold>(std::move(result));

        for (const TKey& key : uncertainties) {
            if (const TValue *value = Self->Data->FindKey(key); value && value->IsWrittenUncertainly()) {
                const auto [it, _] = Keys.try_emplace(key);
                TKeyContext& keyContext = it->second;
                keyContext.DependentRequests.push_back(entry);
                ++entry->NumUncertainKeys;
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT61, "uncertain key", (Id, Self->GetLogId()),
                    (Sender, entry->Result.GetSender()), (Cookie, entry->Result.GetCookie()), (Key, key));

                // obtain list of blobs belonging to this key only once and here
                EnumerateBlobsForValueChain(value->ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                    keyContext.BlobState.emplace(id, std::make_tuple(EKeyBlobState::INITIAL, TString()));
                });

                // try to process the blobs
                CheckAndFinishKeyIfPossible(&*it);
            } else {
                // this value is not uncertainly written anymore, we can issue response
                // FIXME: handle race when underlying value gets changed here and we reply with old value chain
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT62, "racing uncertain key", (Id, Self->GetLogId()),
                    (Sender, entry->Result.GetSender()), (Cookie, entry->Result.GetCookie()), (Key, key));
            }
        }

        if (entry->NumUncertainKeys == 0) {
            // we had no more uncertain keys to resolve
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT63, "uncertainty resolver finished with noop", (Id, Self->GetLogId()),
                (Sender, entry->Result.GetSender()), (Cookie, entry->Result.GetCookie()));
            entry->Result.Send(NKikimrProto::OK, std::nullopt);
        } else {
            NumKeysQueried += entry->NumUncertainKeys;
        }
    }

    void TData::TUncertaintyResolver::MakeKeyCertain(const TKey& key) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT70, "TUncertaintyResolver::MakeKeyCertain", (Id, Self->GetLogId()), (Key, key));
        FinishKey(key, NKikimrProto::OK, {});
    }

    void TData::TUncertaintyResolver::DropBlobs(const std::vector<TLogoBlobID>& blobIds) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT71, "TUncertaintyResolver::DropBlobs", (Id, Self->GetLogId()), (BlobIds, blobIds));
        for (const TLogoBlobID& id : blobIds) {
            FinishBlob(id, EKeyBlobState::WASNT_WRITTEN, {});
        }
    }

    void TData::TUncertaintyResolver::DropKey(const TKey& key) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT72, "TUncertaintyResolver::DropKey", (Id, Self->GetLogId()), (Key, key));
        FinishKey(key, NKikimrProto::NODATA, {});
        ++NumKeysDropped;
    }

    void TData::TUncertaintyResolver::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT73, "TUncertaintyResolver::Handle(TEvGetResult)", (Id, Self->GetLogId()),
            (Msg, msg));
        Y_ABORT_UNLESS(msg.ResponseSz == 1);
        auto& resp = msg.Responses[0];
        FinishBlob(resp.Id, resp.Status == NKikimrProto::OK ? EKeyBlobState::CONFIRMED :
            resp.Status == NKikimrProto::NODATA ? EKeyBlobState::WASNT_WRITTEN :
            EKeyBlobState::ERROR, msg.ErrorReason ? msg.ErrorReason : "EvGet failed");
    }

    void TData::TUncertaintyResolver::FinishBlob(TLogoBlobID id, EKeyBlobState state, const TString& errorReason) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT64, "TUncertaintyResolver::FinishBlob", (Id, Self->GetLogId()), (BlobId, id),
            (State, state), (ErrorReason, errorReason), (Contained, Blobs.contains(id)));

        THashSet<TKeys::value_type*> keyRecordsToCheck;

        const auto it = Blobs.find(id);
        if (it == Blobs.end()) {
            return;
        }
        TBlobContext& blob = it->second;
        for (TKeys::value_type *keyRecord : blob.KeysWaitingForThisBlob) {
            TKeyContext& keyContext = keyRecord->second;
            const auto blobStateIt = keyContext.BlobState.find(id);
            Y_ABORT_UNLESS(blobStateIt != keyContext.BlobState.end());
            blobStateIt->second = {state, errorReason};
            keyRecordsToCheck.insert(keyRecord);
        }
        Blobs.erase(it);

        for (TKeys::value_type *keyRecord : keyRecordsToCheck) {
            CheckAndFinishKeyIfPossible(keyRecord);
        }
    }

    void TData::TUncertaintyResolver::CheckAndFinishKeyIfPossible(TKeys::value_type *keyRecord) {
        auto& [key, keyContext] = *keyRecord;

        bool wait = false;
        bool nodata = false;
        bool error = false;
        TStringStream errorReason;

        for (auto& [id, item] : keyContext.BlobState) {
            auto& [state, blobErrorReason] = item;
            switch (state) {
                case EKeyBlobState::INITIAL: {
                    TBlobContext& blob = Blobs[id];
                    const auto [_, inserted] = blob.KeysWaitingForThisBlob.insert(keyRecord);
                    Y_ABORT_UNLESS(inserted);
                    if (blob.KeysWaitingForThisBlob.size() == 1) {
                        // have to query this blob and wait for the response
                        const ui32 groupId = Self->Info()->GroupFor(id.Channel(), id.Generation());
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT65, "TUncertaintyResolver sending Get", (Id, Self->GetLogId()),
                            (BlobId, id), (Key, key), (GroupId, groupId));
                        SendToBSProxy(Self->SelfId(), groupId, new TEvBlobStorage::TEvGet(id, 0, 0, TInstant::Max(),
                            NKikimrBlobStorage::EGetHandleClass::FastRead, true, true));
                        ++NumGetsIssued;
                    }

                    state = EKeyBlobState::QUERY_IN_FLIGHT;
                    [[fallthrough]];
                }
                case EKeyBlobState::QUERY_IN_FLIGHT:
                    // still have to wait for this one
                    wait = true;
                    break;

                case EKeyBlobState::CONFIRMED:
                    // blob was found and it is ok
                    break;

                case EKeyBlobState::WASNT_WRITTEN:
                    // the blob hasn't been written completely; this may also be a race when it is being written
                    // right now, but we are asking for the data too early (like in scan request); however this means
                    // that blob couldn't have been reported as OK to the agent, and we may respond with NODATA to it
                    nodata = true;
                    break;

                case EKeyBlobState::ERROR:
                    // we can't figure out this blob's state; this means we have to respond with ERROR for this
                    // particular blob
                    if (error) {
                        errorReason << ", ";
                    }
                    errorReason << id << ": " << blobErrorReason;
                    error = true;
                    break;
            }
        }

        if (error) {
            FinishKey(key, NKikimrProto::ERROR, errorReason.Str());
        } else if (nodata) {
            FinishKey(key, NKikimrProto::NODATA, {});
        } else if (wait) {
            // just do nothing, wait for the request to fulfill
        } else {
            Self->Data->MakeKeyCertain(key);
        }
    }

    void TData::TUncertaintyResolver::FinishKey(const TKey& key, NKikimrProto::EReplyStatus status,
            const TString& errorReason) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT66, "TUncertaintyResolver::FinishKey", (Id, Self->GetLogId()), (Key, key),
            (Status, status));

        const auto keyIt = Keys.find(key);
        if (keyIt == Keys.end()) {
            return;
        }
        auto& keyContext = keyIt->second;

        ++(status == NKikimrProto::OK ? NumKeysResolved : NumKeysUnresolved);

        for (auto& request : keyContext.DependentRequests) {
            switch (status) {
                case NKikimrProto::OK:
                    break;

                case NKikimrProto::NODATA:
                    request->Result.AddKeyWithNoData(key);
                    break;

                case NKikimrProto::ERROR:
                    request->Result.AddKeyWithError(key, errorReason);
                    break;

                default:
                    Y_ABORT();
            }
            if (--request->NumUncertainKeys == 0) { // we can finish the request
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT67, "uncertainty resolver finished", (Id, Self->GetLogId()),
                    (Sender, request->Result.GetSender()), (Cookie, request->Result.GetCookie()));
                request->Result.Send(NKikimrProto::OK, std::nullopt);
            }
        }

        for (const auto& [id, s] : keyContext.BlobState) {
            const auto& [state, errorReason] = s;
            if (state == EKeyBlobState::QUERY_IN_FLIGHT) {
                const auto blobIt = Blobs.find(id);
                Y_ABORT_UNLESS(blobIt != Blobs.end());
                TBlobContext& blobContext = blobIt->second;
                const size_t numErased = blobContext.KeysWaitingForThisBlob.erase(&*keyIt);
                Y_ABORT_UNLESS(numErased == 1);
                if (blobContext.KeysWaitingForThisBlob.empty()) {
                    Blobs.erase(blobIt);
                }
            }
        }

        Keys.erase(keyIt);
    }

    void TData::TUncertaintyResolver::RenderMainPage(IOutputStream& s) {
        HTML(s) {
            KEYVALUE_TABLE({
                KEYVALUE_P("Keys queried", NumKeysQueried);
                KEYVALUE_P("Gets issued", NumGetsIssued);
                KEYVALUE_P("Keys resolved", NumKeysResolved);
                KEYVALUE_P("Keys unresolved", NumKeysUnresolved);
                KEYVALUE_P("Keys dropped", NumKeysDropped);
                KEYVALUE_P("Keys being processed", Keys.size());
                KEYVALUE_P("Blobs in flight", Blobs.size());
            })
        }
    }

} // NKikimr::NBlobDepot
