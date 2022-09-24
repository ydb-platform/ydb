#include "group_state.h"

namespace NKikimr::NTesting {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Block

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvBlock& msg) {
        TBlockInfo& block = Blocks[msg.TabletId];
        const auto inflightIt = block.InFlight.emplace(msg);
        const bool inserted1 = block.QueryToInFlight.emplace(queryId, inflightIt).second;
        Y_VERIFY(inserted1);
        const bool inserted2 = BlockQueryToTabletId.emplace(queryId, msg.TabletId).second;
        Y_VERIFY(inserted2);
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvBlockResult& msg) {
        auto query = BlockQueryToTabletId.extract(queryId);
        Y_VERIFY(query);

        TBlockInfo& block = Blocks[query.mapped()];

        auto queryToInFlight = block.QueryToInFlight.extract(queryId);
        Y_VERIFY(queryToInFlight);
        auto inFlight = block.InFlight.extract(queryToInFlight.mapped());
        Y_VERIFY(inFlight);
        TBlockedGeneration& gen = inFlight.value();

        if (msg.Status == NKikimrProto::OK) {
            if (!block.Confirmed || block.Confirmed->Generation < gen.Generation || gen.Same(*block.Confirmed)) {
                block.Confirmed.emplace(gen);
            } else {
                Y_FAIL("incorrect successful block");
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Put

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvPut& msg) {
        TBlobInfo& blob = *LookupBlob(msg.Id, true);

        TBlobValueHash valueHash(msg);

        if (blob.ValueHash) {
            Y_VERIFY(*blob.ValueHash == valueHash);
        } else {
            blob.ValueHash.emplace(valueHash);
        }

        EConfidence isBlocked = IsBlocked(msg.Id.TabletID(), msg.Id.Generation());
        for (const auto& [tabletId, generation] : msg.ExtraBlockChecks) {
            isBlocked = Max(isBlocked, IsBlocked(tabletId, generation));
        }

        const EConfidence isCollected = IsCollected(msg.Id,
            blob.ConfirmedKeep ? EConfidence::CONFIRMED :
                blob.NumKeepsInFlight ?  EConfidence::POSSIBLE :
                EConfidence::SURELY_NOT,
            blob.ConfirmedDoNotKeep ? EConfidence::CONFIRMED :
                blob.NumDoNotKeepsInFlight ?  EConfidence::POSSIBLE :
                EConfidence::SURELY_NOT);

        const bool inserted = blob.PutsInFlight.emplace(queryId, TBlobInfo::TQueryContext{
            .IsBlocked = isBlocked == EConfidence::CONFIRMED, // if true, we can't get OK answer
            .IsCollected = isCollected == EConfidence::CONFIRMED, // if true, this blob is being put beyond the barrier
        }).second;
        Y_VERIFY(inserted);
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvPutResult& msg) {
        const auto it = Blobs.find(msg.Id);
        if (msg.Status != NKikimrProto::OK && it == Blobs.end()) {
            Y_VERIFY(GetBlobState(msg.Id) == EBlobState::CERTAINLY_COLLECTED_OR_NEVER_WRITTEN);
            return; // it's okay to get ERROR for TEvPut when the blob is already collected and is not in the map
        }
        Y_VERIFY(it != Blobs.end());
        TBlobInfo& blob = it->second;

        auto putInFlight = blob.PutsInFlight.extract(queryId);
        Y_VERIFY(putInFlight);

        auto& v = putInFlight.mapped();
        if (v.IsBlocked && (msg.Status != NKikimrProto::BLOCKED && msg.Status != NKikimrProto::ERROR)) {
            Y_FAIL("incorrect TEvPut result status code -- was BLOCKED at the begin of the query");
        }

        if (v.IsCollected && msg.Status != NKikimrProto::ERROR) {
            Y_FAIL("incorrect TEvPut result status code -- was already beyond the barrier at begin of the query");
        }

        if (msg.Status == NKikimrProto::OK) {
            Y_VERIFY(blob.ValueHash);
            blob.ConfirmedValue = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Patch

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId&, const TEvBlobStorage::TEvPatch&) {
        Y_FAIL("not implemented");
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId&, const TEvBlobStorage::TEvPatchResult&) {
        Y_FAIL("not implemented");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // InplacePatch

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId&, const TEvBlobStorage::TEvInplacePatch&) {
        Y_FAIL("not implemented");
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId&, const TEvBlobStorage::TEvInplacePatchResult&) {
        Y_FAIL("not implemented");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // CollectGarbage

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvCollectGarbage& msg) {
        auto processFlags = [&](TVector<TLogoBlobID> *vector, bool isKeepFlag) {
            for (const TLogoBlobID& id : vector ? *vector : TVector<TLogoBlobID>()) {
                TBlobInfo& blob = *LookupBlob(id, true);
                ++(isKeepFlag ? blob.NumKeepsInFlight : blob.NumDoNotKeepsInFlight);
                FlagsInFlight.emplace(queryId, std::make_tuple(isKeepFlag, id));
            }
        };

        processFlags(msg.Keep.Get(), true);
        processFlags(msg.DoNotKeep.Get(), true);

        if (msg.Collect) {
            const TBarrierId id(msg.TabletId, msg.Channel);
            TBarrierInfo& barrier = Barriers[id];

            auto inFlightIt = barrier.InFlight[msg.Hard].insert({
                .Hard = msg.Hard,
                .Value{
                    .RecordGeneration = msg.RecordGeneration,
                    .PerGenerationCounter = msg.PerGenerationCounter,
                    .CollectGeneration = msg.CollectGeneration,
                    .CollectStep = msg.CollectStep
                }
            });

            const bool inserted = barrier.CollectsInFlight.emplace(queryId, inFlightIt).second;
            Y_VERIFY(inserted);
        }
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvCollectGarbageResult& msg) {
        auto [begin, end] = FlagsInFlight.equal_range(queryId);
        for (auto it = begin; it != end; ++it) {
            const auto& [isKeepFlag, id] = it->second;
            TBlobInfo& blob = *LookupBlob(id, true);
            --(isKeepFlag ? blob.NumKeepsInFlight : blob.NumDoNotKeepsInFlight);
            if (msg.Status == NKikimrProto::OK) {
                (isKeepFlag ? blob.ConfirmedKeep : blob.ConfirmedDoNotKeep) = true;
            }
        }
        FlagsInFlight.erase(begin, end);

        const TBarrierId barrierId(msg.TabletId, msg.Channel);
        TBarrierInfo& barrier = Barriers[barrierId];
        if (auto query = barrier.CollectsInFlight.extract(queryId)) {
            auto inFlightIt = query.mapped();
            const bool hard = inFlightIt->Hard;
            auto inFlight = barrier.InFlight[hard].extract(inFlightIt);
            Y_VERIFY(inFlight);
            if (msg.Status == NKikimrProto::OK) {
                if (auto& dest = barrier.Confirmed[hard]) {
                    dest->Supersede(inFlight.value().Value);
                } else {
                    dest.emplace(inFlight.value().Value);
                }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Get

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvGet& msg) {
        (void)queryId, (void)msg;
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvGetResult& msg) {
        (void)queryId, (void)msg;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Range

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvRange& msg) {
        (void)queryId, (void)msg;
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvRangeResult& msg) {
        (void)queryId, (void)msg;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Discover

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvDiscover& msg) {
        (void)queryId, (void)msg;
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvDiscoverResult& msg) {
        (void)queryId, (void)msg;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Common parts

    TGroupState::EConfidence TGroupState::TBlockInfo::IsBlocked(ui32 generation) const {
        if (Confirmed && generation <= Confirmed->Generation) {
            return EConfidence::CONFIRMED;
        } else if (InFlight.empty()) {
            return EConfidence::SURELY_NOT;
        } else {
            const TBlockedGeneration& gen = *--InFlight.end();
            return generation <= gen.Generation ? EConfidence::POSSIBLE : EConfidence::SURELY_NOT;
        }
    }

    TGroupState::EConfidence TGroupState::IsBlocked(ui64 tabletId, ui32 generation) const {
        const auto it = Blocks.find(tabletId);
        return it == Blocks.end() ? EConfidence::SURELY_NOT : it->second.IsBlocked(generation);
    }

    TGroupState::EConfidence TGroupState::IsCollected(TLogoBlobID id, EConfidence keep, EConfidence doNotKeep) const {
        EConfidence result = EConfidence::SURELY_NOT;

        const TBarrierId barrierId(id.TabletID(), id.Channel());
        if (const auto it = Barriers.find(barrierId); it != Barriers.end()) {
            const TBarrierInfo& barrier = it->second;
            const auto genstep = std::make_tuple(id.Generation(), id.Step());

            EConfidence isCollectedBySoftBarrier;
            switch (keep) {
                case EConfidence::SURELY_NOT:
                    isCollectedBySoftBarrier = EConfidence::CONFIRMED;
                    break;

                case EConfidence::POSSIBLE:
                    switch (doNotKeep) {
                        case EConfidence::SURELY_NOT:
                        case EConfidence::POSSIBLE:
                            isCollectedBySoftBarrier = EConfidence::POSSIBLE;
                            break;

                        case EConfidence::CONFIRMED:
                            // this case should not occur
                            isCollectedBySoftBarrier = EConfidence::SURELY_NOT;
                            break;
                    }
                    break;

                case EConfidence::CONFIRMED:
                    switch (doNotKeep) {
                        case EConfidence::SURELY_NOT:
                            isCollectedBySoftBarrier = EConfidence::SURELY_NOT;
                            break;

                        case EConfidence::POSSIBLE:
                        case EConfidence::CONFIRMED:
                            isCollectedBySoftBarrier = EConfidence::POSSIBLE;
                            break;
                    }
                    break;
            }

            auto getBarrierState = [&](const auto& confirmed, const auto& inflight) {
                if (confirmed && genstep <= confirmed->GetCollectGenStep()) {
                    return EConfidence::CONFIRMED;
                }
                if (!inflight.empty()) {
                    const auto& most = *--inflight.end();
                    if (genstep <= most.Value.GetCollectGenStep()) {
                        return EConfidence::POSSIBLE;
                    }
                }
                return EConfidence::SURELY_NOT;
            };

            result = Max(getBarrierState(barrier.Confirmed[true], barrier.InFlight[true]),
                Min(isCollectedBySoftBarrier, getBarrierState(barrier.Confirmed[false], barrier.InFlight[false])));
        }

        return result;
    }

    TGroupState::TBlobInfo *TGroupState::LookupBlob(TLogoBlobID id, bool create) const {
        Y_VERIFY(id.BlobSize() != 0);
        Y_VERIFY(id.PartId() == 0);

        const TLogoBlobID min(id.TabletID(), id.Generation(), id.Step(), id.Channel(), 0, id.Cookie());
        auto it = Blobs.lower_bound(min);
        if (it != Blobs.end()) {
            const TLogoBlobID& key = it->first;
            if (key.TabletID() == id.TabletID() && key.Channel() == id.Channel() && key.Generation() == id.Generation() &&
                    key.Step() == id.Step() && key.Cookie() == id.Cookie()) {
                Y_VERIFY(key == id);
            } else if (create) {
                it = const_cast<decltype(Blobs)&>(Blobs).try_emplace(it, id);
            } else {
                return nullptr;
            }
            return const_cast<TBlobInfo*>(&it->second);
        }
        return create ? &const_cast<decltype(Blobs)&>(Blobs)[id] : nullptr;
    }

    EBlobState TGroupState::GetBlobState(TLogoBlobID id, const TBlobInfo *blob) const {
        if (!blob) {
            blob = LookupBlob(id, false);
        }
        if (!blob) {
            switch (IsCollected(id, EConfidence::SURELY_NOT, EConfidence::SURELY_NOT)) {
                case EConfidence::SURELY_NOT: return EBlobState::NOT_WRITTEN;
                case EConfidence::POSSIBLE: return EBlobState::NOT_WRITTEN;
                case EConfidence::CONFIRMED: return EBlobState::CERTAINLY_COLLECTED_OR_NEVER_WRITTEN;
            }
            Y_UNREACHABLE();
        }
        if (!blob->ValueHash) {
            return EBlobState::NOT_WRITTEN;
        }
        const EConfidence keep = blob->ConfirmedKeep ? EConfidence::CONFIRMED :
            blob->NumKeepsInFlight ? EConfidence::POSSIBLE : EConfidence::SURELY_NOT;
        const EConfidence doNotKeep = blob->ConfirmedDoNotKeep ? EConfidence::CONFIRMED :
            blob->NumDoNotKeepsInFlight ? EConfidence::POSSIBLE : EConfidence::SURELY_NOT;
        switch (IsCollected(id, keep, doNotKeep)) {
            case EConfidence::SURELY_NOT:
                return blob->ConfirmedValue ? EBlobState::CERTAINLY_WRITTEN : EBlobState::POSSIBLY_WRITTEN;

            case EConfidence::POSSIBLE:
                return EBlobState::POSSIBLY_COLLECTED;

            case EConfidence::CONFIRMED:
                Y_FAIL(); // must never reach this point -- blob must be deleted from the map when this state occurs
        }
    }

    void TGroupState::EnumerateBlobs(const std::function<void(TLogoBlobID, EBlobState)>& callback) const {
        for (const auto& [id, blob] : Blobs) {
            callback(id, GetBlobState(id, &blob));
        }
    }

} // NKikimr::NTesting
