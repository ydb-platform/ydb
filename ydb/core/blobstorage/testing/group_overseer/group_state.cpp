#include "group_state.h"

namespace NKikimr::NTesting {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Block

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvBlock& msg) {
        TBlockInfo& block = Blocks[msg.TabletId];
        const auto inflightIt = block.InFlight.emplace(msg);
        const bool inserted1 = block.QueryToInFlight.emplace(queryId, inflightIt).second;
        Y_ABORT_UNLESS(inserted1);
        const bool inserted2 = BlockQueryToTabletId.emplace(queryId, msg.TabletId).second;
        Y_ABORT_UNLESS(inserted2);
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvBlockResult& msg) {
        auto query = BlockQueryToTabletId.extract(queryId);
        Y_ABORT_UNLESS(query);

        TBlockInfo& block = Blocks[query.mapped()];

        auto queryToInFlight = block.QueryToInFlight.extract(queryId);
        Y_ABORT_UNLESS(queryToInFlight);
        auto inFlight = block.InFlight.extract(queryToInFlight.mapped());
        Y_ABORT_UNLESS(inFlight);
        TBlockedGeneration& gen = inFlight.value();

        if (msg.Status == NKikimrProto::OK) {
            if (!block.Confirmed || block.Confirmed->Generation < gen.Generation || gen.Same(*block.Confirmed)) {
                block.Confirmed.emplace(gen);
            } else {
                Y_ABORT("incorrect successful block");
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Put

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvPut& msg) {
        Log(TStringBuilder() << "TEvPut " << msg.ToString());

        TBlobInfo& blob = *LookupBlob(msg.Id, true);

        TBlobValueHash valueHash(msg);

        if (blob.ValueHash) {
            Y_ABORT_UNLESS(*blob.ValueHash == valueHash);
        } else {
            blob.ValueHash.emplace(valueHash);
        }

        EConfidence isBlocked = IsBlocked(msg.Id.TabletID(), msg.Id.Generation());
        for (const auto& [tabletId, generation] : msg.ExtraBlockChecks) {
            isBlocked = Max(isBlocked, IsBlocked(tabletId, generation));
        }

        const EConfidence isCollected = IsCollected(msg.Id, &blob);

        const bool inserted = blob.PutsInFlight.emplace(queryId, TBlobInfo::TQueryContext{
            .IsBlocked = isBlocked == EConfidence::CONFIRMED, // if true, we can't get OK answer
            .IsCollected = isCollected == EConfidence::CONFIRMED, // if true, this blob is being put beyond the barrier
        }).second;
        Y_ABORT_UNLESS(inserted);
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvPutResult& msg) {
        Log(TStringBuilder() << "TEvPutResult " << msg.ToString());

        const auto it = Blobs.find(msg.Id);
        if (msg.Status != NKikimrProto::OK && it == Blobs.end()) {
            Y_ABORT_UNLESS(GetBlobState(msg.Id) == EBlobState::CERTAINLY_COLLECTED_OR_NEVER_WRITTEN);
            return; // it's okay to get ERROR for TEvPut when the blob is already collected and is not in the map
        }
        Y_ABORT_UNLESS(it != Blobs.end());
        TBlobInfo& blob = it->second;

        auto putInFlight = blob.PutsInFlight.extract(queryId);
        Y_ABORT_UNLESS(putInFlight);

        auto& v = putInFlight.mapped();
        if (v.IsBlocked && (msg.Status != NKikimrProto::BLOCKED && msg.Status != NKikimrProto::ERROR)) {
            Y_ABORT("incorrect TEvPut result status code -- was BLOCKED at the begin of the query");
        }

        if (v.IsCollected && msg.Status != NKikimrProto::ERROR) {
            Y_ABORT("incorrect TEvPut result status code -- was already beyond the barrier at begin of the query");
        }

        if (msg.Status == NKikimrProto::OK) {
            Y_ABORT_UNLESS(blob.ValueHash);
            blob.ConfirmedValue = true;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Patch

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId&, const TEvBlobStorage::TEvPatch&) {
        Y_ABORT("not implemented");
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId&, const TEvBlobStorage::TEvPatchResult&) {
        Y_ABORT("not implemented");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // InplacePatch

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId&, const TEvBlobStorage::TEvInplacePatch&) {
        Y_ABORT("not implemented");
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId&, const TEvBlobStorage::TEvInplacePatchResult&) {
        Y_ABORT("not implemented");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // CollectGarbage

    template<>
    void TGroupState::ExamineQueryEvent(const TQueryId& queryId, const TEvBlobStorage::TEvCollectGarbage& msg) {
        Log(TStringBuilder() << "TEvCollectGarbage " << msg.ToString());

        auto processFlags = [&](TVector<TLogoBlobID> *vector, bool isKeepFlag) {
            if (vector) {
                for (const TLogoBlobID& id : *vector) {
                    TBlobInfo& blob = *LookupBlob(id, true);
                    ++(isKeepFlag ? blob.NumKeepsInFlight : blob.NumDoNotKeepsInFlight);
                    FlagsInFlight.emplace(queryId, std::make_tuple(isKeepFlag, id));
                }
            }
        };

        processFlags(msg.Keep.Get(), true);
        processFlags(msg.DoNotKeep.Get(), false);

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
                },
                .Flags{
                    msg.DoNotKeep ? std::set<TLogoBlobID>(msg.DoNotKeep->begin(), msg.DoNotKeep->end()) : std::set<TLogoBlobID>{},
                    msg.Keep ? std::set<TLogoBlobID>(msg.Keep->begin(), msg.Keep->end()) : std::set<TLogoBlobID>{}
                }
            });

            const bool inserted = barrier.CollectsInFlight.emplace(queryId, inFlightIt).second;
            Y_ABORT_UNLESS(inserted);
        }
    }

    template<>
    void TGroupState::ExamineResultEvent(const TQueryId& queryId, const TEvBlobStorage::TEvCollectGarbageResult& msg) {
        Log(TStringBuilder() << "TEvCollectGarbageResult " << msg.ToString());

        std::set<TLogoBlobID> idsToExamine;

        auto [begin, end] = FlagsInFlight.equal_range(queryId);
        for (auto it = begin; it != end; ++it) {
            const auto& [isKeepFlag, id] = it->second;
            TBlobInfo& blob = *LookupBlob(id, true);
            --(isKeepFlag ? blob.NumKeepsInFlight : blob.NumDoNotKeepsInFlight);
            if (msg.Status == NKikimrProto::OK) {
                (isKeepFlag ? blob.ConfirmedKeep : blob.ConfirmedDoNotKeep) = true;
            }
            idsToExamine.insert(id);
        }
        FlagsInFlight.erase(begin, end);

        const TBarrierId barrierId(msg.TabletId, msg.Channel);
        TBarrierInfo& barrier = Barriers[barrierId];
        if (auto query = barrier.CollectsInFlight.extract(queryId)) {
            auto inFlightIt = query.mapped();
            const bool hard = inFlightIt->Hard;
            auto inFlight = barrier.InFlight[hard].extract(inFlightIt);
            Y_ABORT_UNLESS(inFlight);
            if (msg.Status == NKikimrProto::OK) {
                const auto& value = inFlight.value().Value;
                std::optional<std::tuple<ui32, ui32>> prev;
                if (auto& dest = barrier.Confirmed[hard]) {
                    prev.emplace(dest->GetCollectGenStep());
                    dest->Supersede(value);
                } else {
                    dest.emplace(value);
                }
                ApplyBarrier(barrierId, prev, value.GetCollectGenStep());
            }
        }

        for (const TLogoBlobID& id : idsToExamine) {
            if (const auto it = Blobs.find(id); it != Blobs.end() && IsCollected(id, &it->second) == EConfidence::CONFIRMED) {
                Blobs.erase(id);
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

    TGroupState::TGroupState(ui32 groupId)
        : LogPrefix(TStringBuilder() << "[" << groupId << "] ")
    {}

    void TGroupState::Log(TString message) const {
        if (LogPrefix) {
            Cerr << LogPrefix << message << Endl;
        }
    }

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

    TGroupState::EConfidence TGroupState::IsCollected(TLogoBlobID id, const TBlobInfo *blob) const {
        const TBarrierId barrierId(id.TabletID(), id.Channel());
        if (const auto it = Barriers.find(barrierId); it != Barriers.end()) {
            const TBarrierInfo& barrier = it->second;
            const auto genstep = std::make_tuple(id.Generation(), id.Step());

            bool confirmedHard = (barrier.Confirmed[true] && genstep <= barrier.Confirmed[true]->GetCollectGenStep());

            bool inflightHard = false;
            {
                const auto& inflight = barrier.InFlight[true];
                if (!inflight.empty()) {
                    const auto& most = *--inflight.end();
                    if (genstep <= most.Value.GetCollectGenStep()) {
                        inflightHard = true;
                    }
                }
            }

            bool confirmedSoft = (!blob || blob->ConfirmedDoNotKeep || !blob->ConfirmedKeep) &&
                    (barrier.Confirmed[false] && genstep <= barrier.Confirmed[false]->GetCollectGenStep());

            bool inflightSoft = false;
            if (blob && !blob->ConfirmedDoNotKeep) {
                const auto& inflight = barrier.InFlight[false];
                auto it = inflight.begin();
                for (; it != inflight.end() && genstep > it->Value.GetCollectGenStep(); ++it) {}

                if (it != inflight.end()) {
                    for (; it != inflight.end(); ++it) {
                        if (!it->Flags[true].count(id) || it->Flags[false].count(id)) {
                            // The blob can be collected after applying this barrier
                            inflightSoft = true;
                            break;
                        }
                    }
                }
            }

            if (confirmedHard || confirmedSoft) {
                return EConfidence::CONFIRMED;
            }
            if (inflightHard || inflightSoft) {
                return EConfidence::POSSIBLE;
            }
            return EConfidence::SURELY_NOT;
        }
        return EConfidence::SURELY_NOT;
    }

    void TGroupState::ApplyBarrier(TBarrierId barrierId, std::optional<std::tuple<ui32, ui32>> prevGenStep,
            std::tuple<ui32, ui32> collectGenStep) {
        const auto& [tabletId, channel] = barrierId;
        auto it = prevGenStep
            ? Blobs.upper_bound(TLogoBlobID(tabletId, std::get<0>(*prevGenStep), std::get<1>(*prevGenStep), channel,
                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode))
            : Blobs.lower_bound(TLogoBlobID(tabletId, 0, 0, channel, 0, 0));
        auto key = [](const TLogoBlobID& id) {
            return std::make_tuple(id.TabletID(), id.Channel(), id.Generation(), id.Step());
        };
        const auto maxKey = std::tuple_cat(std::make_tuple(tabletId, channel), collectGenStep);
        while (it != Blobs.end() && key(it->first) <= maxKey) {
            if (IsCollected(it->first, &it->second) == EConfidence::CONFIRMED) {
                it = Blobs.erase(it);
            } else {
                ++it;
            }
        }
    }

    TGroupState::TBlobInfo *TGroupState::LookupBlob(TLogoBlobID id, bool create) const {
        Y_ABORT_UNLESS(id.BlobSize() != 0);
        Y_ABORT_UNLESS(id.PartId() == 0);

        const TLogoBlobID min(id.TabletID(), id.Generation(), id.Step(), id.Channel(), 0, id.Cookie());
        auto it = Blobs.lower_bound(min);
        if (it != Blobs.end()) {
            const TLogoBlobID& key = it->first;
            if (key.TabletID() == id.TabletID() && key.Channel() == id.Channel() && key.Generation() == id.Generation() &&
                    key.Step() == id.Step() && key.Cookie() == id.Cookie()) {
                Y_ABORT_UNLESS(key == id);
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
            switch (IsCollected(id, nullptr)) {
                case EConfidence::SURELY_NOT: return EBlobState::NOT_WRITTEN;
                case EConfidence::POSSIBLE: return EBlobState::NOT_WRITTEN;
                case EConfidence::CONFIRMED: return EBlobState::CERTAINLY_COLLECTED_OR_NEVER_WRITTEN;
            }
            Y_UNREACHABLE();
        }
        if (!blob->ValueHash) {
            return EBlobState::NOT_WRITTEN;
        }
        switch (IsCollected(id, blob)) {
            case EConfidence::SURELY_NOT:
                return blob->ConfirmedValue ? EBlobState::CERTAINLY_WRITTEN : EBlobState::POSSIBLY_WRITTEN;

            case EConfidence::POSSIBLE:
                return EBlobState::POSSIBLY_COLLECTED;

            case EConfidence::CONFIRMED:
                Y_ABORT(); // must never reach this point -- blob must be deleted from the map when this state occurs
        }
    }

    void TGroupState::EnumerateBlobs(const std::function<void(TLogoBlobID, EBlobState)>& callback) const {
        for (const auto& [id, blob] : Blobs) {
            callback(id, GetBlobState(id, &blob));
        }
    }

} // NKikimr::NTesting
