#include "data.h"
#include "coro_tx.h"
#include "data_uncertain.h"
#include "garbage_collection.h"
#include "s3.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    enum class EUpdateOutcome {
        CHANGE,
        NO_CHANGE,
        DROP
    };

    TData::TData(TBlobDepot *self)
        : Self(self)
        , UncertaintyResolver(std::make_unique<TUncertaintyResolver>(Self))
    {}

    TData::~TData()
    {}

    bool TData::IsTrashFullyLoaded() const {
        return TrashLoadState == ETrashLoadState::Complete;
    }

    TData::ETrashLoadState TData::GetTrashLoadState() const {
        return TrashLoadState;
    }

    ui64 TData::GetLoadedTrashRecords() const {
        return LoadedTrashRecords;
    }

    bool TData::TValue::Validate(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item) {
        if (!item.HasBlobLocator()) {
            return true;
        }
        const auto& locator = item.GetBlobLocator();
        return locator.HasGroupId() &&
            locator.HasBlobSeqId() &&
            locator.HasTotalDataLen() && std::invoke([&] {
                const auto& blobSeqId = locator.GetBlobSeqId();
                return blobSeqId.HasChannel() && blobSeqId.HasGeneration() && blobSeqId.HasStep() && blobSeqId.HasIndex();
            });
    }

    bool TData::TValue::SameValueChainAsIn(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item) const {
        if (ValueChain.size() != 1) {
            return false;
        }
        const auto& chain = ValueChain[0];
        if (chain.HasSubrangeBegin() || chain.HasSubrangeEnd()) {
            return false;
        }

        if (chain.HasBlobLocator() != item.HasBlobLocator()) {
            return false;
        } else if (chain.HasBlobLocator() && item.HasBlobLocator()) {
            Y_DEBUG_ABORT_UNLESS(chain.HasBlobLocator());
            const auto& locator1 = chain.GetBlobLocator();
            Y_DEBUG_ABORT_UNLESS(locator1.HasGroupId() && locator1.HasBlobSeqId() && locator1.HasTotalDataLen());

            Y_DEBUG_ABORT_UNLESS(item.HasBlobLocator());
            const auto& locator2 = item.GetBlobLocator();
            Y_DEBUG_ABORT_UNLESS(locator2.HasGroupId() && locator2.HasBlobSeqId() && locator2.HasTotalDataLen());

#define COMPARE_FIELD(NAME) \
            if (locator1.Has##NAME() != locator2.Has##NAME()) { \
                return false; \
            } else if (locator1.Has##NAME() && locator1.Get##NAME() != locator2.Get##NAME()) { \
                return false; \
            }
            COMPARE_FIELD(GroupId)
            COMPARE_FIELD(Checksum)
            COMPARE_FIELD(TotalDataLen)
            COMPARE_FIELD(FooterLen)
#undef COMPARE_FIELD

            if (TBlobSeqId::FromProto(locator1.GetBlobSeqId()) != TBlobSeqId::FromProto(locator2.GetBlobSeqId())) {
                return false;
            }
        }

        return true;
    }

    template<typename TLocal>
    bool TData::AccountValueChain(const TValueChain& chain, ES3ReferenceAction action,
            TLocal&& accountLocal, std::vector<TS3ObjectKey>* unreferenced) {
        bool hasS3References = false;
        for (const auto& link : chain) {
            EnumerateBlobsForValueChainItem(link, Self->TabletID(), TOverloaded{
                accountLocal,
                [&](TS3Locator locator) {
                    hasS3References = true;
                    UpdateS3Reference(link, locator, action, unreferenced);
                }
            });
        }
        return hasS3References;
    }

    template<typename T, typename... TArgs>
    bool TData::UpdateKey(TKey key, NTabletFlatExecutor::TTransactionContext& txc, void *cookie, const char *reason,
            T&& callback, TArgs&&... args) {
        bool underSoft = false, underHard = false;
        auto var = key.AsVariant();
        if (auto *id = std::get_if<TLogoBlobID>(&var)) {
            Self->BarrierServer->GetBlobBarrierRelation(*id, &underSoft, &underHard);
        }
        if (underHard && !Data.contains(key)) {
            YDB_LOG_DEBUG("UpdateKey: key under hard barrier, will not be created",
                {"marker", "BDT59"},
                {"id", Self->GetLogId()},
                {"key", key},
                {"reason", reason});
            return false; // no such key existed and will not be created as it hits the barrier
        }

        Y_DEFER {
            Self->JsonHandler.Invalidate();
        };

        const auto [it, inserted] = Data.try_emplace(std::move(key), std::forward<TArgs>(args)...);
        {
            auto& [key, value] = *it;

            std::vector<TLogoBlobID> deleteBlobs;
            std::vector<TS3ObjectKey> deleteS3;
            bool hasS3References = false;

            const ui32 oldHotGeneration = value.HotWriteGeneration;
            const ui64 oldHotSequence = value.HotWriteSequence;
            const bool uncertainWriteBefore = value.UncertainWrite;
            const bool wasUncertain = value.IsWrittenUncertainly();
            const bool wasGoingToAssimilate = value.GoingToAssimilate;
            const ui32 valueVersionBefore = value.ValueVersion;

            const ui32 generation = Self->Executor()->Generation();

#ifndef NDEBUG
            TValue originalValue(value);
#endif

            if (!inserted) {
                auto dropRefCount = [&](auto& map, auto&& key, auto& deleteQ) {
                    if (ui32& rc = --map[key]; !rc) {
                        deleteQ.push_back(std::move(key));
                    } else {
                        Y_ABORT_UNLESS(rc != Max<ui32>());
                    }
                };
                hasS3References = AccountValueChain(value.ValueChain, ES3ReferenceAction::Remove,
                    [&](TLogoBlobID id, ui32, ui32) { dropRefCount(RefCountBlobs, id, deleteBlobs); }, &deleteS3);
            }

            EUpdateOutcome outcome = callback(value, inserted);

#ifndef NDEBUG
            Y_ABORT_UNLESS(outcome != EUpdateOutcome::NO_CHANGE || !value.Changed(originalValue));
            Y_ABORT_UNLESS(inserted || value.ValueVersion == originalValue.ValueVersion + 1 || IsSameValueChain(value.ValueChain, originalValue.ValueChain));
#endif

            if ((underSoft && value.KeepState != EKeepState::Keep) || underHard) {
                outcome = EUpdateOutcome::DROP;
            }

            auto outcomeToString = [outcome] {
                switch (outcome) {
                    case EUpdateOutcome::CHANGE:    return "CHANGE";
                    case EUpdateOutcome::NO_CHANGE: return "NO_CHANGE";
                    case EUpdateOutcome::DROP:      return "DROP";
                }
            };
            YDB_LOG_DEBUG("UpdateKey",
                {"marker", "BDT60"},
                {"id", Self->GetLogId()},
                {"key", key},
                {"reason", reason},
                {"outcome", outcomeToString()},
                {"underSoft", underSoft},
                {"inserted", inserted},
                {"value", value},
                {"uncertainWriteBefore", uncertainWriteBefore});

            auto returnRefCount = [&](auto& map, auto&& key, auto& deleteQ) {
                const auto [it, inserted] = map.try_emplace(std::move(key));
                if (outcome != EUpdateOutcome::DROP) {
                    ++it->second;
                } else if (inserted) {
                    deleteQ.push_back(it->first);
                }
                if (inserted) {
                    AddFirstMentionedBlob(it->first);
                }
                return inserted;
            };

            const auto accountLocalBlob = [&](TLogoBlobID id, ui32, ui32) {
                if (returnRefCount(RefCountBlobs, id, deleteBlobs)) {
                    auto makeValueChain = [&] {
                        TStringStream s;
                        s << '{';
                        for (bool first = true; const auto& item : value.ValueChain) {
                            s << (std::exchange(first, false) ? "" : " ") << SingleLineProto(item);
                        }
                        s << '}';
                        return s.Str();
                    };
                    Y_VERIFY_S(!CanBeCollected(TBlobSeqId::FromLogoBlobId(id)), "BlobId# " << id
                        << " ValueChain# " << makeValueChain());
                    Y_VERIFY_DEBUG_S(id.Generation() == generation, "BlobId# " << id << " Generation# " << generation);
                    Y_VERIFY_DEBUG_S(Self->Channels[id.Channel()].GetLeastExpectedBlobId(generation) <= TBlobSeqId::FromLogoBlobId(id),
                        "LeastExpectedBlobId# " << Self->Channels[id.Channel()].GetLeastExpectedBlobId(generation)
                        << " Id# " << id
                        << " Generation# " << generation);
                }
            };
            const auto s3Action = outcome != EUpdateOutcome::DROP ? ES3ReferenceAction::Add : ES3ReferenceAction::Discard;
            hasS3References |= AccountValueChain(value.ValueChain, s3Action, accountLocalBlob, &deleteS3);

            NIceDb::TNiceDb db(txc.DB);

            auto filterBlobs = [&](TLogoBlobID id) {
                const auto it = RefCountBlobs.find(id);
                Y_ABORT_UNLESS(it != RefCountBlobs.end());
                if (it->second) {
                    return true;
                }
                RefCountBlobs.erase(it);

                const size_t size = id.BlobSize();
                InFlightTrashSize += size;
                Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_IN_FLIGHT_TRASH_SIZE] = InFlightTrashSize;
                TotalStoredDataSize -= size;
                Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_DATA_SIZE] = TotalStoredDataSize;

                InFlightTrashBlobs.emplace(cookie, id);
                const bool inserted = AllInFlightTrashBlobs.insert(id).second;
                Y_ABORT_UNLESS(inserted);
                db.Table<Schema::Trash>().Key(id.AsBinaryString()).Update();
                return false; // keep this blob in deletion queue
            };

            auto filterS3 = [&](const TS3ObjectKey& objectKey) {
                const auto it = RefCountS3.find(objectKey);
                Y_ABORT_UNLESS(it != RefCountS3.end());
                if (it->second.RefCount) {
                    return true;
                }

                const auto locator = it->second.GetLocator(objectKey);
                RefCountS3.erase(it);
                TotalS3DataSize -= locator.Len;
                if (!RetriedS3PutsLoaded || (!S3ReferencesLoaded && Self->Config.GetEverAuthorizedDataCooling())) {
                    // Recovery may still find a retried PUT or another owner of this pack.
                    // Record the candidate in the same transaction as the mapping change.
                    if (PendingS3RefChecks.emplace(objectKey, locator.Len).second) {
                        db.Table<Schema::TrashS3>().Key(objectKey.first, objectKey.second).Update(
                            NIceDb::TUpdate<Schema::TrashS3::Len>(locator.Len),
                            NIceDb::TUpdate<Schema::TrashS3::CheckReferences>(true));
                    }
                    return true;
                }

                YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "Delete_S3",
                    {"marker", "BDEV27"},
                    {"BDT", Self->TabletID()},
                    {"key", key},
                    {"locator", locator});
                AddToS3Trash(locator, txc, cookie);
                return false; // keep this blob in deletion queue
            };

            auto process = [&](auto& deleteQ, auto&& filterFunc) {
                std::ranges::sort(deleteQ);
                if (auto [first, last] = std::ranges::unique(deleteQ); first != last) {
                    deleteQ.erase(first, last);
                }
                if (auto [first, last] = std::ranges::remove_if(deleteQ, filterFunc); first != last) {
                    deleteQ.erase(first, last);
                }
            };

            process(deleteBlobs, filterBlobs);
            process(deleteS3, filterS3);

            if (!deleteBlobs.empty()) {
                UncertaintyResolver->DropBlobs(deleteBlobs);
            }

            if (hasS3References) {
                UpdateS3Counters();
            }
            const bool mappingChanged = outcome == EUpdateOutcome::DROP || value.ValueVersion != valueVersionBefore;
            if (Self->CoolingIndex) {
                if (mappingChanged) {
                    Self->CoolingIndex->UpdateSourceRetry(key, outcome == EUpdateOutcome::DROP ? nullptr : &value);
                }
                if (inserted || mappingChanged ||
                        oldHotGeneration != value.HotWriteGeneration || oldHotSequence != value.HotWriteSequence) {
                    if (!inserted) {
                        Self->CoolingIndex->RemoveHotKey(key, oldHotGeneration, oldHotSequence);
                    }
                    if (outcome != EUpdateOutcome::DROP) {
                        Self->CoolingIndex->AddHotKey(key, value);
                    }
                }
            }

            if (outcome != EUpdateOutcome::NO_CHANGE &&
                    Self->MoveData.IsBlobMovingInProgress() &&
                    !Self->MoveData.ApplyingIndexUpdate) {
                const TString binaryKey = key.MakeBinaryKey();
                if (Self->MoveData.Key && *Self->MoveData.Key == binaryKey &&
                        Self->MoveData.BlobLocator.HasBlobSeqId() && mappingChanged) {
                    Self->MoveData.RecordTouched = true;
                }
                if (outcome != EUpdateOutcome::DROP && !Self->MoveData.NeedsAnotherPass &&
                        (inserted || value.ValueVersion != valueVersionBefore)) {
                    for (const auto& item : value.ValueChain) {
                        if (item.HasBlobLocator() && Self->NeedMoveBlob(item.GetBlobLocator())) {
                            Self->MoveData.NeedsAnotherPass = true;
                            break;
                        }
                    }
                }
            }

            auto row = NIceDb::TNiceDb(txc.DB).Table<Schema::Data>().Key(key.MakeBinaryKey());
            switch (outcome) {
                case EUpdateOutcome::DROP:
                    if (wasGoingToAssimilate) {
                        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_BYTES_TO_DECOMMIT] -= key.GetBlobId().BlobSize();
                    }
                    UncertaintyResolver->DropKey(key);
                    Data.erase(it);
                    row.Delete();
                    ValidateRecords();
                    return true;

                case EUpdateOutcome::CHANGE:
                    {
                        auto makeLocators = [&] {
                            std::vector<TS3Locator> locators;
                            EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), TOverloaded{
                                [&](TLogoBlobID, ui32, ui32) {},
                                [&](TS3Locator locator) { locators.push_back(locator); }
                            });
                            return locators;
                        };
                        YDB_LOG_TRACE_COMP(BLOB_DEPOT_EVENTS, "Change_key",
                            {"marker", "BDEV28"},
                            {"BDT", Self->TabletID()},
                            {"key", key},
                            {"locators", makeLocators()});
                    }
                    row.template Update<Schema::Data::Value>(value.SerializeToString());
                    if (inserted || uncertainWriteBefore != value.UncertainWrite) {
                        if (value.UncertainWrite) {
                            row.template Update<Schema::Data::UncertainWrite>(true);
                        } else if (!inserted) {
                            row.template UpdateToNull<Schema::Data::UncertainWrite>();
                        }
                    }
                    if (wasUncertain && !value.IsWrittenUncertainly()) {
                        UncertaintyResolver->MakeKeyCertain(key);
                    }
                    if (wasGoingToAssimilate != value.GoingToAssimilate) {
                        const i64 sign = value.GoingToAssimilate - wasGoingToAssimilate;
                        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_BYTES_TO_DECOMMIT] += key.GetBlobId().BlobSize() * sign;
                    }
                    ValidateRecords();
                    return true;

                case EUpdateOutcome::NO_CHANGE:
                    ValidateRecords();
                    return false;
            }
        }
    }

    const TData::TValue *TData::FindKey(const TKey& key) const {
        Y_ABORT_UNLESS(IsKeyLoaded(key));
        const auto it = Data.find(key);
        return it != Data.end() ? &it->second : nullptr;
    }

    void TData::SetHotWriteOrder(TValue& value) {
        if (Self->HasAsyncS3History()) {
            value.HotWriteGeneration = Self->Executor()->Generation();
            value.HotWriteSequence = NextHotWriteSequence++;
        }
    }

    void TData::UpdateKey(const TKey& key, const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        YDB_LOG_DEBUG("UpdateKey",
            {"marker", "BDT10"},
            {"id", Self->GetLogId()},
            {"key", key},
            {"item", item});
        Y_ABORT_UNLESS(IsKeyLoaded(key));
        UpdateKey(key, txc, cookie, "UpdateKey", [&](TValue& value, bool inserted) {
            if (!inserted) { // update value items
                value.UpdateFrom(item);
                ++value.ValueVersion;

                // clear assimilation flag -- we have blob overwritten with fresh copy (of the same data)
                value.GoingToAssimilate = false;
            }

            value.HotWriteGeneration = 0;
            value.HotWriteSequence = 0;
            if (item.HasBlobLocator()) {
                SetHotWriteOrder(value);
            }

            return EUpdateOutcome::CHANGE;
        }, item);
    }

    bool TData::ReplaceValueForCooling(const TKey& key, ui32 expectedVersion, const TValueChain& expectedChain,
            TValueChain replacement, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        const TValue *value = FindKey(key);
        if (!value || value->GoingToAssimilate || value->IsWrittenUncertainly() || value->ValueVersion != expectedVersion ||
                !IsSameValueChain(value->ValueChain, expectedChain) || CheckKeyAgainstBarrier(key)) {
            return false;
        }

        return UpdateKey(key, txc, cookie, "ReplaceValueForCooling", [&](TValue& value, bool inserted) {
            Y_ABORT_UNLESS(!inserted && !replacement.empty());
            value.ValueChain = std::move(replacement);
            ++value.ValueVersion;
            value.HotWriteGeneration = 0;
            value.HotWriteSequence = 0;
            // A recent in-memory confirmation may still have a pending UncertainWrite column update.
            NIceDb::TNiceDb(txc.DB).Table<Schema::Data>().Key(key.MakeBinaryKey()).UpdateToNull<Schema::Data::UncertainWrite>();
            return EUpdateOutcome::CHANGE;
        });
    }

    TData::EMoveDataReplaceResult TData::ReplaceLocatorForMoveData(const TKey& key, ui32 valueChainIndex,
            ui32 expectedValueVersion, const NKikimrBlobDepot::TBlobLocator& oldLocator,
            const NKikimrBlobDepot::TBlobLocator& newLocator,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        Y_ABORT_UNLESS(IsKeyLoaded(key));

        YDB_LOG_DEBUG("ReplaceLocatorForMoveData",
            {"marker", "BDT11"},
            {"id", Self->GetLogId()},
            {"key", key},
            {"valueChainIndex", valueChainIndex},
            {"expectedValueVersion", expectedValueVersion});

        const TValue *value = FindKey(key);
        if (!value) {
            return EMoveDataReplaceResult::KeyMissing;
        }

        auto locatorEquals = [](const NKikimrBlobDepot::TBlobLocator& left,
                const NKikimrBlobDepot::TBlobLocator& right) {
            TString leftSerialized;
            TString rightSerialized;
            Y_ABORT_UNLESS(left.SerializeToString(&leftSerialized));
            Y_ABORT_UNLESS(right.SerializeToString(&rightSerialized));
            return leftSerialized == rightSerialized;
        };

        if (value->ValueVersion != expectedValueVersion ||
                valueChainIndex >= static_cast<ui32>(value->ValueChain.size()) ||
                !value->ValueChain[valueChainIndex].HasBlobLocator() ||
                !locatorEquals(value->ValueChain[valueChainIndex].GetBlobLocator(), oldLocator)) {
            return EMoveDataReplaceResult::KeyChanged;
        }

        Y_ABORT_UNLESS(RefCountBlobs.contains(Self->MoveData.BlobId));
        bool multipleRefs = RefCountBlobs[Self->MoveData.BlobId] > 1;

        Self->MoveData.ApplyingIndexUpdate = true;

        const bool changed = UpdateKey(key, txc, cookie, "ReplaceLocatorForMoveData",
            [&](TValue& mutableValue, bool inserted) {
                Y_ABORT_UNLESS(!inserted);
                Y_ABORT_UNLESS(mutableValue.ValueVersion == expectedValueVersion);
                Y_ABORT_UNLESS(valueChainIndex < static_cast<ui32>(mutableValue.ValueChain.size()));
                auto& item = mutableValue.ValueChain[valueChainIndex];
                Y_ABORT_UNLESS(item.HasBlobLocator());
                Y_ABORT_UNLESS(locatorEquals(item.GetBlobLocator(), oldLocator));
                item.MutableBlobLocator()->CopyFrom(newLocator);
                ++mutableValue.ValueVersion;
                return EUpdateOutcome::CHANGE;
            });
        Y_ABORT_UNLESS(changed);

        Self->MoveData.ApplyingIndexUpdate = false;

        if (multipleRefs) {
            Self->MoveData.BlobIdToNewLocator.try_emplace(Self->MoveData.BlobId, newLocator);
        }

        return EMoveDataReplaceResult::Replaced;
    }

    bool TData::IsBlobReferenced(TLogoBlobID id) const {
        return RefCountBlobs.contains(id);
    }

    std::unordered_set<std::tuple<ui8, ui32>> TData::PrepareCheckTrash(const THashSet<ui32>& groups) {
        std::unordered_set<std::tuple<ui8, ui32>> channelGroups;
        for (const auto& [key, _] : RecordsPerChannelGroup) {
            const auto groupId = std::get<1>(key);
            if (groups.contains(groupId)) {
                channelGroups.insert(key);
            }
        }
        return channelGroups;
    }

    void TData::BindToBlob(const TKey& key, TBlobSeqId blobSeqId, bool keep, bool doNotKeep, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        YDB_LOG_DEBUG("BindToBlob",
            {"marker", "BDT49"},
            {"id", Self->GetLogId()},
            {"key", key},
            {"blobSeqId", blobSeqId},
            {"keep", keep},
            {"doNotKeep", doNotKeep});
        Y_ABORT_UNLESS(IsKeyLoaded(key));
        UpdateKey(key, txc, cookie, "BindToBlob", [&](TValue& value, bool /*inserted*/) {
            EUpdateOutcome outcome = EUpdateOutcome::NO_CHANGE;
            if (doNotKeep && value.KeepState < EKeepState::DoNotKeep) {
                value.KeepState = EKeepState::DoNotKeep;
                outcome = EUpdateOutcome::CHANGE;
            } else if (keep && value.KeepState < EKeepState::Keep) {
                value.KeepState = EKeepState::Keep;
                outcome = EUpdateOutcome::CHANGE;
            }
            if (value.ValueChain.empty()) {
                auto *chain = value.ValueChain.Add();
                auto *locator = chain->MutableBlobLocator();
                locator->SetGroupId(Self->Info()->GroupFor(blobSeqId.Channel, blobSeqId.Generation));
                blobSeqId.ToProto(locator->MutableBlobSeqId());
                locator->SetTotalDataLen(key.GetBlobId().BlobSize());
                locator->SetFooterLen(0);
                value.GoingToAssimilate = false;
                ++value.ValueVersion;
                SetHotWriteOrder(value);
                outcome = EUpdateOutcome::CHANGE;
            }
            return outcome;
        });
    }

    void TData::TrackPublishedRetriedPut(TS3Locator locator, TInstant retireAt) {
        const bool inserted = PublishedRetriedS3Puts.emplace(locator.GetObjectKey(), retireAt).second;
        Y_ABORT_UNLESS(inserted);
    }

    void TData::FinishS3Put(TS3Locator locator, ES3PutOutcome outcome, bool published,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        NIceDb::TNiceDb db(txc.DB);
        auto stage = db.Table<Schema::CoolingS3Puts>().Key(locator.Generation, locator.KeyId);
        // Dispatch can outlast the original staging window. Cover late attempts from this PUT.
        const TInstant retireAt = outcome == ES3PutOutcome::MayArriveLate ? Self->S3Manager->GetPutWatchRetireAt() : TInstant();
        if (published && retireAt) {
            stage.Update(NIceDb::TUpdate<Schema::CoolingS3Puts::Len>(locator.Len),
                NIceDb::TUpdate<Schema::CoolingS3Puts::RetireAt>(retireAt),
                NIceDb::TUpdate<Schema::CoolingS3Puts::Published>(true));
            TrackPublishedRetriedPut(locator, retireAt);
        } else if (!retireAt) {
            stage.Delete();
        }
        if (!published && outcome != ES3PutOutcome::NotStored) {
            AddToS3Trash(locator, txc, cookie, retireAt);
        }
    }

    void TData::DiscardAgentS3Puts(std::vector<TS3Locator> locators, ES3PutOutcome outcome) {
        if (locators.empty()) {
            return;
        }
        Self->Execute(std::make_unique<TCoroTx>(Self, TTokens{{Self->Token}},
            [this, locators = std::move(locators), outcome](TCoroTx::TContextBase& tx) {
                NIceDb::TNiceDb db(tx.GetTxc()->DB);
                for (const auto& locator : locators) {
                    if (outcome == ES3PutOutcome::NotStored) {
                        db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId).Delete();
                    } else {
                        const TInstant retireAt = outcome == ES3PutOutcome::MayArriveLate
                            ? Self->S3Manager->GetPutWatchRetireAt() : TInstant();
                        AddToS3Trash(locator, *tx, &tx, retireAt);
                    }
                }
                tx.FinishTx();
                CommitTrash(&tx);
            }));
    }

    void TData::AddToS3Trash(TS3Locator locator, NTabletFlatExecutor::TTransactionContext& txc, void *cookie,
            TInstant retireAt) {
        NIceDb::TNiceDb db(txc.DB);
        auto trash = db.Table<Schema::TrashS3>().Key(locator.Generation, locator.KeyId);
        if (auto it = PublishedRetriedS3Puts.find(locator.GetObjectKey()); it != PublishedRetriedS3Puts.end()) {
            retireAt = it->second;
            PublishedRetriedS3Puts.erase(it);
        }
        if (retireAt) {
            Self->S3Manager->EnqueuePutWatch(locator, retireAt, txc);
        }
        trash.Update(NIceDb::TUpdate<Schema::TrashS3::Len>(locator.Len),
            NIceDb::TUpdate<Schema::TrashS3::CheckReferences>(false),
            NIceDb::TUpdate<Schema::TrashS3::UnfinishedAgentPut>(false));
        InFlightTrashSize += locator.Len;
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_IN_FLIGHT_TRASH_SIZE] = InFlightTrashSize;
        InFlightTrashS3.emplace(cookie, TS3TrashItem{locator, bool(retireAt)});
    }

    void TData::MakeKeyCertain(const TKey& key) {
        const auto it = Data.find(key);
        Y_ABORT_UNLESS(it != Data.end());
        TValue& value = it->second;
        value.UncertainWrite = false;
        KeysMadeCertain.push_back(key);
        if (!CommitCertainKeysScheduled) {
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvCommitCertainKeys, 0,
                Self->SelfId(), {}, nullptr, 0));
            CommitCertainKeysScheduled = true;
        }

        // do this in the end as the 'key' reference may perish
        UncertaintyResolver->MakeKeyCertain(key);
    }

    void TData::HandleCommitCertainKeys() {
        class TTxCommitCertainKeys : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::deque<TKey> KeysMadeCertain;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_COMMIT_CERTAIN_KEYS; }

            TTxCommitCertainKeys(TBlobDepot *self, std::deque<TKey> keysMadeCertain)
                : TTransactionBase(self)
                , KeysMadeCertain(std::move(keysMadeCertain))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                NIceDb::TNiceDb db(txc.DB);
                for (const TKey& key : KeysMadeCertain) {
                    if (const TValue *value = Self->Data->FindKey(key); value && !value->UncertainWrite) {
                        db.Table<Schema::Data>().Key(key.MakeBinaryKey()).UpdateToNull<Schema::Data::UncertainWrite>();
                    }
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->S3Manager->MaybeScheduleCooling();
                if (Self->Data->KeysMadeCertain.empty()) {
                    Self->Data->CommitCertainKeysScheduled = false;
                } else {
                    TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvCommitCertainKeys,
                        0, Self->SelfId(), {}, nullptr, 0));
                }
            }
        };

        Self->Execute(std::make_unique<TTxCommitCertainKeys>(Self, std::exchange(KeysMadeCertain, {})));
    }

    TData::TRecordsPerChannelGroup& TData::GetRecordsPerChannelGroup(TLogoBlobID id) {
        TTabletStorageInfo *info = Self->Info();
        const ui32 groupId = info->GroupFor(id.Channel(), id.Generation());
        Y_ABORT_UNLESS(groupId != Max<ui32>());
        Y_ABORT_UNLESS(id.TabletID() == info->TabletID);
        const auto& key = std::make_tuple(id.Channel(), groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        return it->second;
    }

    TData::TRecordsPerChannelGroup& TData::GetRecordsPerChannelGroup(ui8 channel, ui32 groupId) {
        const auto it = RecordsPerChannelGroup.find(std::make_tuple(channel, groupId));
        Y_ABORT_UNLESS(it != RecordsPerChannelGroup.end());
        return it->second;
    }

    TData::TValue *TData::AddDataOnLoad(TKey key, TString value, bool uncertainWrite) {
        Y_VERIFY_S(!IsKeyLoaded(key), "Id# " << Self->GetLogId() << " Key# " << key.ToString());

        NKikimrBlobDepot::TValue proto;
        const bool success = proto.ParseFromString(value);
        Y_ABORT_UNLESS(success);

        YDB_LOG_DEBUG("AddDataOnLoad",
            {"marker", "BDT79"},
            {"id", Self->GetLogId()},
            {"key", key},
            {"value", proto});

        // we can only add key that is not loaded before; if key exists, it MUST have been loaded from the dataset
        const auto [it, inserted] = Data.try_emplace(std::move(key), std::move(proto), uncertainWrite);
        Y_ABORT_UNLESS(inserted);

        auto addRefCount = [&](auto& map, auto&& key) {
            if (!map[key]++) {
                AddFirstMentionedBlob(key);
            }
        };
        if (AccountValueChain(it->second.ValueChain, ES3ReferenceAction::Add,
                [&](TLogoBlobID id, ui32, ui32) { addRefCount(RefCountBlobs, id); })) {
            UpdateS3Counters();
        }
        if (Self->CoolingIndex) {
            Self->CoolingIndex->AddHotKey(it->first, it->second);
        }
        if (it->second.GoingToAssimilate) {
            Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_BYTES_TO_DECOMMIT] += it->first.GetBlobId().BlobSize();
        }

        ValidateRecords();

        return &it->second;
    }

    bool TData::AddDataOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlob& blob,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        Y_VERIFY_S(IsKeyLoaded(TKey(blob.Id)), "Id# " << Self->GetLogId() << " Blob# " << blob.ToString());

        return UpdateKey(TKey(blob.Id), txc, cookie, "AddDataOnDecommit", [&](TValue& value, bool inserted) {
            bool change = inserted;

            // update keep state if necessary
            if (blob.DoNotKeep && value.KeepState < EKeepState::DoNotKeep) {
                value.KeepState = EKeepState::DoNotKeep;
                change = true;
            } else if (blob.Keep && value.KeepState < EKeepState::Keep) {
                value.KeepState = EKeepState::Keep;
                change = true;
            }

            if (value.ValueChain.empty() && !value.GoingToAssimilate) {
                value.GoingToAssimilate = true;
                change = true;
            }

            return change ? EUpdateOutcome::CHANGE : EUpdateOutcome::NO_CHANGE;
        });
    }

    void TData::AddTrashOnLoad(TLogoBlobID id) {
        if (AllInFlightTrashBlobs.contains(id)) {
            return; // we're trying to add just inserted item, ignore it
        }
        auto& record = GetRecordsPerChannelGroup(id);
        if (const auto [it, inserted] = record.Trash.insert(id); !inserted) {
            return; // the same situation: this item has just been inserted into the set
        }
        ++LoadedTrashRecords;
        AccountBlob(id, true);
        TotalStoredTrashSize += id.BlobSize();
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_TRASH_SIZE] = TotalStoredTrashSize;
    }

    void TData::AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep) {
        const auto& key = std::make_tuple(channel, groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        auto& record = it->second;
        record.IssuedGenStep = issuedGenStep;
        record.LastConfirmedGenStep = confirmedGenStep;
    }

    bool TData::UpdateKeepState(TKey key, EKeepState keepState, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        Y_ABORT_UNLESS(IsKeyLoaded(key));
        return UpdateKey(std::move(key), txc, cookie, "UpdateKeepState", [&](TValue& value, bool inserted) {
             YDB_LOG_DEBUG("UpdateKeepState",
                 {"marker", "BDT51"},
                 {"id", Self->GetLogId()},
                 {"key", key},
                 {"keepState", keepState},
                 {"value", value});
             if (inserted) {
                return EUpdateOutcome::CHANGE;
             } else if (value.KeepState < keepState) {
                value.KeepState = keepState;
                return EUpdateOutcome::CHANGE;
             } else {
                return EUpdateOutcome::NO_CHANGE;
             }
        }, keepState);
    }

    void TData::DeleteKey(const TKey& key, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        YDB_LOG_DEBUG("DeleteKey",
            {"marker", "BDT14"},
            {"id", Self->GetLogId()},
            {"key", key});
        UpdateKey(key, txc, cookie, "DeleteKey", [&](TValue&, bool inserted) {
            Y_ABORT_UNLESS(!inserted);
            return EUpdateOutcome::DROP;
        });
    }

    void TData::OnPushNotifyResult(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
        TAgent& agent = Self->GetAgent(ev->Recipient);

        const auto it = agent.InvalidateStepRequests.find(ev->Get()->Record.GetId());
        Y_ABORT_UNLESS(it != agent.InvalidateStepRequests.end());
        auto items = std::move(it->second);
        agent.InvalidateStepRequests.erase(it);

        const ui32 generation = Self->Executor()->Generation();

        std::vector<TBlobSeqId> writesInFlight;
        for (const auto& item : ev->Get()->Record.GetWritesInFlight()) {
            writesInFlight.push_back(TBlobSeqId::FromProto(item));
        }
        std::sort(writesInFlight.begin(), writesInFlight.end());

        for (const auto& [channelIndex, invalidatedStep] : items) {
            auto& channel = Self->Channels[channelIndex];
            auto& agentGivenIdRanges = agent.GivenIdRanges[channelIndex];
            auto& givenIdRanges = channel.GivenIdRanges;

            auto begin = std::lower_bound(writesInFlight.begin(), writesInFlight.end(), TBlobSeqId{channelIndex, 0, 0, 0});

            auto makeWritesInFlight = [&] {
                TStringStream s;
                s << "[";
                for (auto it = begin; it != writesInFlight.end() && it->Channel == channel.Index; ++it) {
                    s << (it != begin ? " " : "") << it->ToString();
                }
                s << "]";
                return s.Str();
            };

            YDB_LOG_DEBUG("Trim",
                {"marker", "BDT13"},
                {"id", Self->GetLogId()},
                {"agentId", agent.Connection->NodeId},
                {"cookie", ev->Cookie},
                {"channel", int(channelIndex)},
                {"invalidatedStep", invalidatedStep},
                {"givenIdRanges", channel.GivenIdRanges},
                {"Agent.GivenIdRanges", agent.GivenIdRanges[channelIndex]},
                {"writesInFlight", makeWritesInFlight()});

            // sanity check -- ensure that current writes in flight would be conserved when processing garbage
            for (auto it = begin; it != writesInFlight.end() && it->Channel == channelIndex; ++it) {
                Y_VERIFY_S(agentGivenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
                Y_VERIFY_S(givenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
            }

            const TBlobSeqId leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);

            const TBlobSeqId trimmedBlobSeqId{channelIndex, generation, invalidatedStep, TBlobSeqId::MaxIndex};
            const ui64 validSince = trimmedBlobSeqId.ToSequentialNumber() + 1;
            givenIdRanges.Subtract(agentGivenIdRanges.Trim(validSince));

            for (auto it = begin; it != writesInFlight.end() && it->Channel == channelIndex; ++it) {
                agentGivenIdRanges.AddPoint(it->ToSequentialNumber());
                givenIdRanges.AddPoint(it->ToSequentialNumber());
            }

            if (channel.GetLeastExpectedBlobId(generation) != leastExpectedBlobIdBefore) {
                OnLeastExpectedBlobIdChange(channelIndex);
            }
        }
    }

    bool TData::OnBarrierShift(ui64 tabletId, ui8 channel, bool hard, TGenStep previous, TGenStep current, ui32& maxItems,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        YDB_LOG_DEBUG("OnBarrierShift",
            {"marker", "BDT18"},
            {"id", Self->GetLogId()},
            {"tabletId", tabletId},
            {"channel", int(channel)},
            {"hard", hard},
            {"previous", previous},
            {"current", current},
            {"maxItems", maxItems});

        Y_ABORT_UNLESS(Loaded);

        const TData::TKey first(TLogoBlobID(tabletId, previous.Generation(), previous.Step(), channel, 0, 0));
        const TData::TKey last(TLogoBlobID(tabletId, current.Generation(), current.Step(), channel,
            TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));

        // find keys we have to delete
        bool finished = true;
        TScanRange r{first, last, TData::EScanFlags::INCLUDE_END};
        std::vector<TKey> keysToDelete;
        Self->Data->ScanRange(r, nullptr, nullptr, [&](auto& key, auto& value) {
            if (value.KeepState != EKeepState::Keep || hard) {
                if (maxItems) {
                    keysToDelete.push_back(key);
                    --maxItems;
                } else {
                    finished = false;
                    return false;
                }
            }
            return true;
        });

        // delete selected keys
        for (const TKey& key : keysToDelete) {
            DeleteKey(key, txc, cookie);
        }

        return finished;
    }

    bool TData::OnTabletDeleted(ui64 tabletId, ui32& maxItems, NTabletFlatExecutor::TTransactionContext& txc,
            void *cookie) {
        YDB_LOG_DEBUG("OnTabletDeleted",
            {"marker", "BDT85"},
            {"id", Self->GetLogId()},
            {"tabletId", tabletId},
            {"maxItems", maxItems});

        Y_ABORT_UNLESS(Loaded);

        // the whole tablet is gone, so this covers all of its channels in a single scan
        const TData::TKey first(TLogoBlobID(tabletId, 0, 0, 0, 0, 0));
        const TData::TKey last(TLogoBlobID(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
            TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));

        bool finished = true;
        TScanRange r{first, last, TData::EScanFlags::INCLUDE_BEGIN | TData::EScanFlags::INCLUDE_END};
        std::vector<TKey> keysToDelete;
        ScanRange(r, nullptr, nullptr, [&](auto& key, auto& /*value*/) {
            // a complete deletion is a hard barrier, so Keep flags do not save anything here
            if (maxItems) {
                keysToDelete.push_back(key);
                --maxItems;
            } else {
                finished = false;
                return false;
            }
            return true;
        });

        for (const TKey& key : keysToDelete) {
            DeleteKey(key, txc, cookie);
        }

        return finished;
    }

    void TData::AddFirstMentionedBlob(TLogoBlobID id) {
        YDB_LOG_DEBUG("AddFirstMentionedBlob",
            {"marker", "BDT80"},
            {"id", Self->GetLogId()},
            {"blobId", id});
        auto& record = GetRecordsPerChannelGroup(id);
        const auto [_, inserted] = record.Used.insert(id);
        Y_ABORT_UNLESS(inserted);
        AccountBlob(id, true);
        TotalStoredDataSize += id.BlobSize();
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_DATA_SIZE] = TotalStoredDataSize;
    }

    void TData::UpdateS3Reference(const NKikimrBlobDepot::TValueChain& link, TS3Locator locator,
            ES3ReferenceAction action, std::vector<TS3ObjectKey>* unreferenced) {
        const auto objectKey = locator.GetObjectKey();
        const auto [it, inserted] = RefCountS3.try_emplace(objectKey, TS3References{.Len = locator.Len});
        auto& object = it->second;
        Y_ABORT_UNLESS(object.Len == locator.Len);
        if (inserted) {
            Y_ABORT_UNLESS(action != ES3ReferenceAction::Remove);
            TotalS3DataSize += locator.Len;
        }

        const ui64 previousLiveBytes = object.GetLiveBytes();
        if (action == ES3ReferenceAction::Add) {
            ++object.RefCount;
        } else if (action == ES3ReferenceAction::Remove) {
            Y_ABORT_UNLESS(object.RefCount);
            --object.RefCount;
        }

        const ui64 begin = link.GetSubrangeBegin();
        const ui64 end = link.HasSubrangeEnd() ? link.GetSubrangeEnd() : object.Len;
        if (action != ES3ReferenceAction::Discard && begin < end && end <= object.Len && (begin || end < object.Len)) {
            const std::pair<ui32, ui32> range{static_cast<ui32>(begin), static_cast<ui32>(end)};
            if (action == ES3ReferenceAction::Add) {
                if (!object.Partial) {
                    object.Partial = std::make_unique<TS3PartialReferences>();
                }
                auto& partial = *object.Partial;
                ++partial.RefCount;
                // Pack ranges are disjoint. Identical references share the same bytes.
                if (!partial.Ranges[range]++) {
                    partial.LiveBytes += end - begin;
                }
            } else {
                Y_ABORT_UNLESS(object.Partial);
                auto& partial = *object.Partial;
                const auto rangeIt = partial.Ranges.find(range);
                Y_ABORT_UNLESS(rangeIt != partial.Ranges.end());
                if (!--rangeIt->second) {
                    partial.Ranges.erase(rangeIt);
                    partial.LiveBytes -= end - begin;
                }
                if (!--partial.RefCount) {
                    object.Partial.reset();
                }
            }
        }

        const ui64 liveBytes = object.GetLiveBytes();
        TotalS3LiveBytes = TotalS3LiveBytes - previousLiveBytes + liveBytes;
        if (!object.RefCount) {
            Y_ABORT_UNLESS(unreferenced);
            unreferenced->push_back(objectKey);
        }
    }

    void TData::UpdateS3Counters() {
        auto& counters = Self->TabletCounters->Simple();
        counters[NKikimrBlobDepot::COUNTER_TOTAL_S3_DATA_OBJECTS] = RefCountS3.size();
        counters[NKikimrBlobDepot::COUNTER_TOTAL_S3_DATA_SIZE] = TotalS3DataSize;
        counters[NKikimrBlobDepot::COUNTER_S3_LIVE_RANGE_BYTES] = TotalS3LiveBytes;
    }

    bool TData::IsS3ObjectReferenced(const TS3Locator& locator) const {
        return RefCountS3.contains(locator.GetObjectKey());
    }

    void TData::AccountBlob(TLogoBlobID id, bool add) {
        // account record
        YDB_LOG_DEBUG("AccountBlob",
            {"marker", "BDT81"},
            {"id", Self->GetLogId()},
            {"blobId", id},
            {"add", add});
        const ui32 groupId = Self->Info()->GroupFor(id.Channel(), id.Generation());
        auto& groupStat = Self->Groups[groupId];
        if (add) {
            groupStat.AllocatedBytes += id.BlobSize();
        } else {
            groupStat.AllocatedBytes -= id.BlobSize();
        }
    }

    bool TData::CanBeCollected(TBlobSeqId id) const {
        const ui32 groupId = Self->Info()->GroupFor(id.Channel, id.Generation);
        const auto it = RecordsPerChannelGroup.find(std::make_tuple(id.Channel, groupId));
        return it != RecordsPerChannelGroup.end() && TGenStep(id) <= Min(it->second.IssuedGenStep, it->second.HardGenStep);
    }

    void TData::OnLeastExpectedBlobIdChange(ui8 channel) {
        const TTabletChannelInfo *storageChannel = Self->Info()->ChannelInfo(channel);
        Y_ABORT_UNLESS(storageChannel);
        for (const auto& entry : storageChannel->History) {
            const auto& key = std::make_tuple(storageChannel->Channel, entry.GroupID);
            auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
            it->second.OnLeastExpectedBlobIdChange(this);
        }
    }

    void TData::TRecordsPerChannelGroup::MoveToTrash(TData *self, TLogoBlobID id) {
        const auto usedIt = Used.find(id);
        Y_ABORT_UNLESS(usedIt != Used.end());
        const bool inserted = Trash.insert(Used.extract(usedIt)).inserted;
        Y_DEBUG_ABORT_UNLESS(inserted);
        ++self->LoadedTrashRecords;
        self->TotalStoredTrashSize += id.BlobSize();
        self->Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_TRASH_SIZE] = self->TotalStoredTrashSize;
    }

    void TData::TRecordsPerChannelGroup::OnSuccessfulCollect(TData *self) {
        auto it = Trash.begin();
        for (const TLogoBlobID& id : TrashInFlight) {
            for (; it != Trash.end() && *it < id; ++it) {}
            Y_ABORT_UNLESS(it != Trash.end() && *it == id);
            DeleteTrashRecord(self, it);
        }
        LastConfirmedGenStep = IssuedGenStep;
    }

    void TData::TRecordsPerChannelGroup::DeleteTrashRecord(TData *self, std::set<TLogoBlobID>::iterator& it) {
        self->AccountBlob(*it, false);
        Y_ABORT_UNLESS(self->LoadedTrashRecords);
        --self->LoadedTrashRecords;
        self->TotalStoredTrashSize -= it->BlobSize();
        self->Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_TRASH_SIZE] = self->TotalStoredTrashSize;
        it = Trash.erase(it);
    }

    void TData::TRecordsPerChannelGroup::OnLeastExpectedBlobIdChange(TData *self) {
        CollectIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::ClearInFlight(TData *self) {
        Y_ABORT_UNLESS(CollectGarbageRequestsInFlight);
        --CollectGarbageRequestsInFlight;
        CollectIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::CollectIfPossible(TData *self) {
        if (CollectGarbageRequestsInFlight || !self->Loaded) {
            return;
        }

        if (Trash.empty() && self->TrashLoadState == ETrashLoadState::NeedMore && self->IssueLoadTrashBatch()) {
            return;
        }

        if (Collectible(self)) {
            self->HandleTrash(*this);
        }
    }

    bool TData::TRecordsPerChannelGroup::Collectible(TData *self) {
        return !Trash.empty() || HardGenStep < GetHardGenStep(self) || !InitialCollectionComplete;
    }

    TGenStep TData::TRecordsPerChannelGroup::GetHardGenStep(TData *self) {
        auto& channel = self->Self->Channels[Channel];
        const ui32 generation = self->Self->Executor()->Generation();
        TBlobSeqId leastBlobSeqId = channel.GetLeastExpectedBlobId(generation);
        if (!Used.empty()) {
            leastBlobSeqId = Min(leastBlobSeqId, TBlobSeqId::FromLogoBlobId(*Used.begin()));
        }

        // ensure this blob seq id does not decrease
        Y_VERIFY_S(LastLeastBlobSeqId <= leastBlobSeqId, "LastLeastBlobSeqId# " << LastLeastBlobSeqId
            << " leastBlobSeqId# " << leastBlobSeqId
            << " GetLeastExpectedBlobId# " << channel.GetLeastExpectedBlobId(generation)
            << " Generation# " << generation
            << " Channel# " << int(Channel)
            << " GroupId# " << GroupId
            << " Used.begin# " << (Used.empty() ? "<none>" : TBlobSeqId::FromLogoBlobId(*Used.begin()).ToString())
            << " HardGenStep# " << HardGenStep);
        LastLeastBlobSeqId = leastBlobSeqId;

        return TGenStep(leastBlobSeqId).Previous();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    bool TData::BeginCommittingBlobSeqId(TAgent& agent, TBlobSeqId blobSeqId) {
        const ui32 generation = Self->Executor()->Generation();
        if (blobSeqId.Generation != generation) {
            return false;
        }

        Y_ABORT_UNLESS(blobSeqId.Channel < Self->Channels.size());
        auto& channel = Self->Channels[blobSeqId.Channel];

#ifndef NDEBUG
        const TBlobSeqId leastBefore = channel.GetLeastExpectedBlobId(generation);
#endif

        const ui64 value = blobSeqId.ToSequentialNumber();

        agent.GivenIdRanges[blobSeqId.Channel].RemovePoint(value);
        channel.GivenIdRanges.RemovePoint(value);

        const bool inserted = channel.SequenceNumbersInFlight.insert(value).second;
        Y_ABORT_UNLESS(inserted);

#ifndef NDEBUG
        // ensure least expected blob id didn't change
        Y_ABORT_UNLESS(leastBefore == channel.GetLeastExpectedBlobId(generation));
#endif

        return true;
    }

    void TData::EndCommittingBlobSeqId(TAgent& /*agent*/, TBlobSeqId blobSeqId) {
        Y_ABORT_UNLESS(blobSeqId.Channel < Self->Channels.size());
        auto& channel = Self->Channels[blobSeqId.Channel];

        const ui32 generation = Self->Executor()->Generation();
        const auto leastExpectedBlobIdBefore = channel.GetLeastExpectedBlobId(generation);

        const size_t numErased = channel.SequenceNumbersInFlight.erase(blobSeqId.ToSequentialNumber());

        if (numErased && channel.GetLeastExpectedBlobId(generation) != leastExpectedBlobIdBefore) {
            OnLeastExpectedBlobIdChange(blobSeqId.Channel);
        }
    }

    void TData::ValidateRecords() {
        return;

#ifndef NDEBUG
        auto now = TActivationContext::Monotonic();
        if (now < LastRecordsValidationTimestamp + TDuration::MilliSeconds(100)) {
            return;
        }
        LastRecordsValidationTimestamp = now;

        TTabletStorageInfo *info = Self->Info();
        THashMap<TLogoBlobID, ui32> refcountBlobs;
        THashMap<TS3ObjectKey, ui32> refcountS3;
        for (const auto& [key, value] : Data) {
            EnumerateBlobsForValueChain(value.ValueChain, info->TabletID, TOverloaded{
                [&](TLogoBlobID id, ui32, ui32) {
                    ++refcountBlobs[id];
                },
                [&](TS3Locator locator) {
                    ++refcountS3[locator.GetObjectKey()];
                }
            });
        }
        Y_ABORT_UNLESS(RefCountBlobs == refcountBlobs);
        Y_ABORT_UNLESS(RefCountS3.size() == refcountS3.size());
        for (const auto& [key, count] : refcountS3) {
            Y_ABORT_UNLESS(RefCountS3.at(key).RefCount == count);
        }

        for (const auto& [cookie, id] : InFlightTrashBlobs) {
            const bool inserted = refcountBlobs.try_emplace(id).second;
            Y_ABORT_UNLESS(inserted);
            Y_ABORT_UNLESS(AllInFlightTrashBlobs.contains(id));
        }

        for (const auto& [cookie, item] : InFlightTrashS3) {
            const auto& locator = item.Locator;
            const bool inserted = refcountS3.try_emplace(locator.GetObjectKey()).second;
            Y_ABORT_UNLESS(inserted);
        }

        THashSet<std::tuple<ui8, ui32, TLogoBlobID>> used;
        for (const auto& [id, count] : refcountBlobs) {
            const ui32 groupId = info->GroupFor(id.Channel(), id.Generation());
            used.emplace(id.Channel(), groupId, id);
        }

        for (const auto& [key, record] : RecordsPerChannelGroup) {
            for (const TLogoBlobID& id : record.Used) {
                const size_t numErased = used.erase(std::tuple_cat(key, std::make_tuple(id)));
                Y_ABORT_UNLESS(numErased == 1);
            }
        }
        Y_ABORT_UNLESS(used.empty());
#endif
    }

} // NKikimr::NBlobDepot

template<>
void Out<NKikimr::NBlobDepot::TBlobDepot::TData::TKey>(IOutputStream& s, const NKikimr::NBlobDepot::TBlobDepot::TData::TKey& x) {
    x.Output(s);
}

template<>
void Out<NKikimr::NBlobDepot::TBlobSeqId>(IOutputStream& s, const NKikimr::NBlobDepot::TBlobSeqId& x) {
    x.Output(s);
}

template<>
void Out<NKikimr::NBlobDepot::TGivenIdRange>(IOutputStream& s, const NKikimr::NBlobDepot::TGivenIdRange& x) {
    x.Output(s);
}
