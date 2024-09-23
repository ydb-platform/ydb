#include "data.h"
#include "data_uncertain.h"
#include "garbage_collection.h"

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

    bool TData::TValue::Validate(const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item) {
        if (!item.HasBlobLocator()) {
            return false;
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

        Y_DEBUG_ABORT_UNLESS(chain.HasLocator());
        const auto& locator1 = chain.GetLocator();
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

        return true;
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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT59, "UpdateKey: key under hard barrier, will not be created",
                (Id, Self->GetLogId()), (Key, key), (Reason, reason));
            return false; // no such key existed and will not be created as it hits the barrier
        }

        const auto [it, inserted] = Data.try_emplace(std::move(key), std::forward<TArgs>(args)...);
        {
            auto& [key, value] = *it;

            std::vector<TLogoBlobID> deleteQ;
            const bool uncertainWriteBefore = value.UncertainWrite;
            const bool wasUncertain = value.IsWrittenUncertainly();
            const bool wasGoingToAssimilate = value.GoingToAssimilate;

            const ui32 generation = Self->Executor()->Generation();

#ifndef NDEBUG
            TValue originalValue(value);
#endif

            if (!inserted) {
                EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                    const auto it = RefCount.find(id);
                    Y_ABORT_UNLESS(it != RefCount.end());
                    if (!--it->second) {
                        deleteQ.push_back(id);
                    }
                });
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
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT60, "UpdateKey", (Id, Self->GetLogId()), (Key, key), (Reason, reason),
                (Outcome, outcomeToString()), (UnderSoft, underSoft), (Inserted, inserted), (Value, value),
                (UncertainWriteBefore, uncertainWriteBefore));

            EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                const auto [it, inserted] = RefCount.try_emplace(id);
                if (inserted) {
                    Y_ABORT_UNLESS(!CanBeCollected(TBlobSeqId::FromLogoBlobId(id)));
                    Y_VERIFY_DEBUG_S(id.Generation() == generation, "BlobId# " << id << " Generation# " << generation);
                    Y_VERIFY_DEBUG_S(Self->Channels[id.Channel()].GetLeastExpectedBlobId(generation) <= TBlobSeqId::FromLogoBlobId(id),
                        "LeastExpectedBlobId# " << Self->Channels[id.Channel()].GetLeastExpectedBlobId(generation)
                        << " Id# " << id
                        << " Generation# " << generation);
                    AddFirstMentionedBlob(id);
                }
                if (outcome == EUpdateOutcome::DROP) {
                    if (inserted) {
                        deleteQ.push_back(id);
                    }
                } else {
                    ++it->second;
                }
            });

            auto filter = [&](const TLogoBlobID& id) {
                const auto it = RefCount.find(id);
                Y_ABORT_UNLESS(it != RefCount.end());
                if (it->second) {
                    return true; // remove this blob from deletion queue, it still has references
                } else {
                    InFlightTrash.emplace(cookie, id);
                    InFlightTrashSize += id.BlobSize();
                    Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_IN_FLIGHT_TRASH_SIZE] = InFlightTrashSize;
                    NIceDb::TNiceDb(txc.DB).Table<Schema::Trash>().Key(id.AsBinaryString()).Update();
                    RefCount.erase(it);
                    TotalStoredDataSize -= id.BlobSize();
                    Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_DATA_SIZE] = TotalStoredDataSize;
                    return false; // keep this blob in deletion queue
                }
            };
            std::sort(deleteQ.begin(), deleteQ.end());
            deleteQ.erase(std::unique(deleteQ.begin(), deleteQ.end()), deleteQ.end());
            deleteQ.erase(std::remove_if(deleteQ.begin(), deleteQ.end(), filter), deleteQ.end());
            if (!deleteQ.empty()) {
                UncertaintyResolver->DropBlobs(deleteQ);
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

    void TData::UpdateKey(const TKey& key, const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT10, "UpdateKey", (Id, Self->GetLogId()), (Key, key), (Item, item));
        Y_ABORT_UNLESS(IsKeyLoaded(key));
        UpdateKey(key, txc, cookie, "UpdateKey", [&](TValue& value, bool inserted) {
            if (!inserted) { // update value items
                value.Meta = item.GetMeta();
                value.Public = false;
                value.UncertainWrite = item.GetUncertainWrite();

                // update it to keep new blob locator
                value.ValueChain.Clear();
                auto *chain = value.ValueChain.Add();
                auto *locator = chain->MutableLocator();
                locator->CopyFrom(item.GetBlobLocator());
                ++value.ValueVersion;

                // clear assimilation flag -- we have blob overwritten with fresh copy (of the same data)
                value.GoingToAssimilate = false;
            }

            return EUpdateOutcome::CHANGE;
        }, item);
    }

    void TData::BindToBlob(const TKey& key, TBlobSeqId blobSeqId, bool keep, bool doNotKeep, NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT49, "BindToBlob", (Id, Self->GetLogId()), (Key, key), (BlobSeqId, blobSeqId),
            (Keep, keep), (DoNotKeep, doNotKeep));
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
                auto *locator = chain->MutableLocator();
                locator->SetGroupId(Self->Info()->GroupFor(blobSeqId.Channel, blobSeqId.Generation));
                blobSeqId.ToProto(locator->MutableBlobSeqId());
                locator->SetTotalDataLen(key.GetBlobId().BlobSize());
                locator->SetFooterLen(0);
                value.GoingToAssimilate = false;
                ++value.ValueVersion;
                outcome = EUpdateOutcome::CHANGE;
            }
            return outcome;
        });
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

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT79, "AddDataOnLoad", (Id, Self->GetLogId()), (Key, key), (Value, proto));

        // we can only add key that is not loaded before; if key exists, it MUST have been loaded from the dataset
        const auto [it, inserted] = Data.try_emplace(std::move(key), std::move(proto), uncertainWrite);
        Y_ABORT_UNLESS(inserted);
        EnumerateBlobsForValueChain(it->second.ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
            if (!RefCount[id]++) {
                AddFirstMentionedBlob(id);
            }
        });
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
        auto& record = GetRecordsPerChannelGroup(id);
        record.Trash.insert(id);
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
             STLOG(PRI_DEBUG, BLOB_DEPOT, BDT51, "UpdateKeepState", (Id, Self->GetLogId()), (Key, key),
                (KeepState, keepState), (Value, value));
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
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT14, "DeleteKey", (Id, Self->GetLogId()), (Key, key));
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

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT13, "Trim", (Id, Self->GetLogId()), (AgentId, agent.Connection->NodeId),
                (Id, ev->Cookie), (Channel, int(channelIndex)), (InvalidatedStep, invalidatedStep),
                (GivenIdRanges, channel.GivenIdRanges),
                (Agent.GivenIdRanges, agent.GivenIdRanges[channelIndex]),
                (WritesInFlight, makeWritesInFlight()));

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
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT18, "OnBarrierShift", (Id, Self->GetLogId()), (TabletId, tabletId),
            (Channel, int(channel)), (Hard, hard), (Previous, previous), (Current, current), (MaxItems, maxItems));

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

    void TData::AddFirstMentionedBlob(TLogoBlobID id) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT80, "AddFirstMentionedBlob", (Id, Self->GetLogId()), (BlobId, id));
        auto& record = GetRecordsPerChannelGroup(id);
        const auto [_, inserted] = record.Used.insert(id);
        Y_ABORT_UNLESS(inserted);
        AccountBlob(id, true);
        TotalStoredDataSize += id.BlobSize();
        Self->TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_TOTAL_STORED_DATA_SIZE] = TotalStoredDataSize;
    }

    void TData::AccountBlob(TLogoBlobID id, bool add) {
        // account record
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT81, "AccountBlob", (Id, Self->GetLogId()), (BlobId, id), (Add, add));
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
        Trash.insert(Used.extract(usedIt));
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
        if (!CollectGarbageRequestsInFlight && self->Loaded && Collectible(self)) {
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
        THashMap<TLogoBlobID, ui32> refcounts;
        for (const auto& [key, value] : Data) {
            EnumerateBlobsForValueChain(value.ValueChain, info->TabletID, [&](TLogoBlobID id, ui32, ui32) {
                ++refcounts[id];
            });
        }
        Y_ABORT_UNLESS(RefCount == refcounts);

        for (const auto& [cookie, id] : InFlightTrash) {
            const bool inserted = refcounts.try_emplace(id).second;
            Y_ABORT_UNLESS(inserted);
        }

        THashSet<std::tuple<ui8, ui32, TLogoBlobID>> used;
        for (const auto& [id, count] : refcounts) {
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
