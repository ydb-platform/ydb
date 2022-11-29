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

        Y_VERIFY_DEBUG(chain.HasLocator());
        const auto& locator1 = chain.GetLocator();
        Y_VERIFY_DEBUG(locator1.HasGroupId() && locator1.HasBlobSeqId() && locator1.HasTotalDataLen());

        Y_VERIFY_DEBUG(item.HasBlobLocator());
        const auto& locator2 = item.GetBlobLocator();
        Y_VERIFY_DEBUG(locator2.HasGroupId() && locator2.HasBlobSeqId() && locator2.HasTotalDataLen());

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

            if (!inserted) {
                EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                    const auto it = RefCount.find(id);
                    Y_VERIFY(it != RefCount.end());
                    if (!--it->second) {
                        deleteQ.push_back(id);
                    }
                });
            }

            EUpdateOutcome outcome = callback(value, inserted);

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
                Y_VERIFY(it != RefCount.end());
                if (it->second) {
                    return true; // remove this blob from deletion queue, it still has references
                } else {
                    InFlightTrash.emplace(cookie, id);
                    NIceDb::TNiceDb(txc.DB).Table<Schema::Trash>().Key(id.AsBinaryString()).Update();
                    RefCount.erase(it);
                    TotalStoredDataSize -= id.BlobSize();
                    return false; // keep this blob in deletion queue
                }
            };
            deleteQ.erase(std::remove_if(deleteQ.begin(), deleteQ.end(), filter), deleteQ.end());
            if (!deleteQ.empty()) {
                UncertaintyResolver->DropBlobs(deleteQ);
            }

            auto row = NIceDb::TNiceDb(txc.DB).Table<Schema::Data>().Key(key.MakeBinaryKey());
            switch (outcome) {
                case EUpdateOutcome::DROP:
                    UncertaintyResolver->DropKey(key);
                    Data.erase(it);
                    row.Delete();
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
                    if (uncertainWriteBefore && !value.UncertainWrite) {
                        UncertaintyResolver->MakeKeyCertain(key);
                    }
                    return true;

                case EUpdateOutcome::NO_CHANGE:
                    return false;
            }
        }
    }

    const TData::TValue *TData::FindKey(const TKey& key) const {
        const auto it = Data.find(key);
        return it != Data.end() ? &it->second : nullptr;
    }

    void TData::UpdateKey(const TKey& key, const NKikimrBlobDepot::TEvCommitBlobSeq::TItem& item, bool uncertainWrite,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT10, "UpdateKey", (Id, Self->GetLogId()), (Key, key), (Item, item));
        UpdateKey(key, txc, cookie, "UpdateKey", [&](TValue& value, bool inserted) {
            if (!inserted) { // update value items
                value.Meta = item.GetMeta();
                value.Public = false;

                // update it to keep new blob locator
                value.ValueChain.Clear();
                auto *chain = value.ValueChain.Add();
                auto *locator = chain->MutableLocator();
                locator->CopyFrom(item.GetBlobLocator());

                // reset original blob id, if any
                value.OriginalBlobId.reset();
            }

            value.UncertainWrite = uncertainWrite;
            return EUpdateOutcome::CHANGE;
        }, item, uncertainWrite);
    }

    void TData::MakeKeyCertain(const TKey& key) {
        const auto it = Data.find(key);
        Y_VERIFY(it != Data.end());
        TValue& value = it->second;
        value.UncertainWrite = false;
        UncertaintyResolver->MakeKeyCertain(key);
        KeysMadeCertain.push_back(key);
        if (!CommitCertainKeysScheduled) {
            TActivationContext::Schedule(TDuration::Seconds(1), new IEventHandle(TEvPrivate::EvCommitCertainKeys, 0,
                Self->SelfId(), {}, nullptr, 0));
            CommitCertainKeysScheduled = true;
        }
    }

    void TData::HandleCommitCertainKeys() {
        class TTxCommitCertainKeys : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            std::deque<TKey> KeysMadeCertain;

        public:
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
        Y_VERIFY(groupId != Max<ui32>());
        Y_VERIFY(id.TabletID() == info->TabletID);
        const auto& key = std::make_tuple(id.Channel(), groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        return it->second;
    }

    void TData::AddDataOnLoad(TKey key, TString value, bool uncertainWrite) {
        NKikimrBlobDepot::TValue proto;
        const bool success = proto.ParseFromString(value);
        Y_VERIFY(success);

        // we can only add key that is not loaded before; if key exists, it MUST have been loaded from the dataset
        const auto [it, inserted] = Data.try_emplace(std::move(key), std::move(proto), uncertainWrite);
        if (inserted) {
            EnumerateBlobsForValueChain(it->second.ValueChain, Self->TabletID(), [&](TLogoBlobID id, ui32, ui32) {
                if (!RefCount[id]++) {
                    AddFirstMentionedBlob(id);
                }
            });
        }
    }

    void TData::AddDataOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBlob& blob,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        UpdateKey(TKey(blob.Id), txc, cookie, "AddDataOnDecommit", [&](TValue& value, bool inserted) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT49, "AddDataOnDecommit", (Id, Self->GetLogId()), (Blob, blob),
                (Value, value), (Inserted, inserted));

            // update keep state if necessary
            if (blob.DoNotKeep && value.KeepState < EKeepState::DoNotKeep) {
                value.KeepState = EKeepState::DoNotKeep;
            } else if (blob.Keep && value.KeepState < EKeepState::Keep) {
                value.KeepState = EKeepState::Keep;
            }

            // if there is not value chain for this blob, map it to the original blob id
            if (value.ValueChain.empty()) {
                value.OriginalBlobId = blob.Id;
            }

            return EUpdateOutcome::CHANGE;
        });
    }

    void TData::AddTrashOnLoad(TLogoBlobID id) {
        auto& record = GetRecordsPerChannelGroup(id);
        record.Trash.insert(id);
        AccountBlob(id, true);
    }

    void TData::AddGenStepOnLoad(ui8 channel, ui32 groupId, TGenStep issuedGenStep, TGenStep confirmedGenStep) {
        const auto& key = std::make_tuple(channel, groupId);
        const auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
        auto& record = it->second;
        record.IssuedGenStep = issuedGenStep;
        record.LastConfirmedGenStep = confirmedGenStep;
    }

    bool TData::UpdateKeepState(TKey key, EKeepState keepState,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
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
            Y_VERIFY(!inserted);
            return EUpdateOutcome::DROP;
        });
    }

    void TData::OnPushNotifyResult(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
        TAgent& agent = Self->GetAgent(ev->Recipient);

        const auto it = agent.InvalidateStepRequests.find(ev->Get()->Record.GetId());
        Y_VERIFY(it != agent.InvalidateStepRequests.end());
        auto items = std::move(it->second);
        agent.InvalidateStepRequests.erase(it);

        const ui32 generation = Self->Executor()->Generation();

        std::vector<TBlobSeqId> writesInFlight;
        for (const auto& item : ev->Get()->Record.GetWritesInFlight()) {
            writesInFlight.push_back(TBlobSeqId::FromProto(item));
        }
        std::sort(writesInFlight.begin(), writesInFlight.end());

        for (const auto& [channel, invalidatedStep] : items) {
            const ui32 channel_ = channel;
            auto& agentGivenIdRanges = agent.GivenIdRanges[channel];
            auto& givenIdRanges = Self->Channels[channel].GivenIdRanges;

            auto begin = std::lower_bound(writesInFlight.begin(), writesInFlight.end(), TBlobSeqId{channel, 0, 0, 0});

            auto makeWritesInFlight = [&] {
                TStringStream s;
                s << "[";
                for (auto it = begin; it != writesInFlight.end() && it->Channel == channel_; ++it) {
                    s << (it != begin ? " " : "") << it->ToString();
                }
                s << "]";
                return s.Str();
            };

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT13, "Trim", (Id, Self->GetLogId()), (AgentId, agent.Connection->NodeId),
                (Id, ev->Cookie), (Channel, channel_), (InvalidatedStep, invalidatedStep),
                (GivenIdRanges, Self->Channels[channel].GivenIdRanges),
                (Agent.GivenIdRanges, agent.GivenIdRanges[channel]),
                (WritesInFlight, makeWritesInFlight()));

            // sanity check -- ensure that current writes in flight would be conserved when processing garbage
            for (auto it = begin; it != writesInFlight.end() && it->Channel == channel; ++it) {
                Y_VERIFY_S(agentGivenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
                Y_VERIFY_S(givenIdRanges.GetPoint(it->ToSequentialNumber()), "blobSeqId# " << it->ToString());
            }

            const TBlobSeqId leastExpectedBlobIdBefore = Self->Channels[channel].GetLeastExpectedBlobId(generation);

            const TBlobSeqId trimmedBlobSeqId{channel, generation, invalidatedStep, TBlobSeqId::MaxIndex};
            const ui64 validSince = trimmedBlobSeqId.ToSequentialNumber() + 1;
            givenIdRanges.Subtract(agentGivenIdRanges.Trim(validSince));

            for (auto it = begin; it != writesInFlight.end() && it->Channel == channel; ++it) {
                agentGivenIdRanges.AddPoint(it->ToSequentialNumber());
                givenIdRanges.AddPoint(it->ToSequentialNumber());
            }

            const TBlobSeqId leastExpectedBlobIdAfter = Self->Channels[channel].GetLeastExpectedBlobId(generation);
            Y_VERIFY(leastExpectedBlobIdBefore <= leastExpectedBlobIdAfter);

            if (leastExpectedBlobIdBefore != leastExpectedBlobIdAfter) {
                OnLeastExpectedBlobIdChange(channel);
            }
        }
    }

    bool TData::OnBarrierShift(ui64 tabletId, ui8 channel, bool hard, TGenStep previous, TGenStep current, ui32& maxItems,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie) {
        const TData::TKey first(TLogoBlobID(tabletId, previous.Generation(), previous.Step(), channel, 0, 0));
        const TData::TKey last(TLogoBlobID(tabletId, current.Generation(), current.Step(), channel,
            TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode));

        bool finished = true;
        Self->Data->ScanRange(&first, &last, TData::EScanFlags::INCLUDE_END, [&](auto& key, auto& value) {
            if (value.KeepState != EKeepState::Keep || hard) {
                if (maxItems) {
                    Self->Data->DeleteKey(key, txc, cookie);
                    --maxItems;
                } else {
                    finished = false;
                    return false;
                }
            }
            return true;
        });

        return finished;
    }

    void TData::AddFirstMentionedBlob(TLogoBlobID id) {
        auto& record = GetRecordsPerChannelGroup(id);
        const auto [_, inserted] = record.Used.insert(id);
        Y_VERIFY(inserted);
        AccountBlob(id, true);
        TotalStoredDataSize += id.BlobSize();
    }

    void TData::AccountBlob(TLogoBlobID id, bool add) {
        // account record
        const ui32 groupId = Self->Info()->GroupFor(id.Channel(), id.Generation());
        auto& groupStat = Self->Groups[groupId];
        if (add) {
            groupStat.AllocatedBytes += id.BlobSize();
        } else {
            groupStat.AllocatedBytes -= id.BlobSize();
        }
    }

    bool TData::CanBeCollected(ui32 groupId, TBlobSeqId id) const {
        const auto it = RecordsPerChannelGroup.find(std::make_tuple(id.Channel, groupId));
        return it != RecordsPerChannelGroup.end() && TGenStep(id) <= it->second.IssuedGenStep;
    }

    void TData::OnLeastExpectedBlobIdChange(ui8 channel) {
        const TTabletChannelInfo *storageChannel = Self->Info()->ChannelInfo(channel);
        Y_VERIFY(storageChannel);
        for (const auto& entry : storageChannel->History) {
            const auto& key = std::make_tuple(storageChannel->Channel, entry.GroupID);
            auto [it, _] = RecordsPerChannelGroup.emplace(std::piecewise_construct, key, key);
            it->second.OnLeastExpectedBlobIdChange(this);
        }
    }

    void TData::TRecordsPerChannelGroup::MoveToTrash(TLogoBlobID id) {
        const auto usedIt = Used.find(id);
        Y_VERIFY(usedIt != Used.end());
        Trash.insert(Used.extract(usedIt));
    }

    void TData::TRecordsPerChannelGroup::OnSuccessfulCollect(TData *self) {
        auto it = Trash.begin();
        for (const TLogoBlobID& id : TrashInFlight) {
            for (; it != Trash.end() && *it < id; ++it) {}
            Y_VERIFY(it != Trash.end() && *it == id);
            it = Trash.erase(it);
            self->AccountBlob(id, false);
        }
        LastConfirmedGenStep = IssuedGenStep;
    }

    void TData::TRecordsPerChannelGroup::OnLeastExpectedBlobIdChange(TData *self) {
        CollectIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::ClearInFlight(TData *self) {
        Y_VERIFY(CollectGarbageRequestInFlight);
        CollectGarbageRequestInFlight = false;
        CollectIfPossible(self);
    }

    void TData::TRecordsPerChannelGroup::CollectIfPossible(TData *self) {
        if (!CollectGarbageRequestInFlight && !Trash.empty()) {
            self->HandleTrash(*this);
        }
    }

} // NKikimr::NBlobDepot
