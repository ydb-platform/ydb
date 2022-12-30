#include "data.h"
#include "data_resolve.h"
#include "data_uncertain.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

    namespace {
        TLogoBlobID BlobIdUpperBound(TLogoBlobID id) {
            return TLogoBlobID(id.TabletID(), id.Generation(), id.Step(), id.Channel(), TLogoBlobID::MaxBlobSize,
                id.Cookie(), TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode);
        }
    }

    TData::TResolveResultAccumulator::TResolveResultAccumulator(TEventHandle<TEvBlobDepot::TEvResolve>& ev)
        : Sender(ev.Sender)
        , Recipient(ev.Recipient)
        , Cookie(ev.Cookie)
        , InterconnectSession(ev.InterconnectSession)
    {}

    void TData::TResolveResultAccumulator::AddItem(NKikimrBlobDepot::TEvResolveResult::TResolvedKey&& item,
            const NKikimrBlobDepot::TBlobDepotConfig& config) {
        KeyToIndex.emplace(TKey::FromBinaryKey(item.GetKey(), config), Items.size());
        Items.push_back(std::move(item));
        KeysToFilterOut.push_back(false);
    }

    void TData::TResolveResultAccumulator::Send(NKikimrProto::EReplyStatus status, std::optional<TString> errorReason) {
        auto sendResult = [&](std::unique_ptr<TEvBlobDepot::TEvResolveResult> ev) {
            auto handle = std::make_unique<IEventHandle>(Sender, Recipient, ev.release(), 0, Cookie);
            if (InterconnectSession) {
                handle->Rewrite(TEvInterconnect::EvForward, InterconnectSession);
            }
            TActivationContext::Send(handle.release());
        };

        if (status != NKikimrProto::OK) {
            return sendResult(std::make_unique<TEvBlobDepot::TEvResolveResult>(status, std::move(errorReason)));
        }

        size_t lastResponseSize;
        std::unique_ptr<TEvBlobDepot::TEvResolveResult> ev;

        size_t index = 0;
        for (auto it = Items.begin(); it != Items.end(); ++it, ++index) {
            if (KeysToFilterOut[index]) {
                continue;
            }

            auto& item = *it;
            const size_t itemSize = item.ByteSizeLong();
            if (!ev || lastResponseSize + itemSize > EventMaxByteSize) {
                if (ev) {
                    sendResult(std::move(ev));
                }
                ev = std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OVERRUN, std::nullopt);
                lastResponseSize = ev->CalculateSerializedSize();
            }
            item.Swap(ev->Record.AddResolvedKeys());
            lastResponseSize += itemSize;
        }

        if (ev) {
            ev->Record.SetStatus(NKikimrProto::OK);
            sendResult(std::move(ev));
        } else { // no items in response -- all got filtered out
            sendResult(std::make_unique<TEvBlobDepot::TEvResolveResult>(NKikimrProto::OK, std::nullopt));
        }
    }

    std::deque<NKikimrBlobDepot::TEvResolveResult::TResolvedKey> TData::TResolveResultAccumulator::ReleaseItems() {
        return std::exchange(Items, {});
    }

    void TData::TResolveResultAccumulator::AddKeyWithNoData(const TKey& key) {
        const auto it = KeyToIndex.find(key);
        Y_VERIFY(it != KeyToIndex.end());
        KeysToFilterOut[it->second] = true;
    }

    void TData::TResolveResultAccumulator::AddKeyWithError(const TKey& key, const TString& errorReason) {
        const auto it = KeyToIndex.find(key);
        Y_VERIFY(it != KeyToIndex.end());
        auto& item = Items[it->second];
        item.SetErrorReason(item.HasErrorReason() ? TStringBuilder() << item.GetErrorReason() << ", " << errorReason : errorReason);
    }

    class TData::TTxResolve : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::unique_ptr<TEvBlobDepot::TEvResolve::THandle> Request;
        std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob> DecommitBlobs;
        TIntervalMap<TLogoBlobID> Errors;

        bool KeysLoaded = false;
        int ItemIndex = 0;
        std::optional<TKey> LastScannedKey;
        ui32 NumKeysRead = 0; // number of keys already read for this item

        // final state
        TResolveResultAccumulator Result;
        bool SuccessorTx = false;
        std::deque<TKey> Uncertainties;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_RESOLVE; }

        TTxResolve(TBlobDepot *self, TEvBlobDepot::TEvResolve::TPtr request,
                std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob>&& decommitBlobs = {},
                TIntervalMap<TLogoBlobID>&& errors = {})
            : TTransactionBase(self)
            , Request(request.Release())
            , DecommitBlobs(std::move(decommitBlobs))
            , Errors(std::move(errors))
            , Result(*Request)
        {}

        TTxResolve(TTxResolve& predecessor)
            : TTransactionBase(predecessor.Self)
            , Request(std::move(predecessor.Request))
            , DecommitBlobs(std::move(predecessor.DecommitBlobs))
            , Errors(std::move(predecessor.Errors))
            , KeysLoaded(predecessor.KeysLoaded)
            , ItemIndex(predecessor.ItemIndex)
            , LastScannedKey(std::move(predecessor.LastScannedKey))
            , NumKeysRead(predecessor.NumKeysRead)
            , Result(std::move(predecessor.Result))
        {}

        bool GetScanParams(const NKikimrBlobDepot::TEvResolve::TItem& item, std::optional<TKey> *begin,
                std::optional<TKey> *end, TScanFlags *flags, ui64 *maxKeys) {
            switch (item.GetKeyDesignatorCase()) {
                case NKikimrBlobDepot::TEvResolve::TItem::kKeyRange: {
                    const auto& range = item.GetKeyRange();
                    *flags = TScanFlags()
                        | (range.GetIncludeBeginning() ? EScanFlags::INCLUDE_BEGIN : TScanFlags())
                        | (range.GetIncludeEnding() ? EScanFlags::INCLUDE_END : TScanFlags())
                        | (range.GetReverse() ? EScanFlags::REVERSE : TScanFlags());
                    if (range.HasBeginningKey()) {
                        begin->emplace(TKey::FromBinaryKey(range.GetBeginningKey(), Self->Config));
                    } else {
                        begin->reset();
                    }
                    if (range.HasEndingKey()) {
                        end->emplace(TKey::FromBinaryKey(range.GetEndingKey(), Self->Config));
                    } else {
                        end->reset();
                    }
                    *maxKeys = range.GetMaxKeys();
                    return true;
                }

                case NKikimrBlobDepot::TEvResolve::TItem::kExactKey:
                    begin->emplace(TKey::FromBinaryKey(item.GetExactKey(), Self->Config));
                    end->emplace(begin->value());
                    *flags = EScanFlags::INCLUDE_BEGIN | EScanFlags::INCLUDE_END;
                    *maxKeys = 1;
                    return true;

                case NKikimrBlobDepot::TEvResolve::TItem::KEYDESIGNATOR_NOT_SET:
                    return false;
            }

            return false;
        }

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT22, "TTxResolve::Execute", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex),
                (LastScannedKey, LastScannedKey), (DecommitBlobs.size, DecommitBlobs.size()));

            bool progress = false;
            if (!KeysLoaded && !LoadKeys(txc, progress)) {
                return progress;
            } else {
                KeysLoaded = true;
            }

            for (ui32 numItemsRemain = 10'000; !DecommitBlobs.empty(); DecommitBlobs.pop_front()) {
                if (numItemsRemain) {
                    numItemsRemain -= Self->Data->AddDataOnDecommit(DecommitBlobs.front(), txc, this);
                } else {
                    SuccessorTx = true;
                    return true;
                }
            }

            const auto& record = Request->Get()->Record;
            for (const auto& item : record.GetItems()) {
                std::optional<ui64> cookie = item.HasCookie() ? std::make_optional(item.GetCookie()) : std::nullopt;

                std::optional<TKey> begin;
                std::optional<TKey> end;
                TScanFlags flags;
                ui64 maxKeys;
                const bool success = GetScanParams(item, &begin, &end, &flags, &maxKeys);
                Y_VERIFY_DEBUG(success);

                // we have everything we need contained in memory, generate response from memory
                auto callback = [&](const TKey& key, const TValue& value) {
                    IssueResponseItem(cookie, key, value);
                    return --maxKeys != 0;
                };
                Self->Data->ScanRange(begin ? &begin.value() : nullptr, end ? &end.value() : nullptr, flags, callback);
            }

            return true;
        }

        void Complete(const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT30, "TTxResolve::Complete", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie), (SuccessorTx, SuccessorTx),
                (Uncertainties.size, Uncertainties.size()));

            Self->Data->CommitTrash(this);

            if (SuccessorTx) {
                Self->Execute(std::make_unique<TTxResolve>(*this));
            } else if (Uncertainties.empty()) {
                Result.Send(NKikimrProto::OK, std::nullopt);
            } else {
                Self->Data->UncertaintyResolver->PushResultWithUncertainties(std::move(Result), std::move(Uncertainties));
            }
        }

        bool LoadKeys(TTransactionContext& txc, bool& progress) {
            NIceDb::TNiceDb db(txc.DB);

            const auto& record = Request->Get()->Record;
            const auto& items = record.GetItems();
            for (; ItemIndex < items.size(); ++ItemIndex, LastScannedKey.reset(), NumKeysRead = 0) {
                const auto& item = items[ItemIndex];

                std::optional<TKey> begin;
                std::optional<TKey> end;
                TScanFlags flags;
                ui64 maxKeys;
                const bool success = GetScanParams(item, &begin, &end, &flags, &maxKeys);
                Y_VERIFY_DEBUG(success);

                // adjust range according to actually generated data
                if (LastScannedKey) {
                    if (flags & EScanFlags::REVERSE) { // reverse scan
                        end = *LastScannedKey;
                        flags &= ~EScanFlags::INCLUDE_END;
                    } else { // direct scan
                        begin = *LastScannedKey;
                        flags &= ~EScanFlags::INCLUDE_BEGIN;
                    }
                }

                if (Self->Data->Loaded || (end && Self->Data->LastLoadedKey && *end <= *Self->Data->LastLoadedKey)) {
                    continue;
                }

                if (Self->Data->LastLoadedKey && begin <= Self->Data->LastLoadedKey && !(flags & EScanFlags::REVERSE)) {
                    Y_VERIFY(!end || *Self->Data->LastLoadedKey < *end);

                    // special case -- forward scan and we have some data in memory
                    auto callback = [&](const TKey& key, const TValue& /*value*/) {
                        LastScannedKey = key;
                        return ++NumKeysRead != maxKeys;
                    };
                    Self->Data->ScanRange(begin ? &begin.value() : nullptr, &Self->Data->LastLoadedKey.value(),
                        flags | EScanFlags::INCLUDE_END, callback);

                    // adjust range beginning
                    begin = Self->Data->LastLoadedKey;
                    flags &= ~EScanFlags::INCLUDE_BEGIN;

                    // check if we have read all the keys requested
                    if (maxKeys && NumKeysRead == maxKeys) {
                        continue;
                    }
                }

                auto processRange = [&](auto table) {
                    for (auto rowset = table.Select();; rowset.Next()) {
                        if (!rowset.IsReady()) {
                            return false;
                        } else if (!rowset.IsValid()) {
                            // no more keys in our direction
                            return true;
                        }
                        auto key = TKey::FromBinaryKey(rowset.template GetValue<Schema::Data::Key>(), Self->Config);
                        if (key != LastScannedKey) {
                            LastScannedKey = key;
                            progress = true;

                            if (!Self->Data->IsKeyLoaded(key)) {
                                Self->Data->AddDataOnLoad(key, rowset.template GetValue<Schema::Data::Value>(),
                                    rowset.template GetValueOrDefault<Schema::Data::UncertainWrite>(), true);
                            }

                            const bool matchBegin = !begin || (flags & EScanFlags::INCLUDE_BEGIN ? *begin <= key : *begin < key);
                            const bool matchEnd = !end || (flags & EScanFlags::INCLUDE_END ? key <= *end : key < *end);
                            if (matchBegin && matchEnd) {
                                const TValue *value = Self->Data->FindKey(key);
                                Y_VERIFY(value); // value must exist as it was just loaded into memory and exists in the database
                                if (++NumKeysRead == maxKeys) {
                                    // we have hit the MaxItems limit, exit
                                    return true;
                                }
                            } else if (flags & EScanFlags::REVERSE ? !matchBegin : !matchEnd) {
                                // we have exceeded the opposite boundary, exit
                                return true;
                            }
                        }
                    }
                };

                auto applyEnd = [&](auto&& x) {
                    return end
                        ? processRange(x.LessOrEqual(end->MakeBinaryKey()))
                        : processRange(std::forward<std::decay_t<decltype(x)>>(x));
                };
                auto applyBegin = [&](auto&& x) {
                    return begin
                        ? applyEnd(x.GreaterOrEqual(begin->MakeBinaryKey()))
                        : applyEnd(std::forward<std::decay_t<decltype(x)>>(x));
                };
                auto applyReverse = [&](auto&& x) {
                    return flags & EScanFlags::REVERSE
                        ? applyBegin(x.Reverse())
                        : applyBegin(std::forward<std::decay_t<decltype(x)>>(x));
                };
                if (applyReverse(db.Table<Schema::Data>())) {
                    continue; // all work done for this item
                } else if (progress) {
                    // we have already done something, so let's finish this transaction and start a new one, continuing
                    // the job
                    SuccessorTx = true;
                    return true;
                } else {
                    return false; // we'll have to restart this transaction to fetch some data
                }
            }

            return true;
        }

        void IssueResponseItem(std::optional<ui64> cookie, const TKey& key, const TValue& value) {
            NKikimrBlobDepot::TEvResolveResult::TResolvedKey item;

            if (cookie) {
                item.SetCookie(*cookie);
            }
            item.SetKey(key.MakeBinaryKey());
            if (value.ValueChain.empty() && Self->Config.GetIsDecommittingGroup()) {
                Y_VERIFY(!value.UncertainWrite);
                auto *out = item.AddValueChain();
                out->SetGroupId(Self->Config.GetVirtualGroupId());
                LogoBlobIDFromLogoBlobID(key.GetBlobId(), out->MutableBlobId());
            } else {
                EnumerateBlobsForValueChain(value.ValueChain, Self->TabletID(), [&](const TLogoBlobID& id, ui32 begin, ui32 end) {
                    if (begin != end) {
                        auto *out = item.AddValueChain();
                        out->SetGroupId(Self->Info()->GroupFor(id.Channel(), id.Generation()));
                        LogoBlobIDFromLogoBlobID(id, out->MutableBlobId());
                        if (begin) {
                            out->SetSubrangeBegin(begin);
                        }
                        if (end != id.BlobSize()) {
                            out->SetSubrangeEnd(end);
                        }
                    }
                });
            }
            if (value.Meta) {
                item.SetMeta(value.Meta.data(), value.Meta.size());
            }

            bool foundError = false;

            if (Errors) {
                const TLogoBlobID id = key.GetBlobId();
                foundError = TIntervalSet<TLogoBlobID>(id, BlobIdUpperBound(id)).IsSubsetOf(Errors);
            }

            if (foundError) {
                item.SetErrorReason("item resolution error");
            } else if (!item.ValueChainSize()) {
                STLOG(PRI_WARN, BLOB_DEPOT, BDT48, "empty ValueChain on Resolve", (Id, Self->GetLogId()),
                    (Key, key), (Value, value), (Item, item), (Sender, Request->Sender), (Cookie, Request->Cookie));
            }

            Result.AddItem(std::move(item), Self->Config);
            if (value.IsWrittenUncertainly()) {
                Uncertainties.push_back(key);
            }
        }
    };

    void TData::Handle(TEvBlobDepot::TEvResolve::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT21, "TEvResolve", (Id, Self->GetLogId()), (Msg, ev->Get()->ToString()),
            (Sender, ev->Sender), (Cookie, ev->Cookie), (LastAssimilatedBlobId, LastAssimilatedBlobId));

        if (Self->Config.GetIsDecommittingGroup() && Self->DecommitState <= EDecommitState::BlobsFinished) {
            std::vector<std::tuple<ui64, bool, TLogoBlobID, TLogoBlobID>> queries;

            for (const auto& item : ev->Get()->Record.GetItems()) {
                if (!item.HasTabletId()) {
                   STLOG(PRI_CRIT, BLOB_DEPOT, BDT42, "incorrect request", (Id, Self->GetLogId()), (Item, item));
                   auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, NKikimrProto::ERROR,
                        "incorrect request");
                   TActivationContext::Send(response.release());
                   return;
                }

                const ui64 tabletId = item.GetTabletId();
                if (LastAssimilatedBlobId && tabletId < LastAssimilatedBlobId->TabletID()) {
                    continue; // fast path
                }

                TLogoBlobID minId(tabletId, 0, 0, 0, 0, 0);
                TLogoBlobID maxId(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel, TLogoBlobID::MaxBlobSize,
                    TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId, TLogoBlobID::MaxCrcMode);

                switch (item.GetKeyDesignatorCase()) {
                    case NKikimrBlobDepot::TEvResolve::TItem::kKeyRange: {
                        const auto& range = item.GetKeyRange();
                        if (range.HasBeginningKey()) {
                            minId = TKey::FromBinaryKey(range.GetBeginningKey(), Self->Config).GetBlobId();
                        }
                        if (range.HasEndingKey()) {
                            maxId = TKey::FromBinaryKey(range.GetEndingKey(), Self->Config).GetBlobId();
                        }
                        break;
                    }

                    case NKikimrBlobDepot::TEvResolve::TItem::kExactKey:
                        minId = maxId = TKey::FromBinaryKey(item.GetExactKey(), Self->Config).GetBlobId();
                        break;

                    case NKikimrBlobDepot::TEvResolve::TItem::KEYDESIGNATOR_NOT_SET:
                        Y_VERIFY_DEBUG(false);
                        break;
                }

                Y_VERIFY_DEBUG(minId.TabletID() == tabletId);
                Y_VERIFY_DEBUG(maxId.TabletID() == tabletId);

                if (!LastAssimilatedBlobId || *LastAssimilatedBlobId < maxId) {
                    if (LastAssimilatedBlobId && minId < *LastAssimilatedBlobId) {
                        minId = *LastAssimilatedBlobId;
                    }
                    if (minId == maxId) {
                        const auto it = Data.find(TKey(minId));
                        if (it != Data.end() && !it->second.ValueChain.empty()) {
                            continue; // fast path for extreme queries
                        }
                    }
                    queries.emplace_back(tabletId, item.GetMustRestoreFirst(), minId, maxId);
                }
            }

            if (!queries.empty()) {
                const ui64 id = ++LastRangeId;
                for (const auto& [tabletId, mustRestoreFirst, minId, maxId] : queries) {
                    auto ev = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, minId, maxId, mustRestoreFirst,
                        TInstant::Max(), true);
                    ev->Decommission = true;

                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT46, "going to TEvRange", (Id, Self->GetLogId()), (TabletId, tabletId),
                        (MinId, minId), (MaxId, maxId), (MustRestoreFirst, mustRestoreFirst), (Cookie, id));
                    SendToBSProxy(Self->SelfId(), Self->Config.GetVirtualGroupId(), ev.release(), id);
                }
                ResolveDecommitContexts[id] = {ev, (ui32)queries.size()};
                return;
            }
        }

        Self->Execute(std::make_unique<TTxResolve>(Self, ev));
    }

    void TData::Handle(TEvBlobStorage::TEvRangeResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT50, "TEvRangeResult", (Id, Self->GetLogId()), (Msg, msg), (Cookie, ev->Cookie));

        auto& contexts = Self->Data->ResolveDecommitContexts;
        if (const auto it = contexts.find(ev->Cookie); it != contexts.end()) {
            auto& context = it->second;

            if (msg.Status == NKikimrProto::OK) {
                for (const auto& response : msg.Responses) {
                    context.DecommitBlobs.push_back({response.Id, response.Keep, response.DoNotKeep});
                }
            } else {
                context.Errors.Add(msg.From, BlobIdUpperBound(msg.To));
            }

            if (!--context.NumRangesInFlight) {
                Self->Execute(std::make_unique<TTxResolve>(Self, context.Ev, std::move(context.DecommitBlobs),
                    std::move(context.Errors)));
                contexts.erase(it);
            }
        }
    }

} // NKikimr::NBlobDepot
