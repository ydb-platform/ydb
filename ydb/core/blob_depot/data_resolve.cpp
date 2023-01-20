#include "data.h"
#include "data_resolve.h"
#include "data_uncertain.h"
#include "schema.h"

namespace NKikimr::NBlobDepot {

    using TData = TBlobDepot::TData;

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

        size_t lastResponseSize = 0;
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
        TResolveDecommitContext ResolveDecommitContext;

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
                TResolveDecommitContext&& resolveDecommitContext = {})
            : TTransactionBase(self)
            , Request(request.Release())
            , ResolveDecommitContext(std::move(resolveDecommitContext))
            , Result(*Request)
        {}

        TTxResolve(TTxResolve& predecessor)
            : TTransactionBase(predecessor.Self)
            , Request(std::move(predecessor.Request))
            , ResolveDecommitContext(std::move(predecessor.ResolveDecommitContext))
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
                (LastScannedKey, LastScannedKey), (DecommitBlobs.size, ResolveDecommitContext.DecommitBlobs.size()));

            bool progress = false;
            if (!KeysLoaded && !LoadKeys(txc, progress)) {
                return progress;
            } else if (SuccessorTx) {
                return true;
            } else {
                KeysLoaded = true;
            }

            ui32 numItemsRemain = 10'000;

            while (!ResolveDecommitContext.DecommitBlobs.empty()) {
                if (!numItemsRemain) {
                    SuccessorTx = true;
                    return true;
                }

                const auto& blob = ResolveDecommitContext.DecommitBlobs.front();
                if (Self->Data->LastAssimilatedBlobId < blob.Id) {
                    numItemsRemain -= Self->Data->AddDataOnDecommit(blob, txc, this, true);
                }
                ResolveDecommitContext.DecommitBlobs.pop_front();
            }

            while (!ResolveDecommitContext.DropNodataBlobs.empty()) {
                if (!numItemsRemain) {
                    SuccessorTx = true;
                    return true;
                }

                const TKey& key = ResolveDecommitContext.DropNodataBlobs.front();
                const TValue *value = Self->Data->FindKey(key);
                if (value && value->GoingToAssimilate) {
                    Self->Data->DeleteKey(key, txc, this);
                }
                ResolveDecommitContext.DropNodataBlobs.pop_front();
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
                Self->Data->ScanRange(begin, end, flags, callback);
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

            if (Self->Data->Loaded) {
                return true;
            }

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

                if (end && Self->Data->LastLoadedKey && *end <= *Self->Data->LastLoadedKey) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT76, "TTxResolve: skipping subrange", (Id, Self->GetLogId()),
                        (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex));
                    continue; // key is already loaded
                }

                if (Self->Data->LastLoadedKey && begin <= Self->Data->LastLoadedKey && !(flags & EScanFlags::REVERSE)) {
                    Y_VERIFY(!end || *Self->Data->LastLoadedKey < *end);

                    // special case -- forward scan and we have some data in memory
                    auto callback = [&](const TKey& key, const TValue& /*value*/) {
                        LastScannedKey = key;
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT83, "TTxResolve: skipping key in memory", (Id, Self->GetLogId()),
                            (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex), (Key, key));
                        return !ResolveDecommitContext.DecommitBlobs.empty() || ++NumKeysRead != maxKeys;
                    };
                    if (!Self->Data->ScanRange(begin, Self->Data->LastLoadedKey, flags | EScanFlags::INCLUDE_END, callback)) {
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT84, "TTxResolve: skipping remaining subrange", (Id, Self->GetLogId()),
                            (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex));
                        continue; // we have read all the keys required (MaxKeys limit hit)
                    }

                    // adjust range beginning
                    begin = Self->Data->LastLoadedKey;
                    flags &= ~EScanFlags::INCLUDE_BEGIN;
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

                            const bool isKeyLoaded = Self->Data->IsKeyLoaded(key);

                            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT85, "TTxResolve: loading key from database", (Id, Self->GetLogId()),
                                (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex), (Key, key),
                                (IsKeyLoaded, isKeyLoaded));

                            if (!isKeyLoaded) {
                                Self->Data->AddDataOnLoad(key, rowset.template GetValue<Schema::Data::Value>(),
                                    rowset.template GetValueOrDefault<Schema::Data::UncertainWrite>(), true);
                            }

                            const bool matchBegin = !begin || (flags & EScanFlags::INCLUDE_BEGIN ? *begin <= key : *begin < key);
                            const bool matchEnd = !end || (flags & EScanFlags::INCLUDE_END ? key <= *end : key < *end);
                            if (matchBegin && matchEnd) {
                                if (ResolveDecommitContext.DecommitBlobs.empty() && ++NumKeysRead == maxKeys) {
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
            if (value.GoingToAssimilate && Self->Config.GetIsDecommittingGroup()) {
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

            const auto& errors = ResolveDecommitContext.ResolutionErrors;
            if (std::binary_search(errors.begin(), errors.end(), key)) {
                item.SetErrorReason("item resolution error");
                item.ClearValueChain();
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
            std::vector<std::unique_ptr<IEventBase>> outbox;
            std::unordered_map<std::tuple<ui64, bool>, std::vector<TLogoBlobID>> getBlobIds;
            const ui64 id = LastResolveQueryId + 1;

            for (const auto& item : ev->Get()->Record.GetItems()) {
                switch (item.GetKeyDesignatorCase()) {
                    case NKikimrBlobDepot::TEvResolve::TItem::kKeyRange: {
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

                        const auto& range = item.GetKeyRange();

                        TLogoBlobID minId = range.HasBeginningKey()
                            ? TKey::FromBinaryKey(range.GetBeginningKey(), Self->Config).GetBlobId()
                            : TLogoBlobID(tabletId, 0, 0, 0, 0, 0);

                        TLogoBlobID maxId = range.HasEndingKey()
                            ? TKey::FromBinaryKey(range.GetEndingKey(), Self->Config).GetBlobId()
                            : TLogoBlobID(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId,
                                TLogoBlobID::MaxCrcMode);

                        Y_VERIFY(minId <= maxId);

                        if (LastAssimilatedBlobId && maxId <= *LastAssimilatedBlobId) {
                            continue; // we have this range fully assimilated, just skip
                        }

                        // adjust minId to skip already assimilated items
                        minId = Max(minId, LastAssimilatedBlobId.value_or(minId));

                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT46, "going to TEvRange", (Id, Self->GetLogId()), (TabletId, tabletId),
                            (MinId, minId), (MaxId, maxId), (MustRestoreFirst, item.GetMustRestoreFirst()), (Cookie, id));

                        auto ev = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, minId, maxId,
                            item.GetMustRestoreFirst(), TInstant::Max(), true);
                        ev->Decommission = true;
                        outbox.push_back(std::move(ev));

                        break;
                    }

                    case NKikimrBlobDepot::TEvResolve::TItem::kExactKey: {
                        const TLogoBlobID blobId = TKey::FromBinaryKey(item.GetExactKey(), Self->Config).GetBlobId();
                        const auto key = std::make_tuple(blobId.TabletID(), item.GetMustRestoreFirst());
                        getBlobIds[key].push_back(blobId);
                        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT86, "going to TEvGet", (Id, Self->GetLogId()),
                            (BlobId, blobId), (MustRestoreFirst, item.GetMustRestoreFirst()), (Cookie, id));
                        break;
                    }

                    case NKikimrBlobDepot::TEvResolve::TItem::KEYDESIGNATOR_NOT_SET:
                        Y_VERIFY_DEBUG(false);
                        break;
                }

            }

            // convert all gets to events in outbox
            for (auto& [key, blobIds] : getBlobIds) {
                const auto& [tabletId, mustRestoreFirst] = key;
                const ui32 sz = blobIds.size();
                TArrayHolder<TEvBlobStorage::TEvGet::TQuery> q(new TEvBlobStorage::TEvGet::TQuery[sz]);
                for (ui32 i = 0; i < sz; ++i) {
                    q[i].Set(blobIds[i]);
                }
                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(q, sz, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, mustRestoreFirst, true);
                ev->Decommission = true;
                outbox.emplace_back(std::move(ev));
            }

            if (!outbox.empty()) {
                ResolveDecommitContexts[id] = {ev, (ui32)outbox.size()};
                ++LastResolveQueryId;
                Y_VERIFY(LastResolveQueryId == id);

                for (auto& ev : outbox) {
                    SendToBSProxy(Self->SelfId(), Self->Config.GetVirtualGroupId(), ev.release(), id);
                }

                return;
            }
        }

        Self->Execute(std::make_unique<TTxResolve>(Self, ev));
    }

    template<typename TCallback>
    void TData::HandleResolutionResult(ui64 id, TCallback&& callback) {
        auto& contexts = Self->Data->ResolveDecommitContexts;
        if (const auto it = contexts.find(id); it != contexts.end()) {
            auto& context = it->second;
            if (!callback(context)) {
                STLOG(PRI_INFO, BLOB_DEPOT, BDT87, "failing TEvResolve", (Id, Self->GetLogId()), (Sender, context.Ev->Sender),
                    (Cookie, context.Ev->Cookie));
                auto [response, record] = TEvBlobDepot::MakeResponseFor(*context.Ev, NKikimrProto::ERROR,
                    "unassimilated key resolution error");
                TActivationContext::Send(response.release());
            } else if (!--context.NumRangesInFlight) {
                auto ev = context.Ev;
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT88, "actually starting TEvResolve", (Id, Self->GetLogId()), (Sender, ev->Sender),
                    (Cookie, ev->Cookie));
                std::sort(context.ResolutionErrors.begin(), context.ResolutionErrors.end());
                Self->Execute(std::make_unique<TTxResolve>(Self, ev, std::move(context)));
            } else {
                return; // keep context running
            }
            contexts.erase(it);
        }
    }

    void TData::Handle(TEvBlobStorage::TEvRangeResult::TPtr ev) {
        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT50, "TEvRangeResult", (Id, Self->GetLogId()), (Msg, msg), (Cookie, ev->Cookie));

        HandleResolutionResult(ev->Cookie, [&](auto& context) {
            if (msg.Status == NKikimrProto::OK) {
                for (const auto& r : msg.Responses) {
                    context.DecommitBlobs.push_back({r.Id, r.Keep, r.DoNotKeep});
                }
                return true;
            } else {
                // we can't be sure that there are no still unassimilated keys in this range, so we report ERROR to
                // the whole range query; TODO(alexvru): provide a way to mark only specific request item as faulty one
                return false;
            }
        });
    }

    void TData::Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
        if (!ev->Cookie) {
            return UncertaintyResolver->Handle(ev);
        }

        auto& msg = *ev->Get();
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT55, "TEvGetResult", (Id, Self->GetLogId()), (Msg, msg), (Cookie, ev->Cookie));

        HandleResolutionResult(ev->Cookie, [&](auto& context) {
            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                const auto& r = msg.Responses[i];
                if (r.Status == NKikimrProto::OK) {
                    context.DecommitBlobs.push_back({r.Id, r.Keep, r.DoNotKeep});
                } else if (r.Status == NKikimrProto::NODATA) {
                    context.DropNodataBlobs.push_back(TKey(r.Id));
                } else {
                    // mark this specific key as unresolvable
                    context.ResolutionErrors.push_back(TKey(r.Id));
                }
            }
            return true;
        });
    }

} // NKikimr::NBlobDepot
