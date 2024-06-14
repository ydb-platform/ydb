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
        Y_ABORT_UNLESS(it != KeyToIndex.end());
        KeysToFilterOut[it->second] = true;
    }

    void TData::TResolveResultAccumulator::AddKeyWithError(const TKey& key, const TString& errorReason) {
        const auto it = KeyToIndex.find(key);
        Y_ABORT_UNLESS(it != KeyToIndex.end());
        auto& item = Items[it->second];
        item.SetErrorReason(item.HasErrorReason() ? TStringBuilder() << item.GetErrorReason() << ", " << errorReason : errorReason);
    }

    class TData::TTxResolve : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
        std::unique_ptr<TEvBlobDepot::TEvResolve::THandle> Request;
        THashSet<TLogoBlobID> ResolutionErrors;
        size_t ItemIndex = 0;
        std::optional<TScanRange> Range;
        TResolveResultAccumulator Result;
        std::deque<TKey> Uncertainties;

        bool SuccessorTx = false;

    public:
        TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_RESOLVE; }

        TTxResolve(TBlobDepot *self, TEvBlobDepot::TEvResolve::TPtr request, THashSet<TLogoBlobID>&& resolutionErrors)
            : TTransactionBase(self)
            , Request(request.Release())
            , ResolutionErrors(std::move(resolutionErrors))
            , Result(*Request)
        {}

        TTxResolve(TTxResolve& predecessor)
            : TTransactionBase(predecessor.Self)
            , Request(std::move(predecessor.Request))
            , ResolutionErrors(std::move(predecessor.ResolutionErrors))
            , ItemIndex(predecessor.ItemIndex)
            , Range(std::move(predecessor.Range))
            , Result(std::move(predecessor.Result))
            , Uncertainties(std::move(predecessor.Uncertainties))
        {}

        bool Execute(TTransactionContext& txc, const TActorContext&) override {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT22, "TTxResolve::Execute", (Id, Self->GetLogId()),
                (Sender, Request->Sender), (Cookie, Request->Cookie), (ItemIndex, ItemIndex));

            bool progress = false;

            const auto& record = Request->Get()->Record;
            for (; ItemIndex < record.ItemsSize(); ++ItemIndex) {
                const auto& item = record.GetItems(ItemIndex);
                std::optional<ui64> cookie = item.HasCookie() ? std::make_optional(item.GetCookie()) : std::nullopt;
                std::optional<bool> status;
                switch (item.GetKeyDesignatorCase()) {
                    case NKikimrBlobDepot::TEvResolve::TItem::kKeyRange:
                        status = ProcessKeyRange(item.GetKeyRange(), txc, progress, cookie, item.GetMustRestoreFirst());
                        break;

                    case NKikimrBlobDepot::TEvResolve::TItem::kExactKey:
                        status = ProcessExactKey(item.GetExactKey(), txc, progress, cookie, item.GetMustRestoreFirst());
                        break;

                    case NKikimrBlobDepot::TEvResolve::TItem::KEYDESIGNATOR_NOT_SET:
                        Y_DEBUG_ABORT("incorrect query field");
                        break;
                }
                if (status) {
                    return *status;
                }
            }

            return true;
        }

        std::optional<bool> ProcessKeyRange(const NKikimrBlobDepot::TEvResolve::TKeyRange& range,
                TTransactionContext& txc, bool& progress, const std::optional<ui64>& cookie, bool mustRestoreFirst) {
            if (!Range) {
                Range.emplace();
                Range->Begin = range.HasBeginningKey()
                    ? TKey::FromBinaryKey(range.GetBeginningKey(), Self->Config)
                    : TKey::Min();
                Range->End = range.HasEndingKey()
                    ? TKey::FromBinaryKey(range.GetEndingKey(), Self->Config)
                    : TKey::Max();
                Range->Flags = TScanFlags()
                    | (range.GetIncludeBeginning() ? EScanFlags::INCLUDE_BEGIN : TScanFlags())
                    | (range.GetIncludeEnding() ? EScanFlags::INCLUDE_END : TScanFlags())
                    | (range.GetReverse() ? EScanFlags::REVERSE : TScanFlags());
                Range->MaxKeys = range.GetMaxKeys();
            }
            auto callback = [&](const TKey& key, const TValue& value) {
                IssueResponseItem(cookie, key, value, mustRestoreFirst);
                return true;
            };
            if (Self->Data->ScanRange(*Range, &txc, &progress, callback)) {
                Range.reset();
                return std::nullopt;
            } else {
                return SuccessorTx = progress;
            }
        }

        std::optional<bool> ProcessExactKey(const TString& exactKey, TTransactionContext& txc, bool& progress,
                const std::optional<ui64>& cookie, bool mustRestoreFirst) {
            const auto key = TKey::FromBinaryKey(exactKey, Self->Config);
            if (!Self->Data->EnsureKeyLoaded(key, txc)) {
                return SuccessorTx = progress;
            }
            const TValue *value = Self->Data->FindKey(key);
            if (value || (!ResolutionErrors.empty() && ResolutionErrors.contains(key.GetBlobId()))) {
                static const TValue zero;
                IssueResponseItem(cookie, key, value ? *value : zero, mustRestoreFirst);
            }
            return std::nullopt;
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

        void IssueResponseItem(std::optional<ui64> cookie, const TKey& key, const TValue& value, bool reliablyWritten) {
            NKikimrBlobDepot::TEvResolveResult::TResolvedKey item;

            if (cookie) {
                item.SetCookie(*cookie);
            }
            item.SetKey(key.MakeBinaryKey());
            if (value.GoingToAssimilate && Self->Config.GetIsDecommittingGroup()) {
                Y_ABORT_UNLESS(!value.UncertainWrite);
                auto *out = item.AddValueChain();
                out->SetGroupId(Self->Config.GetVirtualGroupId());
                LogoBlobIDFromLogoBlobID(key.GetBlobId(), out->MutableBlobId());
                item.SetReliablyWritten(reliablyWritten);
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
                item.SetReliablyWritten(true);
            }
            if (value.Meta) {
                item.SetMeta(value.Meta.data(), value.Meta.size());
            }

            if (!ResolutionErrors.empty() && ResolutionErrors.contains(key.GetBlobId())) {
                item.SetErrorReason("item resolution error");
                item.ClearValueChain();
            } else {
                if (!item.ValueChainSize()) {
                    STLOG(PRI_WARN, BLOB_DEPOT, BDT48, "empty ValueChain on Resolve", (Id, Self->GetLogId()),
                        (Key, key), (Value, value), (Item, item), (Sender, Request->Sender), (Cookie, Request->Cookie));
                }
                if (item.GetValueVersion() != value.ValueVersion) {
                    item.SetValueVersion(value.ValueVersion);
                }
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
            Self->RegisterWithSameMailbox(CreateResolveDecommitActor(ev));
        } else {
            ExecuteTxResolve(ev);
        }
    }

    void TData::ExecuteTxResolve(TEvBlobDepot::TEvResolve::TPtr ev, THashSet<TLogoBlobID>&& resolutionErrors) {
        Self->Execute(std::make_unique<TTxResolve>(Self, ev, std::move(resolutionErrors)));
    }

} // NKikimr::NBlobDepot
