#include "data.h"
#include "coro_tx.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TData::TResolveDecommitActor : public TActorBootstrapped<TResolveDecommitActor> {
        struct TEvPrivate {
            enum {
                EvTxComplete = EventSpaceBegin(TEvents::ES_PRIVATE),
            };
        };

        TBlobDepot* const Self;
        std::weak_ptr<TToken> Token;
        std::shared_ptr<TToken> ActorToken = std::make_shared<TToken>();
        std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob> DecommitBlobs;
        THashSet<TLogoBlobID> ResolutionErrors;
        TEvBlobDepot::TEvResolve::TPtr Ev;

        ui32 TxInFlight = 0;

        ui32 RangesInFlight = 0;

        std::deque<std::tuple<TLogoBlobID, bool>> GetQ;
        ui32 GetsInFlight = 0;
        ui32 GetBytesInFlight = 0;
        static constexpr ui32 MaxGetsInFlight = 10;
        static constexpr ui32 MaxGetBytesInFlight = 10'000'000;

        ui32 PutsInFlight = 0;

        THashMap<TLogoBlobID, TKey> IdToKey;

        bool Finished = false;

        std::vector<TAssimilatedBlobInfo> AssimilatedBlobs;

        using TRange = std::tuple<ui64, TLogoBlobID, TLogoBlobID, bool>;
        using TScan = std::tuple<TKey, TKey, TScanFlags, bool, std::optional<TRange>>;

        class TTxPrepare : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            TResolveDecommitActor *Actor;
            std::weak_ptr<TToken> ActorToken;
            int Index = 0;
            std::deque<TScan> Scans;
            std::optional<TScanRange> ScanRange;
            bool IssueGets;
            std::optional<TRange> IssueRangeAfter;

            // transaction-local state
            bool Progress = false;
            bool RestartTx = false;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DECOMMIT_BLOBS; }

            TTxPrepare(TResolveDecommitActor *actor, std::deque<TScan>&& scans)
                : TTransactionBase(actor->Self)
                , Actor(actor)
                , ActorToken(Actor->ActorToken)
                , Scans(std::move(scans))
            {}

            TTxPrepare(TTxPrepare& other)
                : TTransactionBase(other.Self)
                , Actor(other.Actor)
                , ActorToken(std::move(other.ActorToken))
                , Index(other.Index)
                , Scans(std::move(other.Scans))
                , ScanRange(std::move(other.ScanRange))
                , IssueGets(other.IssueGets)
                , IssueRangeAfter(std::move(other.IssueRangeAfter))
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                if (ActorToken.expired()) {
                    return true;
                }

                auto checkProgress = [&] {
                    if (Progress) {
                        RestartTx = true;
                        return true;
                    } else {
                        return false;
                    }
                };

                // process pending scans
                auto doScanRange = [&] {
                    auto callback = [&](const TKey& key, const TValue& value) {
                        if (IssueGets && value.GoingToAssimilate) {
                            InvokeOtherActor(*Actor, &TResolveDecommitActor::IssueGet, key.GetBlobId(), true /*mustRestoreFirst*/);
                        }
                        return true;
                    };
                    if (Self->Data->ScanRange(*ScanRange, &txc, &Progress, callback)) { // scan has been finished completely
                        ScanRange.reset();
                        if (IssueRangeAfter) {
                            std::apply([&](auto&&... args) {
                                InvokeOtherActor(*Actor, &TResolveDecommitActor::IssueRange, std::move(args)...);
                            }, *IssueRangeAfter);
                        }
                        return true;
                    } else { // some data remains
                        return false;
                    }
                };
                if (ScanRange && !doScanRange()) {
                    return checkProgress();
                }
                while (!Scans.empty()) {
                    auto& [from, to, flags, issueGets, issueRangeAfter] = Scans.front();
                    ScanRange.emplace(std::move(from), std::move(to), flags);
                    IssueGets = issueGets;
                    IssueRangeAfter = std::move(issueRangeAfter);
                    Scans.pop_front();

                    if (!doScanRange()) {
                        return checkProgress();
                    }
                }

                // process explicit items after doing all scans
                for (auto& items = Actor->Ev->Get()->Record.GetItems(); Index < items.size(); ++Index) {
                    if (const auto& item = items[Index]; item.HasExactKey()) {
                        TData::TKey key = TKey::FromBinaryKey(item.GetExactKey(), Self->Config);
                        if (!Self->Data->EnsureKeyLoaded(key, txc, &Progress)) {
                            return checkProgress();
                        }
                        const TValue *value = Self->Data->FindKey(key);
                        const bool notYetAssimilated = Self->Data->LastAssimilatedBlobId < key.GetBlobId();
                        const bool doGet = !value ? notYetAssimilated :
                            value->GoingToAssimilate ? item.GetMustRestoreFirst() : notYetAssimilated;
                        if (doGet) {
                            InvokeOtherActor(*Actor, &TResolveDecommitActor::IssueGet, key.GetBlobId(),
                                item.GetMustRestoreFirst());
                        }
                    }
                }

                return true;
            }

            void Complete(const TActorContext&) override {
                if (ActorToken.expired()) {
                    return;
                } else if (RestartTx) {
                    Self->Execute(std::make_unique<TTxPrepare>(*this));
                } else {
                    TActivationContext::Send(new IEventHandle(TEvPrivate::EvTxComplete, 0, Actor->SelfId(), {}, nullptr, 0));
                }
            }
        };

        class TTxDecommitBlobs : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            THashSet<TLogoBlobID> ResolutionErrors;
            std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob> DecommitBlobs;
            TEvBlobDepot::TEvResolve::TPtr Ev;

        public:
            TTxType GetTxType() const override { return NKikimrBlobDepot::TXTYPE_DECOMMIT_BLOBS; }

            TTxDecommitBlobs(TBlobDepot *self, THashSet<TLogoBlobID>&& resolutionErrors, 
                    std::deque<TEvBlobStorage::TEvAssimilateResult::TBlob>&& decommitBlobs,
                    TEvBlobDepot::TEvResolve::TPtr ev)
                : TTransactionBase(self)
                , ResolutionErrors(std::move(resolutionErrors))
                , DecommitBlobs(std::move(decommitBlobs))
                , Ev(ev)
            {}

            bool Execute(TTransactionContext& txc, const TActorContext&) override {
                for (size_t num = 0; !DecommitBlobs.empty() && num < 10'000; DecommitBlobs.pop_front()) {
                    num += Self->Data->AddDataOnDecommit(DecommitBlobs.front(), txc, this);
                }
                return true;
            }

            void Complete(const TActorContext&) override {
                Self->Data->CommitTrash(this);
                if (DecommitBlobs.empty()) {
                    Self->Data->ExecuteTxResolve(Ev, std::move(ResolutionErrors));
                } else {
                    Self->Execute(std::make_unique<TTxDecommitBlobs>(Self, std::move(ResolutionErrors),
                        std::move(DecommitBlobs), Ev));
                }
            }
        };

    public:
        TResolveDecommitActor(TBlobDepot *self, TEvBlobDepot::TEvResolve::TPtr ev)
            : Self(self)
            , Token(self->Token)
            , Ev(ev)
        {}

        void Bootstrap() {
            if (Token.expired()) {
                return PassAway();
            }

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT42, "TResolveDecommitActor::Bootstrap", (Id, Self->GetLogId()),
                (Sender, Ev->Sender), (Cookie, Ev->Cookie));

            std::deque<TScan> scans;

            for (const auto& item : Ev->Get()->Record.GetItems()) {
                switch (item.GetKeyDesignatorCase()) {
                    case NKikimrBlobDepot::TEvResolve::TItem::kKeyRange: {
                        if (!item.HasTabletId()) {
                            return FinishWithError(NLog::PRI_CRIT, "incorrect request: tablet id not set");
                        }

                        const ui64 tabletId = item.GetTabletId();
                        const auto& range = item.GetKeyRange();

                        TLogoBlobID minId = range.HasBeginningKey()
                            ? TKey::FromBinaryKey(range.GetBeginningKey(), Self->Config).GetBlobId()
                            : TLogoBlobID(tabletId, 0, 0, 0, 0, 0);

                        TLogoBlobID maxId = range.HasEndingKey()
                            ? TKey::FromBinaryKey(range.GetEndingKey(), Self->Config).GetBlobId()
                            : TLogoBlobID(tabletId, Max<ui32>(), Max<ui32>(), TLogoBlobID::MaxChannel,
                                TLogoBlobID::MaxBlobSize, TLogoBlobID::MaxCookie, TLogoBlobID::MaxPartId,
                                TLogoBlobID::MaxCrcMode);

                        if (maxId < minId) {
                            return FinishWithError(NLog::PRI_CRIT, "incorrect request: ending key goes before beginning one");
                        }

                        Y_ABORT_UNLESS(minId <= maxId);

                        if (Self->Data->LastAssimilatedBlobId < maxId) {
                            // adjust minId to skip already assimilated items in range query
                            if (minId < Self->Data->LastAssimilatedBlobId) {
                                if (item.GetMustRestoreFirst()) {
                                    scans.emplace_back(TKey(minId), TKey(*Self->Data->LastAssimilatedBlobId),
                                        EScanFlags::INCLUDE_BEGIN, true, std::nullopt);
                                }
                                minId = *Self->Data->LastAssimilatedBlobId;
                            }

                            // prepare the range first -- we must have it loaded in memory
                            scans.emplace_back(TKey(minId), TKey(maxId),
                                EScanFlags::INCLUDE_BEGIN | EScanFlags::INCLUDE_END, false,
                                std::make_tuple(tabletId, minId, maxId, item.GetMustRestoreFirst()));
                        } else if (item.GetMustRestoreFirst()) {
                            scans.emplace_back(TKey(minId), TKey(maxId),
                                EScanFlags::INCLUDE_BEGIN | EScanFlags::INCLUDE_END, true, std::nullopt);
                        }

                        break;
                    }

                    case NKikimrBlobDepot::TEvResolve::TItem::kExactKey:
                        // this would be processed inside the tx
                        break;

                    case NKikimrBlobDepot::TEvResolve::TItem::KEYDESIGNATOR_NOT_SET:
                        return FinishWithError(NLog::PRI_CRIT, "incorrect request: key designator not set");
                }
            }

            Self->Execute(std::make_unique<TTxPrepare>(this, std::move(scans)));
            ++TxInFlight;
            Become(&TThis::StateFunc);
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // RANGE QUERIES are for metadata only -- they scan not yet assimilated parts of the original group and do not
        // recover any data; thus they are IsIndexOnly and not MustRestoreFirst range queries

        void IssueRange(ui64 tabletId, TLogoBlobID from, TLogoBlobID to, bool mustRestoreFirst) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT50, "going to TEvRange", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (TabletId, tabletId), (From, from), (To, to), (MustRestoreFirst, mustRestoreFirst));
            auto ev = std::make_unique<TEvBlobStorage::TEvRange>(tabletId, from, to, false, TInstant::Max(), true);
            ev->Decommission = true;
            SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release(), mustRestoreFirst);
            ++RangesInFlight;
        }

        void Handle(TEvBlobStorage::TEvRangeResult::TPtr ev) {
            auto& msg = *ev->Get();
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT55, "TEvRangeResult", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (Msg, msg), (GetsInFlight, GetsInFlight), (RangesInFlight, RangesInFlight),
                (TxInFlight, TxInFlight), (PutsInFlight, PutsInFlight), (GetQ.size, GetQ.size()));

            if (msg.Status == NKikimrProto::OK) {
                for (const auto& r : msg.Responses) {
                    if (ev->Cookie) {
                        if (const TValue *value = Self->Data->FindKey(TKey(r.Id)); !value || value->GoingToAssimilate ||
                                Self->Data->LastAssimilatedBlobId < r.Id) {
                            IssueGet(r.Id, true /*mustRestoreFirst*/);
                        }
                    } else {
                        DecommitBlobs.push_back({r.Id, r.Keep, r.DoNotKeep});
                    }
                }
            } else {
                TStringBuilder err;
                err << "TEvRange query failed: " << NKikimrProto::EReplyStatus_Name(msg.Status);
                if (msg.ErrorReason) {
                    err << " (" << msg.ErrorReason << ')';
                }
                return FinishWithError(NLog::PRI_NOTICE, err);
            }

            Y_ABORT_UNLESS(RangesInFlight);
            --RangesInFlight;
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // GET QUERIES may contain request either just for metadata, or for the data too; in case we receive data, we
        // have to put it to BlobDepot storage

        void IssueGet(TLogoBlobID id, bool mustRestoreFirst) {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT86, "going to TEvGet", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (BlobId, id), (MustRestoreFirst, mustRestoreFirst));
            GetQ.emplace_back(id, mustRestoreFirst);
            ProcessGetQueue();
        }

        static ui32 GetBytesFor(const std::tuple<TLogoBlobID, bool>& q) {
            const auto& [id, mustRestoreFirst] = q;
            return mustRestoreFirst ? id.BlobSize() : 0;
        }

        void ProcessGetQueue() {
            while (!GetQ.empty() && GetsInFlight < MaxGetsInFlight && GetBytesInFlight + GetBytesFor(GetQ.front()) <= MaxGetBytesInFlight) {
                const auto [id, mustRestoreFirst] = GetQ.front();
                ++GetsInFlight;
                const ui32 bytes = GetBytesFor(GetQ.front());
                GetBytesInFlight += bytes;
                GetQ.pop_front();
                auto ev = std::make_unique<TEvBlobStorage::TEvGet>(id, 0, 0, TInstant::Max(),
                    NKikimrBlobStorage::EGetHandleClass::FastRead, false /*mustRestoreFirst*/,
                    !mustRestoreFirst /*isIndexOnly*/);
                ev->Decommission = true;
                SendToBSProxy(SelfId(), Self->Config.GetVirtualGroupId(), ev.release(), bytes);
            }
        }

        void Handle(TEvBlobStorage::TEvGetResult::TPtr ev) {
            auto& msg = *ev->Get();
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT87, "TEvGetResult", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (Msg, msg), (GetsInFlight, GetsInFlight), (RangesInFlight, RangesInFlight),
                (TxInFlight, TxInFlight), (PutsInFlight, PutsInFlight), (GetQ.size, GetQ.size()));

            for (ui32 i = 0; i < msg.ResponseSz; ++i) {
                auto& r = msg.Responses[i];
                if (r.Status == NKikimrProto::OK) {
                    if (r.Buffer) { // wasn't index read
                        IssuePut(TKey(r.Id), std::move(r.Buffer), r.Keep, r.DoNotKeep);
                    } else {
                        DecommitBlobs.push_back({r.Id, r.Keep, r.DoNotKeep});
                    }
                } else if (r.Status == NKikimrProto::NODATA) {
                    AssimilatedBlobs.push_back({TData::TKey(r.Id), TAssimilatedBlobInfo::TDrop{}});
                    if (AssimilatedBlobs.size() >= 10'000) {
                        IssueTxCommitAssimilatedBlob();
                    }
                } else {
                    // mark this specific key as unresolvable
                    ResolutionErrors.emplace(r.Id);
                }
            }

            Y_ABORT_UNLESS(GetsInFlight);
            Y_ABORT_UNLESS(GetBytesInFlight >= ev->Cookie);
            --GetsInFlight;
            GetBytesInFlight -= ev->Cookie;
            ProcessGetQueue();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // PUT QUERIES are used to store retrieved MustRestoreFirst blobs in local storage

        void IssuePut(TKey key, TRope&& buffer, bool keep, bool doNotKeep) {
            std::vector<ui8> channels(1);
            if (Self->PickChannels(NKikimrBlobDepot::TChannelKind::Data, channels)) {
                TChannelInfo& channel = Self->Channels[channels.front()];
                const ui64 value = channel.NextBlobSeqId++;
                const auto blobSeqId = TBlobSeqId::FromSequentalNumber(channel.Index, Self->Executor()->Generation(), value);
                const TLogoBlobID id = blobSeqId.MakeBlobId(Self->TabletID(), EBlobType::VG_DATA_BLOB, 0, buffer.size());
                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT91, "going to TEvPut", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                    (Cookie, Ev->Cookie), (Key, key), (BlobId, id));
                SendToBSProxy(SelfId(), channel.GroupId, new TEvBlobStorage::TEvPut(id, TRcBuf(buffer), TInstant::Max()),
                    (ui64)keep | (ui64)doNotKeep << 1);
                const bool inserted = channel.AssimilatedBlobsInFlight.insert(value).second; // prevent from barrier advancing
                Y_ABORT_UNLESS(inserted);
                const bool inserted1 = IdToKey.try_emplace(id, std::move(key)).second;
                Y_ABORT_UNLESS(inserted1);
                ++PutsInFlight;
            } else { // we couldn't restore this blob -- there was no place to write it to
                ResolutionErrors.insert(key.GetBlobId());
            }
        }

        void Handle(TEvBlobStorage::TEvPutResult::TPtr ev) {
            auto& msg = *ev->Get();

            const auto it = IdToKey.find(msg.Id);
            Y_ABORT_UNLESS(it != IdToKey.end());
            TKey key = std::move(it->second);
            IdToKey.erase(it);

            const bool keep = ev->Cookie & 1;
            const bool doNotKeep = ev->Cookie >> 1 & 1;

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT88, "got TEvPutResult", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (Msg, msg), (Key, key), (Keep, keep), (DoNotKeep, doNotKeep),
                (GetsInFlight, GetsInFlight), (RangesInFlight, RangesInFlight), (TxInFlight, TxInFlight),
                (PutsInFlight, PutsInFlight), (GetQ.size, GetQ.size()));

            if (msg.Status != NKikimrProto::OK) { // do not reply OK to this item
                ResolutionErrors.insert(key.GetBlobId());
            } else {
                AssimilatedBlobs.push_back({std::move(key), TAssimilatedBlobInfo::TUpdate{
                    TBlobSeqId::FromLogoBlobId(msg.Id), keep, doNotKeep}});
                if (AssimilatedBlobs.size() >= 10'000) {
                    IssueTxCommitAssimilatedBlob();
                }
            }

            Y_ABORT_UNLESS(PutsInFlight);
            --PutsInFlight;
        }

        void HandleTxComplete() {
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT84, "HandleTxComplete", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (GetsInFlight, GetsInFlight), (RangesInFlight, RangesInFlight),
                (TxInFlight, TxInFlight), (PutsInFlight, PutsInFlight), (GetQ.size, GetQ.size()));

            Y_ABORT_UNLESS(TxInFlight);
            --TxInFlight;
        }

        void CheckIfDone() {
            if (TxInFlight + RangesInFlight + GetsInFlight + GetQ.size() + PutsInFlight != 0) {
                return;
            }

            if (!AssimilatedBlobs.empty()) {
                return IssueTxCommitAssimilatedBlob();
            }

            Y_ABORT_UNLESS(!Finished);
            Finished = true;

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT92, "request succeeded", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (ResolutionErrors.size, ResolutionErrors.size()),
                (DecommitBlobs.size, DecommitBlobs.size()));

            Self->Execute(std::make_unique<TTxDecommitBlobs>(Self, std::move(ResolutionErrors), std::move(DecommitBlobs), Ev));
            PassAway();
        }

        void IssueTxCommitAssimilatedBlob() {
            Self->Data->ExecuteTxCommitAssimilatedBlob(std::exchange(AssimilatedBlobs, {}), TEvPrivate::EvTxComplete,
                SelfId(), 0);
            ++TxInFlight;
        }

        void FinishWithError(NLog::EPriority prio, TString errorReason) {
            Y_ABORT_UNLESS(!Finished);
            Finished = true;

            STLOG(prio, BLOB_DEPOT, BDT89, "request failed", (Id, Self->GetLogId()), (Sender, Ev->Sender),
                (Cookie, Ev->Cookie), (ErrorReason, errorReason));
            auto [response, record] = TEvBlobDepot::MakeResponseFor(*Ev, NKikimrProto::ERROR, std::move(errorReason));
            TActivationContext::Send(response.release());
            PassAway();
        }

        STATEFN(StateFunc) {
            if (Token.expired()) {
                return PassAway();
            }

            switch (const ui32 type = ev->GetTypeRewrite()) {
                hFunc(TEvBlobStorage::TEvGetResult, Handle);
                hFunc(TEvBlobStorage::TEvRangeResult, Handle);
                hFunc(TEvBlobStorage::TEvPutResult, Handle);
                cFunc(TEvPrivate::EvTxComplete, HandleTxComplete);

                default:
                    Y_DEBUG_ABORT("unexpected event Type# %08" PRIx32, type);
                    STLOG(PRI_CRIT, BLOB_DEPOT, BDT90, "unexpected event", (Id, Self->GetLogId()), (Type, type));
                    break;
            }

            CheckIfDone();
        }
    };

    IActor *TBlobDepot::TData::CreateResolveDecommitActor(TEvBlobDepot::TEvResolve::TPtr ev) {
        return new TResolveDecommitActor(Self, ev);
    }

} // NKikimr::NBlobDepot
