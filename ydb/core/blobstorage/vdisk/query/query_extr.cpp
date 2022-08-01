#include "query_base.h"
#include <ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.h>

using namespace NKikimrServices;

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexExtremeQueryViaBatcherBase
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexExtremeQueryViaBatcherBase : public TLevelIndexQueryBase {
    protected:
        struct TQuery {
            TLogoBlobID LogoBlobID;
            ui32 PartId;
            ui64 Shift;
            ui64 Size;
            ui64 CookieVal;
            bool HasCookie;

            TQuery()
                : LogoBlobID()
                , PartId(0)
                , Shift(0)
                , Size(0)
                , CookieVal(0)
                , HasCookie(false)
            {}
        };

        std::unique_ptr<TLogoBlobsSnapshot::TForwardIterator> ForwardIt;
        TVector<TQuery> Queries;
        TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
        bool BlobInIndex = false;

        TQuery *FetchNextQuery() {
            ui32 queryNum = Queries.size();
            if (queryNum == Record.ExtremeQueriesSize()) {
                return nullptr;
            }

            const NKikimrBlobStorage::TExtremeQuery *query = &Record.GetExtremeQueries(queryNum);
            TQuery q;
            q.LogoBlobID = LogoBlobIDFromLogoBlobID(query->GetId());
            q.PartId = q.LogoBlobID.PartId();
            q.LogoBlobID = TLogoBlobID(q.LogoBlobID, 0);
            q.CookieVal = query->GetCookie();
            q.HasCookie = query->HasCookie();
            q.Shift = query->GetShift();
            q.Size = query->GetSize();
            Queries.push_back(q);
            ForwardIt->Seek(q.LogoBlobID);
            BlobInIndex = ForwardIt->Valid() && ForwardIt->GetCurKey().LogoBlobID() == q.LogoBlobID;

            return &Queries.back();
        }

        void Prepare() {
            Y_VERIFY(Record.ExtremeQueriesSize() > 0);
            // initialize ForwardIt with a delay to avoid work in constructor
            ForwardIt = std::make_unique<TLogoBlobsSnapshot::TForwardIterator>(QueryCtx->HullCtx, &LogoBlobsSnapshot);
            Queries.reserve(Record.ExtremeQueriesSize());
            BarriersEssence = BarriersSnapshot.CreateEssence(QueryCtx->HullCtx);
            BarriersSnapshot.Destroy();
        }

        template<typename TMerger>
        bool IsBlobDeleted(const TLogoBlobID &id, const TMerger &merger) {
            const auto &status = BarriersEssence->Keep(id, merger.GetMemRec(), merger.GetMemRecsMerged(),
                QueryCtx->HullCtx->AllowKeepFlags);
            return !status.KeepData;
        }

        TLevelIndexExtremeQueryViaBatcherBase(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId)
            : TLevelIndexQueryBase(queryCtx, parentId, std::move(logoBlobsSnapshot), std::move(barrierSnapshot),
                    ev, std::move(result), replSchedulerId)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexExtremeQueryViaBatcherIndexOnly
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexExtremeQueryViaBatcherIndexOnly :
            public TLevelIndexExtremeQueryViaBatcherBase,
            public TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherIndexOnly> {

        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob>;
        friend class TLevelIndexQueryBase;
        TIndexRecordMerger Merger;

        friend class TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherIndexOnly>;
        friend class TLevelIndexExtremeQueryViaBatcherBase;

        void Bootstrap(const TActorContext &ctx) {
            Prepare();
            MainCycle(ctx);
        }

        void MainCycle(const TActorContext &ctx) {
            TQuery *query = nullptr;
            while ((query = FetchNextQuery()) && !ResultSize.IsOverflow()) {
                Y_VERIFY(query->PartId == 0); // only full blobs (w/o specifying a part) are allowed
                const ui64 *cookiePtr = query->HasCookie ? &query->CookieVal : nullptr;
                ResultSize.AddLogoBlobIndex();
                if (!BlobInIndex) {
                    // put NODATA
                    Result->AddResult(NKikimrProto::NODATA, query->LogoBlobID, cookiePtr, nullptr);
                } else {
                    // index record(s) are found
                    ForwardIt->PutToMerger(&Merger);
                    Merger.Finish();
                    TIngress ingress = Merger.GetMemRec().GetIngress();
                    // Find out if we have all parts locally that we must have according to ingress
                    NMatrix::TVectorType mustHave = ingress.PartsWeMustHaveLocally(QueryCtx->HullCtx->VCtx->Top.get(),
                            QueryCtx->HullCtx->VCtx->ShortSelfVDisk, query->LogoBlobID);
                    NMatrix::TVectorType actuallyHave = ingress.LocalParts(QueryCtx->HullCtx->VCtx->Top->GType);
                    NMatrix::TVectorType missingParts = mustHave - actuallyHave;
                    // If we don't have something locally we return NOT_YET unless that blob is going to be collected
                    auto status = IsBlobDeleted(query->LogoBlobID, Merger) ? NKikimrProto::NODATA :
                        missingParts.Empty() ? NKikimrProto::OK : NKikimrProto::NOT_YET;
                    // Add result
                    ui64 ingressRaw = ingress.Raw();
                    ui64 *pingr = (ShowInternals ? &ingressRaw : nullptr);
                    Result->AddResult(status, query->LogoBlobID, cookiePtr, pingr);
                    Merger.Clear();
                }
            }

            // send response and die
            SendResponseAndDie(ctx, this);
        }

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULLQUERY_EXTREME_INDEX_ONLY;
        }

        TLevelIndexExtremeQueryViaBatcherIndexOnly(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId)
            : TLevelIndexExtremeQueryViaBatcherBase(queryCtx, parentId, std::move(logoBlobsSnapshot),
                    std::move(barrierSnapshot), ev, std::move(result), replSchedulerId)
            , TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherIndexOnly>()
            , Merger(QueryCtx->HullCtx->VCtx->Top->GType)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TLevelIndexExtremeQueryViaBatcherMergeData
    ////////////////////////////////////////////////////////////////////////////
    class TLevelIndexExtremeQueryViaBatcherMergeData :
            public TLevelIndexExtremeQueryViaBatcherBase,
            public TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherMergeData>
    {

        typedef ::NKikimr::TRecordMergerCallback<TKeyLogoBlob, TMemRecLogoBlob, TReadBatcher> TRecordMergerCallback;
        friend class TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherMergeData>;
        friend class TLevelIndexExtremeQueryViaBatcherBase;
        friend class TLevelIndexQueryBase;

        const TBlobStorageGroupType GType;
        TReadBatcher Batcher;
        TRecordMergerCallback Merger;
        TActiveActors ActiveActors;

        void Bootstrap(const TActorContext &ctx) {
            Prepare();
            MainCycle(ctx);
        }

        void Finish(const TActorContext &ctx) {
            std::unordered_map<TLogoBlobID, NMatrix::TVectorType, THash<TLogoBlobID>> neededParts;

            // build result
            TReadBatcherResult::TIterator rit(&Batcher.GetResult());
            for (rit.SeekToFirst(); rit.Valid(); rit.Next()) {
                const NReadBatcher::TDataItem *it = rit.Get();
                const TQuery *query = static_cast<const TQuery*>(it->Cookie);
                Y_VERIFY_DEBUG(query->LogoBlobID.PartId() == 0);
                const ui64 *cookiePtr = query->HasCookie ? &query->CookieVal : nullptr;

                ui64 ingr = it->Ingress.Raw();
                ui64 *pingr = (ShowInternals ? &ingr : nullptr);

                NReadBatcher::TDataItem::EType t = it->GetType();
                switch (t) {
                    case NReadBatcher::TDataItem::ET_CLEAN:
                        Y_FAIL("Impossible case");
                    case NReadBatcher::TDataItem::ET_NODATA:
                        // put NODATA
                        Result->AddResult(NKikimrProto::NODATA, it->Id, cookiePtr, pingr);
                        break;
                    case NReadBatcher::TDataItem::ET_ERROR:
                        // put ERROR
                        Y_VERIFY(it->Id.PartId() > 0);
                        Result->AddResult(NKikimrProto::ERROR, it->Id, cookiePtr, pingr);
                        break;
                    case NReadBatcher::TDataItem::ET_NOT_YET:
                        // put NOT_YET
                        Y_VERIFY(it->Id.PartId() > 0);
                        Result->AddResult(NKikimrProto::NOT_YET, it->Id, query->Shift, nullptr, query->Size, cookiePtr, pingr);
                        break;
                    case NReadBatcher::TDataItem::ET_SETDISK:
                    case NReadBatcher::TDataItem::ET_SETMEM:
                    {
                        // GOOD
                        Y_VERIFY(it->Id.PartId() > 0);
                        struct TProcessor {
                            std::unique_ptr<TEvBlobStorage::TEvVGetResult>& Result;
                            TLogoBlobID Id;
                            ui64 Shift;
                            ui64 Size;
                            const ui64 *CookiePtr;
                            const ui64 *IngrPtr;
                            bool Success = true;
                            void operator()(NReadBatcher::TReadError) {
                                Result->AddResult(NKikimrProto::CORRUPTED, Id, Shift, nullptr, Size, CookiePtr, IngrPtr);
                                Success = false;
                            }
                            void operator()(const char *data, size_t size) const {
                                Result->AddResult(NKikimrProto::OK, Id, Shift, data, size, CookiePtr, IngrPtr);
                            }
                            void operator()(const TRope& data) const {
                                const TString s = data.ConvertToString();
                                (*this)(s.data(), s.size());
                            }
                        } processor{Result, it->Id, query->Shift, query->Size, cookiePtr, pingr};
                        rit.GetData(processor);
                        if (!processor.Success) {
                            NMatrix::TVectorType& v = neededParts[it->Id.FullID()];
                            if (!v.GetSize()) {
                                v = NMatrix::TVectorType(0, GType.TotalPartCount());
                            }
                            v.Set(it->Id.PartId() - 1);
                        }
                        break;
                    }
                }
            }
            // send response and die
            QueryCtx->PDiskReadBytes += Batcher.GetPDiskReadBytes();

            if (neededParts.empty()) {
                return Finish2(ctx);
            }

            std::vector<TEvRestoreCorruptedBlob::TItem> items;
            for (const auto& [id, needed] : neededParts) {
                items.emplace_back(id, needed, GType, TDiskPart());
            }
            ctx.Send(QueryCtx->SkeletonId, new TEvRestoreCorruptedBlob(ctx.Now() + TDuration::Minutes(2),
                std::move(items), true, true));
        }

        void Handle(TEvRestoreCorruptedBlobResult::TPtr ev, const TActorContext& ctx) {
            std::unordered_map<TLogoBlobID, TEvRestoreCorruptedBlobResult::TItem*, THash<TLogoBlobID>> map;
            for (auto& item : ev->Get()->Items) {
                map.emplace(item.BlobId, &item);
            }
            for (auto& res : *Result->Record.MutableResult()) {
                if (res.GetStatus() == NKikimrProto::CORRUPTED) {
                    const TLogoBlobID& id = LogoBlobIDFromLogoBlobID(res.GetBlobID());
                    const auto it = map.find(id.FullID());
                    Y_VERIFY(it != map.end());
                    if (it->second->Status == NKikimrProto::OK) {
                        const TString& buffer = it->second->GetPartData(id);
                        res.SetBuffer(buffer.substr(res.GetShift(), res.GetSize() ? res.GetSize() : buffer.size() - res.GetShift()));
                        res.SetStatus(NKikimrProto::OK);
                    }
                }
            }
            Finish2(ctx);
        }

        void Finish2(const TActorContext& ctx) {
            TReplQuoter::TPtr quoter;
            if (IsRepl()) {
                quoter = QueryCtx->HullCtx->VCtx->ReplNodeResponseQuoter;
            }
            const TDuration duration = quoter
                ? quoter->Take(TActivationContext::Now(), Result->CalculateSerializedSizeCached())
                : TDuration::Zero();
            if (duration != TDuration::Zero()) {
                Schedule(duration, new TEvents::TEvWakeup);
                Become(&TThis::StateFunc);
            } else {
                SendResponse(ctx);
            }
        }

        void SendResponse(const TActorContext& ctx) {
            SendResponseAndDie(ctx, this);
        }

        void HandleReadCompletion(TEvents::TEvCompleted::TPtr& ev, const TActorContext &ctx) {
            ActiveActors.Erase(ev->Sender);
            Finish(ctx);
        }

        void MainCycle(const TActorContext &ctx) {
            TQuery *query = nullptr;
            while ((query = FetchNextQuery()) && !ResultSize.IsOverflow()) {
                const TLogoBlobID &fullId = query->LogoBlobID; // full blob id we are looking for
                const TLogoBlobID partId = TLogoBlobID(fullId, query->PartId);
                bool found = false;
                TMaybe<TIngress> ingress;

                ResultSize.AddLogoBlobIndex();
                if (BlobInIndex) {
                    ResultSize.AddLogoBlobData(GType.PartSize(partId), query->Shift, query->Size);
                    Batcher.StartTraverse(fullId, query, query->PartId, query->Shift, query->Size);
                    ForwardIt->PutToMerger(&Merger);
                    Merger.Finish();
                    ingress = Merger.GetMemRec().GetIngress();
                    if (IsBlobDeleted(fullId, Merger)) {
                        // do nothing for this case -- this blob is scheduled for deletion and will not be reported
                        Batcher.AbortTraverse();
                    } else {
                        // finish traversing and generate requests for reads
                        Batcher.FinishTraverse(*ingress);
                        found = true;
                    }
                    Merger.Clear();
                }

                if (!found) {
                    // report NODATA for requested part if it either was not found in index nor was scheduled for compaction
                    Batcher.PutNoData(partId, ingress, query);
                }
            }

            if (ResultSize.IsOverflow()) {
                SendResponseAndDie(ctx, this);
            } else {
                ui8 priority = PDiskPriority();
                std::unique_ptr<IActor> a(Batcher.CreateAsyncDataReader(ctx.SelfID, priority, /*std::move(Result->TraceId)*/ NWilson::TTraceId(), // FIXME: trace
                    IsRepl()));
                if (a) {
                    auto aid = ctx.Register(a.release());
                    ActiveActors.Insert(aid);
                    Become(&TThis::StateFunc);
                    // wait for reply
                } else {
                    Finish(ctx);
                }
            }

            BarriersEssence.Reset();
        }

        void HandlePoison(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
            Y_UNUSED(ev);
            ActiveActors.KillAndClear(ctx);
            Die(ctx);
        }

        STRICT_STFUNC(StateFunc,
            HFunc(TEvents::TEvCompleted, HandleReadCompletion)
            HFunc(TEvents::TEvPoisonPill, HandlePoison)
            CFunc(TEvents::TSystem::Wakeup, SendResponse)
            HFunc(TEvRestoreCorruptedBlobResult, Handle)
        )

    public:
        static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
            return NKikimrServices::TActivity::BS_HULLQUERY_EXTREME_DATA;
        }

        TLevelIndexExtremeQueryViaBatcherMergeData(
                std::shared_ptr<TQueryCtx> &queryCtx,
                const TActorId &parentId,
                TLogoBlobsSnapshot &&logoBlobsSnapshot,
                TBarriersSnapshot &&barrierSnapshot,
                TEvBlobStorage::TEvVGet::TPtr &ev,
                std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                TActorId replSchedulerId)
            : TLevelIndexExtremeQueryViaBatcherBase(queryCtx, parentId, std::move(logoBlobsSnapshot),
                    std::move(barrierSnapshot), ev, std::move(result), replSchedulerId)
            , TActorBootstrapped<TLevelIndexExtremeQueryViaBatcherMergeData>()
            , GType(QueryCtx->HullCtx->VCtx->Top->GType)
            , Batcher(BatcherCtx)
            , Merger(&Batcher, GType)
        {}
    };


    ////////////////////////////////////////////////////////////////////////////
    // CreateLevelIndexExtremeQueryActor
    ////////////////////////////////////////////////////////////////////////////
    IActor *CreateLevelIndexExtremeQueryActor(
                    std::shared_ptr<TQueryCtx> &queryCtx,
                    const TActorId &parentId,
                    TLogoBlobsSnapshot &&logoBlobsSnapshot,
                    TBarriersSnapshot &&barrierSnapshot,
                    TEvBlobStorage::TEvVGet::TPtr &ev,
                    std::unique_ptr<TEvBlobStorage::TEvVGetResult> result,
                    TActorId replSchedulerId) {
        const bool indexOnly = ev->Get()->Record.GetIndexOnly();
        if (indexOnly)
            return new TLevelIndexExtremeQueryViaBatcherIndexOnly(queryCtx, parentId,
                    std::move(logoBlobsSnapshot), std::move(barrierSnapshot), ev, std::move(result), replSchedulerId);
        else
            return new TLevelIndexExtremeQueryViaBatcherMergeData(queryCtx, parentId,
                    std::move(logoBlobsSnapshot), std::move(barrierSnapshot), ev, std::move(result), replSchedulerId);
    }

} // NKikimr
