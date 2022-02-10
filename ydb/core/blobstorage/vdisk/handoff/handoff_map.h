#pragma once

#include "defs.h"
#include "handoff_synclogproxy.h"
#include "handoff_delegate.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullmergeits.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // Handoff map - we build a map of keys to pass to other replicas
    // Only template specification for LogoBlobs does something usefull,
    // for other types it does nothing (because we replicate LogoBlobs only)
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class THandoffMap : public TThrRefBase {
    public:
        typedef ::NKikimr::TLevelIndex<TKey, TMemRec> TLevelIndex;
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;

        //////////////// Statistics ////////////////////////////////////////////
        struct TStat {
            // build plan stat
            ui64 ItemsTotal = 0;
            ui64 ItemsMarkedMoveOnly = 0;
            ui64 ItemsMarkedDeleteOnly = 0;
            ui64 ItemsMarkedMoveAndDelete = 0;
            ui64 SkippedLevelItems = 0;

            // restore/send stat
            ui64 SuccessfulMoveSend = 0;
            ui64 FailedMoveSend = 0;

            TString ToStringBuildPlanStat() const {
                return Sprintf("Total# %" PRIu64 " MarkedMoveOnly# %" PRIu64 " MarkedDeleteOnly# %" PRIu64
                               " MarkedMoveAndDelete# %" PRIu64 " SkippedLevelItems# %" PRIu64,
                               ItemsTotal, ItemsMarkedMoveOnly, ItemsMarkedDeleteOnly, ItemsMarkedMoveAndDelete,
                               SkippedLevelItems);
            }

            TString ToStringRuntimeStat() const {
                return Sprintf("SuccessfulMoveSend# %" PRIu64 " FailedMoveSend# %" PRIu64,
                               SuccessfulMoveSend, FailedMoveSend);
            }
        };

        //////////////// Transformed Item //////////////////////////////////////
        struct TTransformedItem {
            TKey Key;
            const TMemRec *MemRec;
            const TDataMerger *DataMerger;

            // intermediate data
            TMemRec NewMemRec;                      // new mem rec is build here if required
            TDataMerger NewDataMerger;              // new DataMerger, if we need to rebuild it

            TTransformedItem();
            const TTransformedItem *SetRaw(const TKey &key, const TMemRec *memRec, const TDataMerger *dataMerger);
            const TTransformedItem *SetNewDisk(const TKey &key, const TIngress &ingress, TDataMerger &dataMerger);
            const TTransformedItem *SetRmData(const TKey &key, const TMemRec *memRec, const TDataMerger *dataMerger);
        };


        THandoffMap(const THullCtxPtr &hullCtx,
                const TIntrusivePtr<THandoffDelegate> &handoffDelegate,
                bool runHandoff,
                const TActorId &skeletonId)
            : HullCtx(hullCtx)
            , Top(HullCtx->VCtx->Top)
            , HandoffDelegate(handoffDelegate)
            , RunHandoff(runHandoff)
            , SkeletonId(skeletonId)
            , MoveMap()
            , DelMap()
            , Counter(0)
            , TrRes()
            , Stat()
            , ProxyID()
            , NotifyID()
        {}

        // Prepares a map of commands for every record, whether we need to move some part, delete some part or keep
        // the record unchanged.
        template <class TIterator>
        TActorId BuildMap(
                const TActorContext &ctx,
                const TLevelIndexSnapshot &levelSnap,
                const TIterator &i,
                const TActorId &notifyID)
        {
            // do nothing by default, all work is done in template specialization for logo blobs
            Y_UNUSED(ctx);
            Y_UNUSED(levelSnap);
            Y_UNUSED(i);
            NotifyID = notifyID;
            return ProxyID;
        }

        // Transforms record according to the built handoff map. It returns item we need to write, pointer is
        // valid until next call. Nullptr indicates that item is to be removed completely.
        const TTransformedItem *Transform(const TActorContext &ctx, const TKey &key, const TMemRec *memRec,
                                          const TDataMerger *dataMerger, bool keepData) {
            // do nothing by default, all work is done in template specialization for logo blobs
            Y_UNUSED(ctx);
            Y_UNUSED(keepData);
            Counter++;
            Y_VERIFY(dataMerger->Empty());
            return TrRes.SetRaw(key, memRec, dataMerger);
        }

        // Finish transforming items, shutdown proxy
        void Finish(const TActorContext &ctx) {
            // do nothing by default, all work is done in template specialization for logo blobs
            Y_VERIFY_DEBUG(ProxyID == TActorId());
            ctx.Send(NotifyID, new TEvHandoffSyncLogFinished(false));
        }

    private:
        THullCtxPtr HullCtx;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        TIntrusivePtr<THandoffDelegate> HandoffDelegate;
        const bool RunHandoff;
        const TActorId SkeletonId;
        // TODO: we can store MoveMap and DelMap in a more compact way
        //       Hints: 1. most of values are zero (we don't want to move or delete)
        //              2. we can store only 2 bit for every local part, not 8 bits for the whole logoblob key
        TDeque<ui8> MoveMap;
        TDeque<ui8> DelMap;
        unsigned Counter;
        TTransformedItem TrRes;
        TStat Stat;
        TActorId ProxyID;
        TActorId NotifyID;
    };


    ////////////////////////////////////////////////////////////////////////////
    // TTransformedItem implementation
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    THandoffMap<TKey, TMemRec>::TTransformedItem::TTransformedItem()
        : Key()
        , MemRec(nullptr)
        , DataMerger(nullptr)
        , NewMemRec()
        , NewDataMerger()
    {}

    template <class TKey, class TMemRec>
    const typename THandoffMap<TKey, TMemRec>::TTransformedItem *
        THandoffMap<TKey, TMemRec>::TTransformedItem::SetRaw(const TKey &key, const TMemRec *memRec,
                                                             const TDataMerger *dataMerger) {
        Key = key;
        MemRec = memRec;
        DataMerger = dataMerger;
        NewMemRec.SetNoBlob();
        NewDataMerger.Clear();
        return this;
    }

    template <class TKey, class TMemRec>
    const typename THandoffMap<TKey, TMemRec>::TTransformedItem *
        THandoffMap<TKey, TMemRec>::TTransformedItem::SetNewDisk(const TKey &key, const TIngress &ingress,
                                                                 TDataMerger &dataMerger) {
        NewMemRec = TMemRecLogoBlob(ingress);
        NewDataMerger.Swap(dataMerger);
        NewMemRec.SetType(NewDataMerger.GetType());
        Y_VERIFY(NewDataMerger.HasSmallBlobs() ||
                 (NewMemRec.GetType() == TBlobType::DiskBlob && !NewMemRec.HasData())); // i.e. we also work for empty blobs

        Key = key;
        MemRec = &NewMemRec;
        DataMerger = &NewDataMerger;
        return this;
    }

    template <class TKey, class TMemRec>
    const typename THandoffMap<TKey, TMemRec>::TTransformedItem *
        THandoffMap<TKey, TMemRec>::TTransformedItem::SetRmData(const TKey &key, const TMemRec *memRec,
                                                                const TDataMerger *dataMerger) {
            NewMemRec = TMemRecLogoBlob(memRec->GetIngress());
            NewDataMerger.SetEmptyFromAnotherMerger(dataMerger);
            NewMemRec.SetType(NewDataMerger.GetType());

            Key = key;
            MemRec = &NewMemRec;
            DataMerger = &NewDataMerger;
            return this;
    }


    ////////////////////////////////////////////////////////////////////////////
    // Template specialization for LogoBlobs
    ////////////////////////////////////////////////////////////////////////////
    template <>
    inline const THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::TTransformedItem *
        THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::Transform(
            const TActorContext &ctx,
            const TKeyLogoBlob &key,
            const TMemRecLogoBlob *memRec,
            const TDataMerger *dataMerger,
            bool keepData)
    {
        const TTransformedItem *result = nullptr;
        if (!keepData) {
            result = TrRes.SetRmData(key, memRec, dataMerger);
        } else {
            result = TrRes.SetRaw(key, memRec, dataMerger); // unchanged by default
            if (RunHandoff) {
                Y_VERIFY_DEBUG(MoveMap.size() == DelMap.size() && Counter < MoveMap.size());
                const NMatrix::TVectorType localVec = memRec->GetIngress().LocalParts(Top->GType);
                ui8 vecSize = Top->GType.TotalPartCount();
                const NMatrix::TVectorType movePlan(MoveMap.at(Counter), vecSize);
                const NMatrix::TVectorType delPlan(DelMap.at(Counter), vecSize);

                // handle move
                const NMatrix::TVectorType moveVec = movePlan & localVec;
                if (!moveVec.Empty()) {
                    if (memRec->GetType() == TBlobType::HugeBlob) {
                        Y_FAIL("Implement"); // FIXME
                    } else if (memRec->GetType() == TBlobType::DiskBlob) {
                        const auto& merger = dataMerger->GetDiskBlobMerger();
                        const TDiskBlob& blob = merger.GetDiskBlob();
                        ui32 fullDataSize = blob.GetFullDataSize();

                        // iterate via parts
                        for (TDiskBlob::TPartIterator it = blob.begin(), e = blob.end(); it != e; ++it) {
                            ui8 partId = it.GetPartId();
                            Y_VERIFY_DEBUG(partId > 0);
                            if (moveVec.Get(partId - 1)) {
                                TLogoBlobID id(key.LogoBlobID(), partId);
                                TVDiskIdShort vdisk(memRec->GetIngress().GetMainReplica(Top.get(), id));
                                // FIXME: for huge blobs we need to implement an actor which makes read, because we don't have
                                // this blob in memory
                                TRope temp;
                                bool res = HandoffDelegate->Restore(ctx, vdisk, id, fullDataSize, it.GetPart());
                                Stat.SuccessfulMoveSend += ui64(res);
                                Stat.FailedMoveSend += ui64(!res);
                            }
                        }
                    } else
                        Y_FAIL("Unexpected case");
                }

                // handle delete (avoid writing some parts)
                // what we do:
                // 1. remove some local parts according to del plan
                // 2. change handoff parts to status deleted
                // 3. form a new blob w/o deleted parts, resulting blob may be empty. Result size is written to a new memrec
                const NMatrix::TVectorType delVec = delPlan & localVec;
                if (!delVec.Empty()) {
                    // yes, we ready to delete some parts
                    if (memRec->GetType() == TBlobType::HugeBlob) {
                        Y_FAIL("Implement"); // FIXME
                    } else if (memRec->GetType() == TBlobType::ManyHugeBlobs) {
                        Y_FAIL("Implement"); // FIXME
                    } else if (memRec->GetType() == TBlobType::DiskBlob) {
                        TIngress ingress = memRec->GetIngress(); // ingress we are going to change
                        const auto& merger = dataMerger->GetDiskBlobMerger();
                        const TDiskBlob& blob = merger.GetDiskBlob();

                        // a vector of local parts we have in ingress; we may have some of them missing in actual
                        // data merger as it contains only MemBlob items
                        NMatrix::TVectorType localParts = ingress.LocalParts(Top->GType);

                        // mark deleted handoff parts in ingress
                        for (ui8 i = localParts.FirstPosition(); i != localParts.GetSize(); i = localParts.NextPosition(i)) {
                            if (delVec.Get(i)) {
                                const ui8 partId = i + 1;
                                TLogoBlobID id(key.LogoBlobID(), partId);
                                ingress.DeleteHandoff(Top.get(), HullCtx->VCtx->ShortSelfVDisk, id);
                                localParts.Clear(i);
                            }
                        }

                        // create new blob from kept parts
                        TDataMerger dataMerger;
                        for (auto it = blob.begin(); it != blob.end(); ++it) {
                            if (localParts.Get(it.GetPartId() - 1)) {
                                dataMerger.AddPart(blob, it);
                            }
                        }
                        // SetNewDisk can handle empty dataMerger correctly.
                        // If dataMerger is empty, we still keep the record, it contains knowledge about
                        // this logoblob, only garbage collection removes records completely
                        result = TrRes.SetNewDisk(key, ingress, dataMerger);

                        // put new stat to SyncLog
                        TIngress syncLogIngress = ingress.CopyWithoutLocal(Top->GType);
                        ctx.Send(ProxyID, new TEvHandoffSyncLogDel(key.LogoBlobID(), syncLogIngress));
                    } else
                        Y_FAIL("Unexpected case");
                }
            }
        }

        Counter++;
        return result;
    }


    template <>
    template <class TIterator>
    inline TActorId THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::BuildMap(
            const TActorContext &ctx,
            const TLevelIndexSnapshot &levelSnap,
            const TIterator &i,
            const TActorId &notifyID)
    {
        typedef ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob> TIndexRecordMerger;
        typedef TLogoBlobsSnapshot::TForwardIterator TLevelIt;

        NotifyID = notifyID;

        if (RunHandoff) {

            auto newItem = [this] (const TIterator &subsIt, const TIndexRecordMerger &subsMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
                Stat.ItemsTotal++;
            };

            auto doMerge = [this] (const TIterator &subsIt, const TLevelIt &dbIt,
                                   const TIndexRecordMerger &subsMerger, const TIndexRecordMerger &dbMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
                // check ingress
                TLogoBlobID id(dbIt.GetCurKey().LogoBlobID());
                TIngress levelIngress = dbMerger.GetMemRec().GetIngress();
                TIngress::TPairOfVectors moveDel = levelIngress.HandoffParts(Top.get(), HullCtx->VCtx->ShortSelfVDisk, id);
                MoveMap.push_back(moveDel.first.Raw());
                DelMap.push_back(moveDel.second.Raw());

                // update stat
                bool moveEmpty = moveDel.first.Empty();
                bool delEmpty = moveDel.second.Empty();
                Stat.ItemsMarkedMoveOnly += unsigned(!moveEmpty && delEmpty);
                Stat.ItemsMarkedDeleteOnly += unsigned(moveEmpty && !delEmpty);
                Stat.ItemsMarkedMoveAndDelete += unsigned(!moveEmpty && !delEmpty);
            };

            using namespace std::placeholders;
            auto crash = std::bind(MergeIteratorWithWholeDbDefaultCrashReport<const TIterator &, const TLevelIt &>,
                                   HullCtx->VCtx->VDiskLogPrefix, _1, _2);


            // the subset we processing
            TIterator subsIt(i);
            subsIt.SeekToFirst();
            // for the whole level index
            TLevelIt dbIt(HullCtx, std::addressof(levelSnap)); // addressof instead of "&" fixes ICE for MSVC (CROWDFUNDING-8)

            MergeIteratorWithWholeDb<TIterator, TLevelIt, TIndexRecordMerger>(Top->GType, subsIt, dbIt,
                                                                              newItem, doMerge, crash);
            // run sync log proxy
            ProxyID = ctx.Register(::NKikimr::CreateHandoffSyncLogProxy(SkeletonId,  notifyID)); // specifying NKikimr namespace fixes ICE for MSVC (CROWDFUNDING-8)
        }

        LOG_INFO(ctx, NKikimrServices::BS_HANDOFF,
                 VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                    "THandoffMap: map build: %s", Stat.ToStringBuildPlanStat().data()));

        return ProxyID;
    }

    template <>
    inline void THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::Finish(const TActorContext &ctx) {
        LOG_INFO(ctx, NKikimrServices::BS_HANDOFF,
            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                "THandoffMap: finished: %s", Stat.ToStringRuntimeStat().data()));

        if (RunHandoff) {
            Y_VERIFY_DEBUG(ProxyID != TActorId());
            ctx.Send(ProxyID, new TEvHandoffSyncLogDel()); // i.e. finish
        } else {
            Y_VERIFY_DEBUG(ProxyID == TActorId());
            ctx.Send(NotifyID, new TEvHandoffSyncLogFinished(false));
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // CreateHandoffMap
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    TIntrusivePtr<THandoffMap<TKey, TMemRec>> CreateHandoffMap(
                const THullCtxPtr &hullCtx,
                const TIntrusivePtr<THandoffDelegate> &handoffDelegate,
                bool runHandoff,
                const TActorId &skeletonId) {
        return new THandoffMap<TKey, TMemRec>(hullCtx, handoffDelegate, runHandoff, skeletonId);
    }

} // NKikimr
