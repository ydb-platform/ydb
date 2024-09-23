#pragma once

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
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;

        //////////////// Statistics ////////////////////////////////////////////
        struct TStat {
            // build plan stat
            ui64 ItemsTotal = 0;
            ui64 ItemsMarkedDeleteOnly = 0;
            ui64 SkippedLevelItems = 0;

            // restore/send stat
            ui64 SuccessfulMoveSend = 0;
            ui64 FailedMoveSend = 0;

            TString ToStringBuildPlanStat() const {
                return Sprintf("Total# %" PRIu64 "  MarkedDeleteOnly# %" PRIu64 " SkippedLevelItems# %" PRIu64,
                               ItemsTotal, ItemsMarkedDeleteOnly, SkippedLevelItems);
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
                bool runHandoff,
                const TActorId &skeletonId)
            : HullCtx(hullCtx)
            , Top(HullCtx->VCtx->Top)
            , RunHandoff(runHandoff)
            , SkeletonId(skeletonId)
            , DelMap()
            , Counter(0)
            , TrRes()
            , Stat()
        {
        }

        // Prepares a map of commands for every record, whether we need to move some part, delete some part or keep
        // the record unchanged.
        template <class TIterator>
        void BuildMap(
                const TLevelIndexSnapshot& /*levelSnap*/,
                const TIterator& /*i*/)
        {
            // do nothing by default, all work is done in template specialization for logo blobs
        }

        // Transforms record according to the built handoff map. It returns item we need to write, pointer is
        // valid until next call. Nullptr indicates that item is to be removed completely.
        const TTransformedItem *Transform(const TKey& key, const TMemRec* memRec,
                                          const TDataMerger* dataMerger, bool /*keepData*/, bool keepItem) {
            // do nothing by default, all work is done in template specialization for logo blobs
            Counter++;
            if (!keepItem) {
                return nullptr;
            }
            Y_DEBUG_ABORT_UNLESS(dataMerger->Empty());
            return TrRes.SetRaw(key, memRec, dataMerger);
        }

    private:
        THullCtxPtr HullCtx;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const bool RunHandoff;
        const TActorId SkeletonId;

        TDeque<ui8> DelMap;
        unsigned Counter;
        TTransformedItem TrRes;
        TStat Stat;
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
            const TKeyLogoBlob& key,
            const TMemRecLogoBlob* memRec,
            const TDataMerger* dataMerger,
            bool keepData,
            bool keepItem)
    {
        Y_DEFER { Counter++; };

        if (!keepItem) {
            return nullptr;
        }

        if (!keepData) {
            return TrRes.SetRmData(key, memRec, dataMerger);
        }

        const TTransformedItem *defaultResult = TrRes.SetRaw(key, memRec, dataMerger); // unchanged by default
        if (!RunHandoff) {
            return defaultResult;
        }

        Y_VERIFY(Counter < DelMap.size());
        TIngress ingress = memRec->GetIngress(); // ingress we are going to change
        NMatrix::TVectorType localParts = ingress.LocalParts(Top->GType);
        ui8 vecSize = Top->GType.TotalPartCount();

        const NMatrix::TVectorType delPlan(DelMap.at(Counter), vecSize);
        const NMatrix::TVectorType delVec = delPlan & localParts;

        if (delVec.Empty()) {
            return defaultResult;
        }

        // mark deleted handoff parts in ingress
        for (ui8 i = localParts.FirstPosition(); i != localParts.GetSize(); i = localParts.NextPosition(i)) {
            if (delVec.Get(i)) {
                const ui8 partId = i + 1;
                TLogoBlobID id(key.LogoBlobID(), partId);
                ingress.DeleteHandoff(Top.get(), HullCtx->VCtx->ShortSelfVDisk, id, true);
                localParts.Clear(i);
            }
        }

        if (localParts.Empty()) {
            // we have deleted all parts, we can remove the record completely
            return nullptr;
        }

        TDataMerger newMerger;
        switch (memRec->GetType()) {
            case TBlobType::DiskBlob: {
                const TDiskBlob& blob = dataMerger->GetDiskBlobMerger().GetDiskBlob();

                // create new blob from kept parts
                for (auto it = blob.begin(); it != blob.end(); ++it) {
                    if (localParts.Get(it.GetPartId() - 1)) {
                        newMerger.AddPart(blob, it);
                    }
                }
                break;
            }
            case TBlobType::HugeBlob:
            case TBlobType::ManyHugeBlobs: {
                const auto& oldMerger = dataMerger->GetHugeBlobMerger();

                auto parts = oldMerger.GetParts();
                Y_DEBUG_ABORT_UNLESS(oldMerger.SavedData().size() == parts.CountBits());
                Y_DEBUG_ABORT_UNLESS(oldMerger.SavedData().size() == oldMerger.GetCircaLsns().size());

                for (ui8 i = parts.FirstPosition(), j = 0; i != parts.GetSize(); i = parts.NextPosition(i), ++j) {
                    if (localParts.Get(i)) {
                        auto curOneHotPart = NMatrix::TVectorType::MakeOneHot(i, parts.GetSize());
                        newMerger.AddHugeBlob(&oldMerger.SavedData()[j], &oldMerger.SavedData()[j] + 1, curOneHotPart, oldMerger.GetCircaLsns()[j]);
                    } else {
                        newMerger.AddDeletedHugeBlob(oldMerger.SavedData()[j]);
                    }
                }
                break;
            }
            default: {
                Y_DEBUG_ABORT_UNLESS(false);
                return defaultResult;
            }
        }

        // SetNewDisk can handle empty dataMerger correctly.
        // If dataMerger is empty, we still keep the record, it contains knowledge about
        // this logoblob, only garbage collection removes records completely
        return TrRes.SetNewDisk(key, ingress, newMerger);
    }


    template <>
    template <class TIterator>
    inline void THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::BuildMap(
            const TLevelIndexSnapshot& levelSnap,
            const TIterator& i)
    {
        typedef ::NKikimr::TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob> TIndexRecordMerger;
        typedef TLogoBlobsSnapshot::TForwardIterator TLevelIt;

        if (RunHandoff) {

            auto newItem = [this] (const TIterator& /*subsIt*/, const TIndexRecordMerger& /*subsMerger*/) {
                Stat.ItemsTotal++;
            };

            auto doMerge = [this] (const TIterator& /*subsIt*/, const TLevelIt& dbIt,
                                   const TIndexRecordMerger& /*subsMerger*/, const TIndexRecordMerger& dbMerger) {
                // check ingress
                TLogoBlobID id(dbIt.GetCurKey().LogoBlobID());
                TIngress globalIngress = dbMerger.GetMemRec().GetIngress();
                auto del = globalIngress.GetVDiskHandoffDeletedVec(Top.get(), HullCtx->VCtx->ShortSelfVDisk, id) & globalIngress.LocalParts(Top->GType);
                DelMap.push_back(del.Raw());

                // update stat
                bool delEmpty = del.Empty();
                Stat.ItemsMarkedDeleteOnly += unsigned(!delEmpty);
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
        }

        LOG_INFO(*TlsActivationContext, NKikimrServices::BS_HANDOFF,
                 VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                    "THandoffMap: map build: %s", Stat.ToStringBuildPlanStat().data()));
    }

    ////////////////////////////////////////////////////////////////////////////
    // CreateHandoffMap
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    TIntrusivePtr<THandoffMap<TKey, TMemRec>> CreateHandoffMap(
                const THullCtxPtr& hullCtx,
                bool runHandoff,
                const TActorId& skeletonId) {
        return new THandoffMap<TKey, TMemRec>(hullCtx, runHandoff, skeletonId);
    }

} // NKikimr
