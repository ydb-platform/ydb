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

        THandoffMap(const THullCtxPtr &hullCtx,
                bool runHandoff,
                const TActorId &skeletonId)
            : HullCtx(hullCtx)
            , Top(HullCtx->VCtx->Top)
            , RunHandoff(runHandoff)
            , SkeletonId(skeletonId)
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
        void Transform(const TKey& /*key*/, TMemRec& /*memRec*/, TDataMerger& dataMerger) {
            // do nothing by default, all work is done in template specialization for logo blobs
            Counter++;
            Y_DEBUG_ABORT_UNLESS(dataMerger.Empty());
        }

    private:
        THullCtxPtr HullCtx;
        const std::shared_ptr<TBlobStorageGroupInfo::TTopology> Top;
        const bool RunHandoff;
        const TActorId SkeletonId;

        std::vector<ui8> DelMap;
        size_t Counter = 0;
        TStat Stat;
    };

    template<>
    inline void THandoffMap<TKeyLogoBlob, TMemRecLogoBlob>::Transform(const TKeyLogoBlob& key, TMemRecLogoBlob& memRec,
            TDataMerger& dataMerger) {
        Y_DEFER { Counter++; };

        if (!RunHandoff) {
            return;
        }

        Y_VERIFY(Counter < DelMap.size());
        TIngress ingress = memRec.GetIngress(); // ingress we are going to change
        ui8 vecSize = Top->GType.TotalPartCount();

        const NMatrix::TVectorType delPlan(DelMap[Counter], vecSize);
        const NMatrix::TVectorType delVec = delPlan & ingress.LocalParts(Top->GType);

        if (delVec.Empty()) {
            return;
        }

        // mark deleted handoff parts in ingress
        for (ui8 i : delVec) {
            // this clears local bit too
            ingress.DeleteHandoff(Top.get(), HullCtx->VCtx->ShortSelfVDisk, TLogoBlobID(key.LogoBlobID(), i + 1), true);
        }

        // update merger with the filtered parts (only remaining local parts are kept)
        dataMerger.FilterLocalParts(ingress.LocalParts(Top->GType));

        // reinstate memRec
        memRec = TMemRecLogoBlob(ingress);
        memRec.SetDiskBlob(TDiskPart(0, 0, dataMerger.GetInplacedBlobSize(key.LogoBlobID())));
        memRec.SetType(dataMerger.GetType());

        Y_ABORT_UNLESS(memRec.GetLocalParts(Top->GType) == dataMerger.GetParts());
    }

    template<>
    template<class TIterator>
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
