#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullstorageratio.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TSinglePassRatioCollector
        //
        // Computes per-SST TSstRatio for ALL SSTs of a level index snapshot in a
        // SINGLE whole-DB, heap-merged pass. It produces values field-identical to
        // TStrategyStorageRatio::CalculateSstRatio() run once per SST, but visits
        // each key only once (instead of re-walking the whole DB per SST).
        //
        // It plugs into THeapIterator::Walk() using the same merger contract as
        // TDefragScanner (defrag_search.h): per merged key, every source with that
        // key calls AddFromSegment()/AddFromFresh() before Finish() fires once.
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TSinglePassRatioCollector {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TForwardIterator = typename TLevelIndexSnapshot::TForwardIterator;
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKey, TMemRec>;

            TSinglePassRatioCollector(TIntrusivePtr<THullCtx> hullCtx,
                                      const TLevelIndexSnapshot &levelSnap,
                                      TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriersEssence,
                                      bool allowGarbageCollection)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , BarriersEssence(std::move(barriersEssence))
                , AllowGarbageCollection(allowGarbageCollection)
                , GType(HullCtx->VCtx->Top->GType)
                , WholeMerger(GType)
            {}

            // Walk the whole DB once, filling the per-SST ratio map.
            void Run() {
                TForwardIterator it(HullCtx, &LevelSnap);
                it.SeekToFirst();
                THeapIterator<TKey, TMemRec, true> heapIt(&it);
                heapIt.Walk(std::nullopt, this, [](const TKey&, auto*) { return true; });
            }

            const THashMap<const TLevelSegment*, TSstRatioPtr>& GetRatios() const {
                return Ratios;
            }

            ////////////////////////////////////////////////////////////////////////
            // THeapIterator merger contract
            ////////////////////////////////////////////////////////////////////////
            void AddFromSegment(const TMemRec& memRec, const TDiskPart* outbound, const TKey& key, ui64 /*circaLsn*/,
                    const TLevelSegment* sst) {
                SetCurKey(key);
                WholeMerger.AddFromSegment(memRec, outbound, key, 0, sst);

                TPerSstCur* cur = nullptr;
                for (TPerSstCur& c : CurSsts) {
                    if (c.Sst == sst) {
                        cur = &c;
                        break;
                    }
                }
                if (!cur) {
                    cur = &CurSsts.emplace_back();
                    cur->Sst = sst;
                }

                // per-SST subset keep-flag counts, mirroring TRecordMergerBase::AddBasic()
                if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                    const int mode = memRec.GetIngress().GetCollectMode(TIngress::IngressMode(GType));
                    cur->NumKeepFlags += mode & CollectModeKeep;
                    cur->NumDoNotKeepFlags += mode & CollectModeDoNotKeep;
                }

                // sizes come from THIS SST's own copy (memRec + outbound), like subsIt.GetDiskData()
                TDiskDataExtractor extr;
                memRec.GetDiskData(&extr, outbound);
                cur->InplacedDataSize += extr.GetInplacedDataSize();
                cur->HugeDataSize += extr.GetHugeDataSize();
            }

            void AddFromFresh(const TMemRec& memRec, const TRope* data, const TKey& key, ui64 lsn) {
                SetCurKey(key);
                // Fresh contributes to the whole-DB counts only; it is not an SST, so it produces no
                // per-SST ratio entry (matches TStrategyStorageRatio iterating SliceSnap SSTs only,
                // while its dbIt spans fresh).
                WholeMerger.AddFromFresh(memRec, data, key, lsn);
            }

            static constexpr bool HaveToMergeData() { return false; }

            void Clear() {
                WholeMerger.Clear();
                CurSsts.clear();
                CurKeyValid = false;
            }

            void Finish() {
                WholeMerger.Finish();
                const ui32 wholeKeep = WholeMerger.GetNumKeepFlags();
                const ui32 wholeDoNotKeep = WholeMerger.GetNumDoNotKeepFlags();
                const TMemRec& wholeMemRec = WholeMerger.GetMemRec();
                const bool allowKeepFlags = HullCtx->AllowKeepFlags;

                for (const TPerSstCur& cur : CurSsts) {
                    NGc::TKeepStatus keep = BarriersEssence->Keep(CurKey, wholeMemRec,
                        {cur.NumKeepFlags, cur.NumDoNotKeepFlags >> 1, wholeKeep, wholeDoNotKeep},
                        allowKeepFlags, AllowGarbageCollection);

                    TSstRatioPtr& ratio = Ratios[cur.Sst];
                    if (!ratio) {
                        ratio = MakeIntrusive<TSstRatio>();
                    }
                    const ui64 indexItemByteSize = sizeof(TKey) + sizeof(TMemRec);
                    ratio->IndexItemsTotal++;
                    ratio->IndexBytesTotal += indexItemByteSize;
                    ratio->InplacedDataTotal += cur.InplacedDataSize;
                    ratio->HugeDataTotal += cur.HugeDataSize;
                    if (keep.KeepIndex) {
                        ratio->IndexItemsKeep++;
                        ratio->IndexBytesKeep += indexItemByteSize;
                    }
                    if (keep.KeepData) {
                        ratio->InplacedDataKeep += cur.InplacedDataSize;
                        ratio->HugeDataKeep += cur.HugeDataSize;
                    }
                }
            }

        private:
            // Per distinct source SST seen for the CURRENT key. One record per key per SST, so raw
            // keep-flag counters (NumDoNotKeepFlags uses the x2 encoding of TRecordMergerBase) are
            // sufficient — the subset merger's merged memRec is never used by Keep().
            struct TPerSstCur {
                const TLevelSegment* Sst = nullptr;
                ui32 NumKeepFlags = 0;
                ui32 NumDoNotKeepFlags = 0;
                ui64 InplacedDataSize = 0;
                ui64 HugeDataSize = 0;
            };

            void SetCurKey(const TKey& key) {
                Y_DEBUG_ABORT_UNLESS(!CurKeyValid || CurKey == key);
                CurKey = key;
                CurKeyValid = true;
            }

            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot& LevelSnap;
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
            const bool AllowGarbageCollection;
            const TBlobStorageGroupType GType;

            TIndexRecordMerger WholeMerger;   // whole-DB counts + merged memRec (== legacy dbMerger)
            TStackVec<TPerSstCur, 32> CurSsts; // per-key scratch, reset in Clear()
            TKey CurKey;
            bool CurKeyValid = false;
            THashMap<const TLevelSegment*, TSstRatioPtr> Ratios;
        };

    } // NHullComp
} // NKikimr
