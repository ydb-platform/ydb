#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullmergeits.h>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyStorageRatio
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyStorageRatio {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
            typedef typename TLevelSliceSnapshot::TSstIterator TSstIterator;
            typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
            typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
            typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
            typedef typename TLevelSegment::TMemIterator TMemIterator;
            typedef typename TLevelIndexSnapshot::TForwardIterator TLevelIt;
            typedef ::NKikimr::TIndexRecordMerger<TKey, TMemRec> TIndexRecordMerger;

            TStrategyStorageRatio(TIntrusivePtr<THullCtx> hullCtx,
                                  const TLevelIndexSnapshot &levelSnap,
                                  TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> &&barriersEssence,
                                  bool allowGarbageCollection)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , BarriersEssence(std::move(barriersEssence))
                , AllowGarbageCollection(allowGarbageCollection)
            {}


            void Work() {
                TInstant startTime(TAppData::TimeProvider->Now());
                TStat stat;
                UpdateStorageRatioForDb(startTime, stat);
                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    LOG_INFO(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP,
                            VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                                "%s: StorageRatio: timeSpent# %s stat# %s",
                                PDiskSignatureForHullDbKey<TKey>().ToString().data(),
                                (finishTime - startTime).ToString().data(), stat.ToString().data()));
                }

                BarriersEssence.Reset();
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
            const bool AllowGarbageCollection;

            struct TStat {
                ui32 SstsChecked = 0;
                bool BreakedActualRatio = false;
                bool BreakedTimeout = false;

                TString ToString() const {
                    auto bool2str = [] (bool v) { return v ? "true" : "false"; };
                    return Sprintf("{SstsChecked# %" PRIu32 " BreakedActualRatio# %s "
                                   "BreakedTimeout# %s}", SstsChecked, bool2str(BreakedActualRatio),
                                   bool2str(BreakedTimeout));
                }
            };

            struct TTimeSst {
                ui64 Seconds;
                TLevelSstPtr LevelSstPtr;

                TTimeSst(ui64 seconds, const TLevelSstPtr &p)
                    : Seconds(seconds)
                    , LevelSstPtr(p)
                {}

                bool operator < (const TTimeSst &s) const {
                    return Seconds < s.Seconds;
                }
            };

            void OrderSstByStorageRatioTime(TVector<TTimeSst> &vec) {
                vec.clear();
                TSstIterator it(&LevelSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    ui64 seconds = p.SstPtr->StorageRatio.GetTime().Seconds();
                    vec.push_back(TTimeSst(seconds, p));
                    it.Next();
                }
                Sort(vec.begin(), vec.end());
            }

            void UpdateStorageRatioForDb(TInstant startTime, TStat &stat) {
                const TDuration &calcPeriod = HullCtx->HullCompStorageRatioCalcPeriod;
                const TDuration &calcDuration = HullCtx->HullCompStorageRatioMaxCalcDuration;

                // order all ssts (including level 0) by storage ratio calculation time
                TVector<TTimeSst> vec;
                vec.reserve(1000u);
                OrderSstByStorageRatioTime(vec);

                // calculate storage ratio, don't spend much time on it, skip ssts that are actualized
                for (const auto &x : vec) {
                    const TInstant time = x.LevelSstPtr.SstPtr->StorageRatio.GetTime();
                    if (startTime >= time + calcPeriod) {
                        TSstRatioPtr newRatio = CalculateSstRatio(x.LevelSstPtr.SstPtr, startTime);
                        x.LevelSstPtr.SstPtr->StorageRatio.Set(newRatio);
                        stat.SstsChecked++;
                    } else {
                        stat.BreakedActualRatio = true;
                        break;
                    }

                    // avoid spending too much time on storage ratio calculation
                    TInstant now = TAppData::TimeProvider->Now();
                    if (now > startTime + calcDuration) {
                        stat.BreakedTimeout = true;
                        break;
                    }
                }

                BarriersEssence.Reset();
            }

            TSstRatioPtr CalculateSstRatio(TLevelSegmentPtr sst, TInstant now) {
                TSstRatioPtr r = MakeIntrusive<TSstRatio>(now);
                TSstRatio *ratio = r.Get();

                // the subset we processing
                TMemIterator subsIt(sst.Get());
                subsIt.SeekToFirst();
                // for the whole level index
                TLevelIt dbIt(HullCtx, &LevelSnap);

                auto newItem = [] (const TMemIterator &subsIt, const TIndexRecordMerger &subsMerger) {
                    Y_UNUSED(subsIt);
                    Y_UNUSED(subsMerger);
                };

                auto doMerge = [this, ratio] (const TMemIterator &subsIt,
                                              const TLevelIt &dbIt,
                                              const TIndexRecordMerger &subsMerger,
                                              const TIndexRecordMerger &dbMerger) {
                    Y_UNUSED(subsIt);
                    Y_UNUSED(subsMerger);
                    TDiskDataExtractor extr;
                    subsIt.GetDiskData(&extr);
                    // calculate item's parameters
                    const ui64 indexItemByteSize = sizeof(TKey) + sizeof(TMemRec);
                    const ui64 inplacedDataSize = extr.GetInplacedDataSize();
                    const ui64 hugeDataSize = extr.GetHugeDataSize();
                    // update ratio
                    ratio->IndexItemsTotal++;
                    ratio->IndexBytesTotal += indexItemByteSize;
                    ratio->InplacedDataTotal += inplacedDataSize;
                    ratio->HugeDataTotal += hugeDataSize;
                    // calculate keep status
                    bool allowKeepFlags = HullCtx->AllowKeepFlags;
                    NGc::TKeepStatus keep = BarriersEssence->Keep(dbIt.GetCurKey(),
                                                                  dbMerger.GetMemRec(),
                                                                  {subsMerger, dbMerger},
                                                                  allowKeepFlags,
                                                                  AllowGarbageCollection);
                    if (keep.KeepIndex) {
                        // calculate index overhead
                        ratio->IndexItemsKeep++;
                        ratio->IndexBytesKeep += indexItemByteSize;
                    }
                    if (keep.KeepData) {
                        // calculate data overhead
                        ratio->InplacedDataKeep += inplacedDataSize;
                        ratio->HugeDataKeep += hugeDataSize;
                    }
                };

                auto crash = [ratio, this] (const TMemIterator &subsIt, const TLevelIt &dbIt) {
                    TStringStream str;
                    str << MergeIteratorWithWholeDbDefaultCrashReport(HullCtx->VCtx->VDiskLogPrefix,
                                                                      subsIt, dbIt);
                    str << " Ratio:   " << ratio->ToString() << "\n";
                    return str.Str();
                };

                MergeIteratorWithWholeDb<TMemIterator, TLevelIt, TIndexRecordMerger>(
                            HullCtx->VCtx->Top->GType, subsIt, dbIt, newItem, doMerge, crash);
                return r;
            }
        };

    } // NHullComp
} // NKikimr
