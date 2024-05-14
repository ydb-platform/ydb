#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullmergeits.h>

#include <util/generic/bitmap.h>


namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // GC map - we build a map of keys subject for garbage collection
    ////////////////////////////////////////////////////////////////////////////
    template <class TKey, class TMemRec>
    class TGcMap : public TThrRefBase {
    public:
        typedef ::NKikimr::TLevelIndex<TKey, TMemRec> TLevelIndex;
        typedef ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec> TLevelIndexSnapshot;
        typedef ::NKikimr::TGcMap<TKey, TMemRec> TThis;
        typedef ::NKikimr::TIndexRecordMerger<TKey, TMemRec> TIndexRecordMerger;
        typedef typename TLevelIndexSnapshot::TForwardIterator TLevelIt;


        //////////////// Statistics ////////////////////////////////////////////
        struct TStat {
            // build plan stat
            ui64 ItemsTotal = 0;
            ui64 ItemsKeepIndex = 0;
            ui64 ItemsKeepData = 0;

            TStringStream KeepItemsDbgStream;
            ui64 MaxKeepItemsDbg = 0;

            TStat(ui64 maxKeepItemsDbg = 0)
                : MaxKeepItemsDbg(maxKeepItemsDbg)
            {
                KeepItemsDbgStream << "{KEEP:";
            }

            void Finish() {
                KeepItemsDbgStream << "}";
            }

            TString ToString() const {
                TStringStream str;
                str << "{Total# " << ItemsTotal << " IndexKeep# " << ItemsKeepIndex << " DataKeep# " << ItemsKeepData;
                str << " " << KeepItemsDbgStream.Str() << "}";
                return str.Str();
            }

            void Update(const TKey &key, NGc::TKeepStatus keep) {
                ItemsTotal++;
                ItemsKeepIndex += ui64(keep.KeepIndex);
                ItemsKeepData += ui64(keep.KeepData);

                if (keep.KeepIndex && MaxKeepItemsDbg) {
                    --MaxKeepItemsDbg;
                    KeepItemsDbgStream << " " << key.ToString();
                }
            }
        };
        //////////////// Statistics ////////////////////////////////////////////


        //////////////// Iterator //////////////////////////////////////////////
        class TIterator {
        public:
            TIterator()
                : GcMap(nullptr)
                , Pos(0)
            {}

            TIterator(const TThis *gcMap)
                : GcMap(gcMap)
                , Pos(0)
            {
                Y_DEBUG_ABORT_UNLESS(GcMap);
            }

            void Next() {
                Y_DEBUG_ABORT_UNLESS(Valid());
                Pos++;
            }

            bool Valid() const {
                return GcMap && (Pos < GcMap->Stat.ItemsTotal);
            }

            bool KeepItem() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                bool keepIndex = GcMap->IndexKeepMap.Get(Pos);
                bool keepData = GcMap->DataKeepMap.Get(Pos);
                return keepIndex || keepData;
            }

            bool KeepData() const {
                Y_DEBUG_ABORT_UNLESS(Valid());
                bool keepData = GcMap->DataKeepMap.Get(Pos);
                return keepData;
            }

        private:
            const TThis *GcMap;
            ui64 Pos;
        };
        //////////////// Iterator //////////////////////////////////////////////


        TGcMap(TIntrusivePtr<THullCtx> &&hullCtx, ui64 incomingElementsApproximation, bool allowGarbageCollection)
            : HullCtx(std::move(hullCtx))
            , IncomingElementsApproximation(incomingElementsApproximation)
            , IndexKeepMap()
            , DataKeepMap()
            , Stat()
            , AllowGarbageCollection(allowGarbageCollection)
        {}

        // Prepares a map of keep/don't keep commands for every record
        template <class TIterator>
        void BuildMap(
                const TActorContext &ctx,
                TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> barriersEssence,
                const TLevelIndexSnapshot &levelSnap,
                const TIterator &i)
        {
            // FIXME
            // subsMerger must return circaLsn
            // dbMerger must return max circaLsn for the record with _data_
            // we must switch to a special kind of TIndexRecordMerger
            auto newItem = [] (const TIterator &subsIt, const TIndexRecordMerger &subsMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
            };

            auto doMerge = [this, barriersEssence] (const TIterator &subsIt, const TLevelIt &dbIt,
                                                    const TIndexRecordMerger &subsMerger,
                                                    const TIndexRecordMerger &dbMerger) {
                Y_UNUSED(subsIt);
                Y_UNUSED(subsMerger);
                bool allowKeepFlags = HullCtx->AllowKeepFlags;
                NGc::TKeepStatus keep = barriersEssence->Keep(dbIt.GetCurKey(), dbMerger.GetMemRec(),
                    {subsMerger, dbMerger}, allowKeepFlags, AllowGarbageCollection);
                Stat.Update(dbIt.GetCurKey(), keep);
                if (keep.KeepIndex) {
                    IndexKeepMap.Set(Stat.ItemsTotal - 1);
                }
                if (keep.KeepData) {
                    DataKeepMap.Set(Stat.ItemsTotal - 1);
                }
            };

            using namespace std::placeholders;
            auto crash = std::bind(MergeIteratorWithWholeDbDefaultCrashReport<const TIterator &, const TLevelIt &>,
                                   HullCtx->VCtx->VDiskLogPrefix, _1, _2);

            IndexKeepMap.Reserve(IncomingElementsApproximation);
            DataKeepMap.Reserve(IncomingElementsApproximation);

            TIterator subsIt(i);
            subsIt.SeekToFirst();
            TLevelIt levelIt(HullCtx, &levelSnap);
            MergeIteratorWithWholeDb<TIterator, TLevelIt, TIndexRecordMerger>(
                    HullCtx->VCtx->Top->GType, subsIt, levelIt, newItem, doMerge, crash);
            Stat.Finish();
            LOG_INFO(ctx, NKikimrServices::BS_HULLCOMP,
                    VDISKP(HullCtx->VCtx->VDiskLogPrefix,
                            "TGcMap: map build: %s", Stat.ToString().data()));
        }

        const TStat &GetStat() const {
            return Stat;
        }

    private:
        TIntrusivePtr<THullCtx> HullCtx;
        const ui64 IncomingElementsApproximation;
        TDynBitMap IndexKeepMap;
        TDynBitMap DataKeepMap;
        TStat Stat;
        const bool AllowGarbageCollection;
    };


    template <class TKey, class TMemRec>
    TIntrusivePtr<TGcMap<TKey, TMemRec>> CreateGcMap(
                TIntrusivePtr<THullCtx> hullCtx,
                ui64 incomingElementsApproximation,
                bool allowGarbageCollection) {
        return new TGcMap<TKey, TMemRec>(std::move(hullCtx), incomingElementsApproximation, allowGarbageCollection);
    }

} // NKikimr
