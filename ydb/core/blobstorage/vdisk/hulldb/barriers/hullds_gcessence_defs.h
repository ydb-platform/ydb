#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>

#include <ydb/library/actors/util/named_tuple.h>

namespace NKikimr {
    namespace NGc {

        //////////////////////////////////////////////////////////////////////////////////////////
        // TKeepStatus
        //////////////////////////////////////////////////////////////////////////////////////////
        struct TKeepStatus {
            // We need this index item (possibly for some internal reasons,
            // for instance to compact all records for the given key before deletion)
            bool KeepIndex;
            // We need this item together with data, client can use it
            bool KeepData;
            // Technical flag 'is we need to keep this item by barrier
            // (dispite keep/don't keep flags)'
            bool KeepByBarrier;

            TKeepStatus(bool keepIndex, bool keepData, bool keepByBarrier)
                : KeepIndex(keepIndex)
                , KeepData(keepData)
                , KeepByBarrier(keepByBarrier)
            {
                Y_DEBUG_ABORT_UNLESS(keepIndex >= keepData);
            }

            TKeepStatus(bool keepWholeRecord)
                : KeepIndex(keepWholeRecord)
                , KeepData(keepWholeRecord)
                , KeepByBarrier(keepWholeRecord)
            {}

            TKeepStatus(const TKeepStatus &) = default;
            TKeepStatus &operator=(const TKeepStatus &) = default;

            bool operator==(const TKeepStatus &s) const {
                return KeepIndex == s.KeepIndex &&
                    KeepData == s.KeepData &&
                    KeepByBarrier == s.KeepByBarrier;
            }

            bool KeepItem() const {
                Y_ABORT_UNLESS(KeepIndex >= KeepData);
                return KeepIndex;
            }

            void Output(IOutputStream &str) {
                str << "[index# " << KeepIndex << " data# " << KeepData
                    << " barrier# " << KeepByBarrier << "]";
            }
        };

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierKey -- structure that identifies entity for the garbage collector, that is tablet id and channel
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        struct TBarrierKey
            : public TNamedTupleBase<TBarrierKey>
        {
            ui64 TabletId = 0;
            ui32 Channel = 0;

            TBarrierKey() = default;
            TBarrierKey(const TBarrierKey& other) = default;

            TBarrierKey(ui64 tabletId, ui32 channel)
                : TabletId(tabletId)
                , Channel(channel)
            {}

            TBarrierKey(const TLogoBlobID& id)
                : TabletId(id.TabletID())
                , Channel(id.Channel())
            {}

            TBarrierKey(const TKeyBarrier& key)
                : TabletId(key.TabletId)
                , Channel(key.Channel)
            {}

            auto ConvertToTuple() const {
                return std::make_tuple(TabletId, Channel);
            }

            void Output(IOutputStream& str) const {
                str << TabletId << ":" << Channel;
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        //////////////////////////////////////////////////////////////////////////////////////////
        // TBarrier
        //////////////////////////////////////////////////////////////////////////////////////////
        struct TBarrier
            : public TNamedTupleBase<TBarrier>
        {
            ui32 BarrierGen = 0;        // generation of tablet that issued this barrier
            ui32 BarrierGenCounter = 0; // in-generation counter (step) of this entry
            ui32 CollectGen = 0;        // generation
            ui32 CollectStep = 0;       // step

            TBarrier() = default;
            TBarrier(const TBarrier& other) = default;

            TBarrier(ui32 barrierGen, ui32 barrierGenCounter, ui32 collectGen, ui32 collectStep)
                : BarrierGen(barrierGen)
                , BarrierGenCounter(barrierGenCounter)
                , CollectGen(collectGen)
                , CollectStep(collectStep)
            {}

            TBarrier(const TKeyBarrier& key, const TMemRecBarrier& memRec)
                : BarrierGen(key.Gen)
                , BarrierGenCounter(key.GenCounter)
                , CollectGen(memRec.CollectGen)
                , CollectStep(memRec.CollectStep)
            {}

            auto ConvertToTuple() const {
                return std::make_tuple(BarrierGen, BarrierGenCounter, CollectGen, CollectStep);
            }

            // checks whether the barrier is set; the default value is treated as empty
            operator bool() const {
                return *this != TBarrier();
            }

            void Output(IOutputStream& str) const {
                str << "Issued:[" << BarrierGen << ":" << BarrierGenCounter << "]" << " -> Collect:[" << CollectGen << ":" << CollectStep << "]";
            }

            TString ToString() const {
                TStringStream str;
                Output(str);
                return str.Str();
            }
        };

        //////////////////////////////////////////////////////////////////////////////////////////
        // TFindResult
        //////////////////////////////////////////////////////////////////////////////////////////
        struct TFindResult {
            bool EntryFound;
            TBarrier SoftBarrier;
            TBarrier HardBarrier;

            TFindResult()
                : EntryFound(false)
            {}

            TFindResult(const TBarrier& soft, const TBarrier& hard)
                : EntryFound(true)
                , SoftBarrier(soft)
                , HardBarrier(hard)
            {}
        };

        //////////////////////////////////////////////////////////////////////////////////////
        // TBuildStat
        //////////////////////////////////////////////////////////////////////////////////////
        struct TBuildStat {
            ui64 SkipNum = 0;
            ui64 AddNum = 0;
            ui64 NotSyncedNum = 0;
            TStringStream DbgStream;

            void Init(const THullCtxPtr &hullCtx, int debugLevel) {
                if (debugLevel > 0) {
                    DbgStream << hullCtx->VCtx->VDiskLogPrefix;
                }
            }
        };

        //////////////////////////////////////////////////////////////////////////////////////
        // TEssence
        //////////////////////////////////////////////////////////////////////////////////////
        class TEssence;

        //////////////////////////////////////////////////////////////////////////////////////
        // CompleteDelCmd
        //////////////////////////////////////////////////////////////////////////////////////
        inline bool CompleteDelCmd(ui32 collectGeneration, ui32 collectStep) {
            return collectGeneration == Max<ui32>() && collectStep == Max<ui32>();
        }

    } // NGc
} // NKikimr
