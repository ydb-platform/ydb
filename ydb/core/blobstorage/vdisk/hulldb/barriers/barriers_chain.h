#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

namespace NKikimr {
    namespace NBarriers {

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TCurrentBarrier
        ////////////////////////////////////////////////////////////////////////////////////////////
        struct TCurrentBarrier {
            ui32 Gen = 0;
            ui32 GenCounter = 0;
            ui32 CollectGen = 0;
            ui32 CollectStep = 0;

            TCurrentBarrier(ui32 gen, ui32 genCounter, ui32 collectGen, ui32 collectStep);
            void Output(IOutputStream& str) const;
            TString ToString() const;
            bool IsDead() { return CollectGen == Max<ui32>() && CollectStep == Max<ui32>(); }
            bool operator ==(const TCurrentBarrier &b) const {
                return Gen == b.Gen && GenCounter == b.GenCounter &&
                    CollectGen == b.CollectGen && CollectStep == b.CollectStep;
            }
        };


        ////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierChain -- update accumulator for [TableId, Channel, Soft/Hard]
        ////////////////////////////////////////////////////////////////////////////////////////////
        class TBarrierChain {
        public:
            ///////////////////////////////////////////////////////////////////////////////////////
            struct TTabletCmdId {
                ui32 Gen = 0;
                ui32 GenCounter = 0;

                TTabletCmdId() = default;
                TTabletCmdId(ui32 gen, ui32 genCounter);
                bool operator <(const TTabletCmdId &v) const;
                void Output(IOutputStream &str) const;
            };

            ////////////////////////////////////////////////////////////////////////////////////////
            struct TBarrierValue {
                ui32 CollectGen = 0;
                ui32 CollectStep = 0;

                TBarrierValue() = default;
                TBarrierValue(ui32 collectGen, ui32 collectStep);
                bool operator ==(const TBarrierValue &v) const;
                bool operator <(const TBarrierValue &v) const;
                void Output(IOutputStream &str) const;
            };

            ///////////////////////////////////////////////////////////////////////////////////////
            struct TTabletCmdValue {
                TBarrierValue BarrierValue;
                TBarrierIngress Ingress;

                TTabletCmdValue() = default;
                TTabletCmdValue(TBarrierValue barVal, TBarrierIngress ingress)
                    : BarrierValue(barVal)
                    , Ingress(ingress)
                {}
                void Output(IOutputStream &str, const TIngressCache *ingrCache) const;
            };


            ///////////////////////////////////////////////////////////////////////////////////////
            void Update(
                const TIngressCache *ingrCache,
                bool gcOnlySynced,
                const TKeyBarrier &key,
                const TMemRecBarrier &memRec);
            TMaybe<TCurrentBarrier> GetBarrier() const {
                return CurrentBarrier;
            }
            void Output(IOutputStream &str, const TIngressCache *ingrCache) const;

        private:
            using TChain = TMap<TTabletCmdId, TTabletCmdValue>;
            TChain Chain;
            TMaybe<TCurrentBarrier> CurrentBarrier;
        };

    } // NBarriers
} // NKikimr

