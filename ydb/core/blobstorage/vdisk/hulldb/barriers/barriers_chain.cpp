#include "barriers_chain.h"

namespace NKikimr {
    namespace NBarriers {

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TCurrentBarrier
        ////////////////////////////////////////////////////////////////////////////////////////////
        TCurrentBarrier::TCurrentBarrier(ui32 gen, ui32 genCounter, ui32 collectGen, ui32 collectStep)
            : Gen(gen)
            , GenCounter(genCounter)
            , CollectGen(collectGen)
            , CollectStep(collectStep)
        {}

        void TCurrentBarrier::Output(IOutputStream& str) const {
            str << "Issued:[" << Gen << ":" << GenCounter << "]"
                << " -> Collect:[" << CollectGen << ":" << CollectStep << "]";
        }

        TString TCurrentBarrier::ToString() const {
            TStringStream str;
            Output(str);
            return str.Str();
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierChain::TTabletCmdId
        ////////////////////////////////////////////////////////////////////////////////////////////
        TBarrierChain::TTabletCmdId::TTabletCmdId(ui32 gen, ui32 genCounter)
            : Gen(gen)
            , GenCounter(genCounter)
        {}

        bool TBarrierChain::TTabletCmdId::operator <(const TTabletCmdId &v) const {
            return Gen < v.Gen || (Gen == v.Gen && GenCounter < v.GenCounter);
        }
        void TBarrierChain::TTabletCmdId::Output(IOutputStream &str) const {
            str << "{Gen# " << Gen << " GenCounter# " << GenCounter << "}";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierChain::TBarrierValue
        ////////////////////////////////////////////////////////////////////////////////////////////
        TBarrierChain::TBarrierValue::TBarrierValue(ui32 collectGen, ui32 collectStep)
            : CollectGen(collectGen)
            , CollectStep(collectStep)
        {}

        bool TBarrierChain::TBarrierValue::operator ==(const TBarrierValue &v) const {
            return CollectGen == v.CollectGen && CollectStep == v.CollectStep;
        }

        bool TBarrierChain::TBarrierValue::operator <(const TBarrierValue &v) const {
            return CollectGen < v.CollectGen || (CollectGen == v.CollectGen && CollectStep < v.CollectStep);
        }

        void TBarrierChain::TBarrierValue::Output(IOutputStream &str) const {
            str << "{CollectGen# " << CollectGen << " CollectStep# " << CollectStep << "}";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierChain::TTabletCmdValue
        ////////////////////////////////////////////////////////////////////////////////////////////
        void TBarrierChain::TTabletCmdValue::Output(IOutputStream &str, const TIngressCache *ingrCache) const {
            str << "{BarrierValue# ";
            BarrierValue.Output(str);
            str << " Ingress# " << Ingress.ToString(ingrCache) << "}";
        }

        ////////////////////////////////////////////////////////////////////////////////////////////
        // TBarrierChain
        ////////////////////////////////////////////////////////////////////////////////////////////
        void TBarrierChain::Update(
                const TIngressCache *ingrCache,
                bool gcOnlySynced,
                const TKeyBarrier &key,
                const TMemRecBarrier &memRec)
        {
            TTabletCmdId newCmdId(key.Gen, key.GenCounter);
            TBarrierValue newBarVal(memRec.CollectGen, memRec.CollectStep);

            auto insRes = Chain.insert(TChain::value_type(newCmdId, TTabletCmdValue(newBarVal, memRec.Ingress)));
            // iterator to the element in map
            TChain::iterator &it = insRes.first;
            // stored value
            TTabletCmdValue &val = it->second;
            if (!insRes.second) {
                // already exists
                Y_ABORT_UNLESS(val.BarrierValue == newBarVal);
                // update stored ingress
                TBarrierIngress::Merge(val.Ingress, memRec.Ingress);
            }

            const bool noSync = !gcOnlySynced;
            if (val.Ingress.IsQuorum(ingrCache) || noSync || key.Hard) {
                if (CurrentBarrier.Empty() ||
                        std::make_tuple(CurrentBarrier->CollectGen, CurrentBarrier->CollectStep) <=
                        std::make_tuple(val.BarrierValue.CollectGen, val.BarrierValue.CollectStep)) {

                    CurrentBarrier = TCurrentBarrier(key.Gen, key.GenCounter, val.BarrierValue.CollectGen,
                            val.BarrierValue.CollectStep);
                    // remove prevs
                    for (auto b = Chain.begin(); b->first < it->first; b = Chain.begin()) {
                        Chain.erase(b);
                    }
                } else {
                    // TODO: tablet invalidates blobstorage contract
                }
            }
        }

        void TBarrierChain::Output(IOutputStream &str, const TIngressCache *ingrCache) const {
            str << "{CurrentBarrier# ";
            if (CurrentBarrier) {
                CurrentBarrier->Output(str);
            } else {
                str << "null";
            }
            str << " Chain# [";
            for (const auto &x : Chain) {
                str << " {CmdId# ";
                x.first.Output(str);
                str << " CmdValue# ";
                x.second.Output(str, ingrCache);
                str << "}";
            }
            str << "]}";
        }

    } // NBarriers
} // NKikimr
