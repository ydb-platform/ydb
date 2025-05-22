#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>

namespace NKikimr {

    class TBarrierCache {
        using TKey = std::pair<ui64, ui32>; // (tablet, channel)

        struct TCollectBarrier {
            ui32 Gen = 0;
            ui32 Step = 0;

            TCollectBarrier(ui32 gen, ui32 step)
                : Gen(gen)
                , Step(step)
            {}

            TCollectBarrier() = default;
            TCollectBarrier(const TCollectBarrier& other) = default;
            TCollectBarrier &operator=(const TCollectBarrier& other) = default;

            auto ConvertToTuple() const {
                return std::make_tuple(Gen, Step);
            }

            TString ToString() const {
                TStringStream str;
                str << "{Gen# " << Gen << " Step# " << Step << "}";
                return str.Str();
            }
        };

        struct TValue {
            TMaybe<TCollectBarrier> Soft;
            TMaybe<TCollectBarrier> Hard;

            TString ToString() const {
                TStringStream str;
                str << "{Soft# " << (Soft ? Soft->ToString() : "<not set>")
                    << " Hard# " << (Hard ? Hard->ToString() : "<not set>")
                    << "}";
                return str.Str();
            }
        };

        THashMap<TKey, TValue> Cache;

    public:
        void Build(const THullDs *hullDs) {
            // take a snapshot of all barriers; we don't care about LSN's here, because there should be no data in fresh
            // segment at this point of time
            TBarriersSnapshot snapshot(hullDs->Barriers->GetIndexSnapshot());

            // create iterator and start traversing the whole barrier database
            TBarriersSnapshot::TForwardIterator it(hullDs->HullCtx, &snapshot);
            THeapIterator<TKeyBarrier, TMemRecBarrier, true> heapIt(&it);
            TIndexRecordMerger<TKeyBarrier, TMemRecBarrier> merger(hullDs->HullCtx->VCtx->Top->GType);
            auto callback = [&] (TKeyBarrier key, auto* merger) -> bool {
                const TMemRecBarrier& memRec = merger->GetMemRec();
                if (!hullDs->HullCtx->GCOnlySynced || memRec.Ingress.IsQuorum(hullDs->HullCtx->IngressCache.Get()) ||
                        key.Hard) {
                    Update(key.TabletId, key.Channel, key.Hard, memRec.CollectGen, memRec.CollectStep);
                }
                return true;
            };
            heapIt.Walk(TKeyBarrier::First(), &merger, callback);
        }

        void Update(ui64 tabletId, ui32 channel, bool hard, ui32 collectGen, ui32 collectStep) {
            const TKey key(tabletId, channel);
            TValue& value = Cache[key];
            TMaybe<TCollectBarrier>& barrier = hard ? value.Hard : value.Soft;
            if (!barrier || std::make_tuple(collectGen, collectStep) > barrier->ConvertToTuple()) {
                barrier = TCollectBarrier(collectGen, collectStep);
            }
        }

        bool Keep(const TLogoBlobID& id, bool keepByIngress, TString *explanation = nullptr) const {
            const TKey key(id.TabletID(), id.Channel());
            auto it = Cache.find(key);
            if (it == Cache.end()) {
                return true;
            }
            const TValue& value = it->second;
            if (explanation) {
                *explanation = value.ToString();
            }
            auto position = std::make_tuple(id.Generation(), id.Step());
            return (!value.Hard || position > value.Hard->ConvertToTuple()) &&
                (keepByIngress || !value.Soft || position > value.Soft->ConvertToTuple());
        }
    };

} // NKikimr
