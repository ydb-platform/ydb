#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer {
        TBlobDepot* const Self;

        struct TBarrier {
            TGenStep SoftGenCtr;
            TGenStep Soft;
            TGenStep HardGenCtr;
            TGenStep Hard;
            std::deque<std::unique_ptr<TEvBlobDepot::TEvCollectGarbage::THandle>> ProcessingQ;
        };

        THashMap<std::pair<ui64, ui8>, TBarrier> Barriers;

    private:
        class TTxCollectGarbage;

    public:
        TBarrierServer(TBlobDepot *self)
            : Self(self)
        {}

        void AddBarrierOnLoad(const TBlobDepot::TBarrier& barrier);
        void AddBarrierOnDecommit(const TBlobDepot::TBarrier& barrier, NTabletFlatExecutor::TTransactionContext& txc);
        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);
        bool CheckBlobForBarrier(TLogoBlobID id) const;
        void GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const;
        void OnDataLoaded();

        template<typename TCallback>
        void Enumerate(TCallback&& callback) {
            for (const auto& [key, value] : Barriers) {
                callback(key.first, key.second, value.SoftGenCtr, value.Soft, value.HardGenCtr, value.Hard);
            }
        }
    };

} // NKikimr::NBlobDepot
