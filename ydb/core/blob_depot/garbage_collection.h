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

        THashMap<std::tuple<ui64, ui8>, TBarrier> Barriers;

    private:
        class TTxCollectGarbage;

    public:
        TBarrierServer(TBlobDepot *self)
            : Self(self)
        {}

        void AddBarrierOnLoad(ui64 tabletId, ui8 channel, TGenStep softGenCtr, TGenStep soft, TGenStep hardGenCtr, TGenStep hard);
        void AddBarrierOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBarrier& barrier, NTabletFlatExecutor::TTransactionContext& txc);
        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);
        bool CheckBlobForBarrier(TLogoBlobID id) const;
        void GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const;
        void OnDataLoaded();

        template<typename TCallback>
        void Enumerate(TCallback&& callback) {
            for (const auto& [key, value] : Barriers) {
                const auto& [tabletId, channel] = key;
                callback(tabletId, channel, value.SoftGenCtr, value.Soft, value.HardGenCtr, value.Hard);
            }
        }
    };

} // NKikimr::NBlobDepot
