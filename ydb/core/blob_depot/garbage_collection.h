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
        bool AddBarrierOnDecommit(const TEvBlobStorage::TEvAssimilateResult::TBarrier& barrier, ui32& maxItems,
            NTabletFlatExecutor::TTransactionContext& txc, void *cookie);
        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);
        void GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const;
        void OnDataLoaded();

        void ValidateBlobInvariant(ui64 tabletId, ui8 channel);

        TString ToStringBarrier(ui64 tabletId, ui8 channel, bool hard) const {
            if (const auto it = Barriers.find(std::make_tuple(tabletId, channel)); it == Barriers.end()) {
                return "<none>";
            } else if (auto& b = it->second; hard) {
                return TStringBuilder() << "hard{" << b.HardGenCtr.ToString() << "=>" << b.Hard.ToString() << "}";
            } else {
                return TStringBuilder() << "soft{" << b.SoftGenCtr.ToString() << "=>" << b.Soft.ToString() << "}";
            }
        }

        template<typename TCallback>
        void Enumerate(TCallback&& callback) {
            for (const auto& [key, value] : Barriers) {
                const auto& [tabletId, channel] = key;
                callback(tabletId, channel, value.SoftGenCtr, value.Soft, value.HardGenCtr, value.Hard);
            }
        }
    };

} // NKikimr::NBlobDepot
