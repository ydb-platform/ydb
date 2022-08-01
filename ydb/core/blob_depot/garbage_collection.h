#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer {
        TBlobDepot* const Self;

        struct TBarrier {
            ui32 RecordGeneration = 0;
            ui32 PerGenerationCounter = 0;
            TGenStep Soft;
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

        void AddBarrierOnLoad(ui64 tabletId, ui8 channel, ui32 recordGeneration, ui32 perGenerationCounter, TGenStep soft,
            TGenStep hard);
        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);
        bool CheckBlobForBarrier(TLogoBlobID id) const;
        void GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const;
        void OnDataLoaded();

        template<typename TCallback>
        void Enumerate(TCallback&& callback) {
            for (const auto& [key, value] : Barriers) {
                callback(key.first, key.second, value.RecordGeneration, value.PerGenerationCounter, value.Soft, value.Hard);
            }
        }
    };

} // NKikimr::NBlobDepot
