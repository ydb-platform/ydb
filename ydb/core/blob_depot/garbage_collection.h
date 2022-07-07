#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TBarrierServer {
        TBlobDepot* const Self;

        struct TBarrier {
            ui64 LastRecordGenStep = 0;
            ui64 Soft = 0;
            ui64 Hard = 0;
        };

        THashMap<std::pair<ui64, ui8>, TBarrier> Barriers;

    private:
        class TTxCollectGarbage;

    public:
        TBarrierServer(TBlobDepot *self)
            : Self(self)
        {}

        void AddBarrierOnLoad(ui64 tabletId, ui8 channel, ui64 lastRecordGenStep, ui64 soft, ui64 hard);
        void Handle(TEvBlobDepot::TEvCollectGarbage::TPtr ev);
        bool CheckBlobForBarrier(TLogoBlobID id) const;
        void GetBlobBarrierRelation(TLogoBlobID id, bool *underSoft, bool *underHard) const;

        template<typename TCallback>
        void Enumerate(TCallback&& callback) {
            for (const auto& [key, value] : Barriers) {
                callback(key.first, key.second, static_cast<ui32>(value.LastRecordGenStep >> 32),
                    static_cast<ui32>(value.LastRecordGenStep), static_cast<ui32>(value.Soft >> 32),
                    static_cast<ui32>(value.Soft), static_cast<ui32>(value.Hard >> 32), static_cast<ui32>(value.Hard));
            }
        }
    };

} // NKikimr::NBlobDepot
