#pragma once

#include "defs.h"
#include "flat_comp.h"
#include "flat_sausagecache.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    struct TCompactionReadState {
        using TPinned = THashMap<TLogoBlobID, THashMap<ui32, TIntrusivePtr<TPrivatePageCachePinPad>>>;

        TCompactionReadState(ui64 readId, THolder<NTable::ICompactionRead> read)
            : ReadId(readId)
            , Read(std::move(read))
        { }

        void Describe(IOutputStream& out) const noexcept {
            out << "CompactionRead{" << ReadId << "}";
        }

        const ui64 ReadId;
        const THolder<NTable::ICompactionRead> Read;
        ui64 Retries = 0;
        TPinned Pinned;
    };

}
}
