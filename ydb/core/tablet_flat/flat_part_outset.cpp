#include "flat_part_outset.h"
#include "flat_part_loader.h"

namespace NKikimr {
namespace NTable {

    void TPageCollectionComponents::SetMeta(TSharedData raw) {
        if (raw) {
            SharedMeta = MakeAtomicShared<NPageCollection::TMeta>(std::move(raw), LargeGlobId.Group);
        } else {
            SharedMeta.Reset();
        }
    }

TEpoch TPartComponents::GetEpoch() const {
    if (Epoch != TEpoch::Max()) {
        return Epoch;
    }

    Y_ENSURE(PageCollectionComponents && PageCollectionComponents[0].SharedMeta,
        "PartComponents has neither a known epoch, nor metadata");

    return TLoader::GrabEpoch(*this);
}

}
}
