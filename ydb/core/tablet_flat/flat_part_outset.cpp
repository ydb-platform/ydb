#include "flat_part_outset.h"
#include "flat_part_loader.h"

namespace NKikimr {
namespace NTable {

void TPageCollectionComponents::ParsePageCollection(TSharedData meta) {
    Y_DEBUG_ABORT_UNLESS(!PageCollection, "PageCollection is already parsed");

    PageCollection = new NPageCollection::TPageCollection(LargeGlobId, std::move(meta));
}

TEpoch TPartComponents::GetEpoch() const {
    if (Epoch != TEpoch::Max()) {
        return Epoch;
    }

    Y_ENSURE(PageCollectionComponents && PageCollectionComponents[0].PageCollection,
        "PartComponents has neither a known epoch, nor a parsed meta packet");

    return TLoader::GrabEpoch(*this);
}

}
}
