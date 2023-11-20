#include "flat_part_outset.h"
#include "flat_part_loader.h"

namespace NKikimr {
namespace NTable {

void TPageCollectionComponents::ParsePacket(TSharedData meta) {
    Y_DEBUG_ABORT_UNLESS(!Packet, "Packet is already parsed");

    Packet = new NPageCollection::TPageCollection(LargeGlobId, std::move(meta));
}

TEpoch TPartComponents::GetEpoch() const {
    if (Epoch != TEpoch::Max()) {
        return Epoch;
    }

    Y_ABORT_UNLESS(PageCollectionComponents && PageCollectionComponents[0].Packet,
        "PartComponents has neither a known epoch, nor a parsed meta packet");

    return TLoader::GrabEpoch(*this);
}

}
}
