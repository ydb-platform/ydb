#include "flat_part_outset.h"
#include "flat_part_loader.h"

namespace NKikimr {
namespace NTable {

TEpoch TPartComponents::GetEpoch() const {
    if (Epoch != TEpoch::Max()) {
        return Epoch;
    }

    Y_ENSURE(PageCollectionComponents && PageCollectionComponents[0].RawMeta,
        "PartComponents has neither a known epoch, nor raw meta data");

    return TLoader::GrabEpoch(*this);
}

}
}
