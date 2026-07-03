#include "ddisk_helpers.h"

namespace NYdb::NBS::NBlockStore::NStorage {

bool TDDiskIdLess::operator()(const TDDiskId& lh, const TDDiskId& rh) const
{
    auto makeTuple = [](const TDDiskId& item)
    {
        return std::make_tuple(
            item.GetNodeId(),
            item.GetPDiskId(),
            item.GetDDiskSlotId());
    };
    return makeTuple(lh) < makeTuple(rh);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage
