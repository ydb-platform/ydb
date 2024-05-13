#include "common.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStatistics {

TIdentifier::TIdentifier(const EType type, const std::vector<ui32>& entities)
    : Type(type)
    , EntityIds(entities)
{
    AFL_VERIFY(EntityIds.size());
}

bool TIdentifier::operator<(const TIdentifier& item) const {
    if (Type != item.Type) {
        return (ui32)Type < (ui32)item.Type;
    }
    for (ui32 i = 0; i < std::min(EntityIds.size(), item.EntityIds.size()); ++i) {
        if (EntityIds[i] < item.EntityIds[i]) {
            return true;
        }
    }
    return false;
}

bool TIdentifier::operator==(const TIdentifier& item) const {
    if (Type != item.Type) {
        return false;
    }
    if (EntityIds.size() != item.EntityIds.size()) {
        return false;
    }
    for (ui32 i = 0; i < EntityIds.size(); ++i) {
        if (EntityIds[i] != item.EntityIds[i]) {
            return false;
        }
    }
    return true;
}

}