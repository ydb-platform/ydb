#pragma once
#include <util/generic/string.h>
#include <map>
#include <ydb/core/tx/tiering/tier/identifier.h>

namespace NKikimr::NColumnShard {
namespace NTiers {
class TManager;
}

class ITiersManager {
public:
    const NTiers::TManager& GetManagerVerified(const NTiers::TExternalStorageId& tierId) const;
    virtual const NTiers::TManager* GetManagerOptional(const NTiers::TExternalStorageId& tierId) const = 0;
    virtual const std::map<NTiers::TExternalStorageId, NTiers::TManager>& GetManagers() const = 0;
    virtual ~ITiersManager() = default;
};

}
