#pragma once
#include <util/generic/string.h>
#include <map>

namespace NKikimr::NColumnShard {
namespace NTiers {
class TManager;
}

class ITiersManager {
public:
    const NTiers::TManager& GetManagerVerified(const TString& tierId) const;
    virtual const NTiers::TManager* GetManagerOptional(const TString& tierId) const = 0;
    virtual const std::map<TString, NTiers::TManager>& GetManagers() const = 0;
    virtual ~ITiersManager() = default;
};

}
