#include "object.h"
#include "behaviour.h"

#include <ydb/services/metadata/manager/ydb_value_operator.h>

namespace NKikimr::NColumnShard::NTiers {

TTieringRule::TFactory::TRegistrator<TTieringRule> TTieringRule::Registrator(NKikimrSchemeOp::EPathTypeTieringRule);

NKikimr::NOlap::TTiering TTieringRule::BuildOlapTiers() const {
    AFL_VERIFY(!GetIntervals().empty());
    NOlap::TTiering result;
    for (auto&& r : GetIntervals()) {
        AFL_VERIFY(result.Add(std::make_shared<NOlap::TTierInfo>(r.GetTierName(), r.GetDurationForEvict(), GetDefaultColumn())));
    }
    return result;
}

bool TTieringRule::ContainsTier(const TString& tierName) const {
    for (auto&& i : GetIntervals()) {
        if (i.GetTierName() == tierName) {
            return true;
        }
    }
    return false;
}

NMetadata::IClassBehaviour::TPtr TTieringRule::GetBehaviour() {
    static std::shared_ptr<TTieringRuleBehaviour> result = std::make_shared<TTieringRuleBehaviour>();
    return result;
}

}
