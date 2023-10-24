#include "coordination_helper.h"

namespace NYql {

ICoordinationHelper::TPtr CreateCoordiantionHelper(const NProto::TDqConfig::TYtCoordinator& config, const TString& role)
{
    Y_UNUSED(config);
    Y_UNUSED(role);

    return nullptr;
}

} // namespace NYql
