#include "domain_info.h"

namespace NKikimr {
namespace NHive {

ENodeSelectionPolicy TDomainInfo::GetNodeSelectionPolicy() const {
    if (!ServerlessComputeResourcesMode) {
        return ENodeSelectionPolicy::Default;
    }

    switch (*ServerlessComputeResourcesMode) {
        case NKikimrSubDomains::SERVERLESS_COMPUTE_RESOURCES_MODE_EXCLUSIVE:
            return ENodeSelectionPolicy::PreferObjectDomain;
        case NKikimrSubDomains::SERVERLESS_COMPUTE_RESOURCES_MODE_SHARED:
            return ENodeSelectionPolicy::Default;
        default:
            return ENodeSelectionPolicy::Default;
    }
}

} // NHive
} // NKikimr
