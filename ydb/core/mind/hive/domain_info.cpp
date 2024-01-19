#include "domain_info.h"

namespace NKikimr {
namespace NHive {

ENodeSelectionPolicy TDomainInfo::GetNodeSelectionPolicy() const {
    if (ServerlessComputeResourcesMode.Empty()) {
        return ENodeSelectionPolicy::Default;
    }

    switch (*ServerlessComputeResourcesMode) {
        case NKikimrSubDomains::EServerlessComputeResourcesModeExclusive:
            return ENodeSelectionPolicy::PreferObjectDomain;
        case NKikimrSubDomains::EServerlessComputeResourcesModeShared:
            return ENodeSelectionPolicy::Default;
        default:
            return ENodeSelectionPolicy::Default;
    }
}

} // NHive
} // NKikimr
