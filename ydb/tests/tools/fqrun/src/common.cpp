#include "common.h"

namespace NFqRun {

void SetupAcl(FederatedQuery::Acl* acl) {
    if (acl->visibility() == FederatedQuery::Acl::VISIBILITY_UNSPECIFIED) {
        acl->set_visibility(FederatedQuery::Acl::SCOPE);
    }
}

}  // namespace NFqRun
