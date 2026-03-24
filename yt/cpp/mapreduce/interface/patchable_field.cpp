#include "patchable_field.h"

#include "client.h"

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

const TNode::TMapType& GetDynamicConfiguration(const IClientPtr& client, const TString& configProfile)
{
    return client->GetDynamicConfiguration(configProfile);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
