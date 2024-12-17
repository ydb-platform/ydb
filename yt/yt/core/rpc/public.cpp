#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;
const TMutationId NullMutationId;

const std::string RootUserName("root");

const TString RequestIdAnnotation("rpc.request_id");
const TString EndpointAnnotation("rpc.endpoint");
const TString RequestInfoAnnotation("rpc.request_info");
const TString RequestUser("rpc.request_user");
const TString ResponseInfoAnnotation("rpc.response_info");

const TString FeatureIdAttributeKey("feature_id");
const TString FeatureNameAttributeKey("feature_name");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
