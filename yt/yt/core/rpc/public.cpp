#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;
const TMutationId NullMutationId;

const std::string RootUserName("root");

const std::string RequestIdAnnotation("rpc.request_id");
const std::string EndpointAnnotation("rpc.endpoint");
const std::string RequestInfoAnnotation("rpc.request_info");
const std::string RequestUser("rpc.request_user");
const std::string ResponseInfoAnnotation("rpc.response_info");

const std::string FeatureIdAttributeKey("feature_id");
const std::string FeatureNameAttributeKey("feature_name");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
