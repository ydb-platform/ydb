#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

const TRequestId NullRequestId;
const TRealmId NullRealmId;
const TMutationId NullMutationId;

const std::string RootUserName("root");

const std::string RequestIdAnnotation("rpc.request_id");
const std::string EndpointAnnotation("rpc.endpoint");
const std::string EndpointAddressAnnotation("rpc.endpoint_address");
const std::string RequestAnnotationsTraceTag("rpc.request_annotations");
const std::string RequestUser("rpc.request_user");
const std::string ResponseAnnotationsTraceTag("rpc.response_annotations");

const std::string FeatureIdAttributeKey("feature_id");
const std::string FeatureNameAttributeKey("feature_name");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
