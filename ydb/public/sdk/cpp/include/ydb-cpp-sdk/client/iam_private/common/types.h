#pragma once

#include <ydb-cpp-sdk/client/iam/common/types.h>

namespace NYdb::inline Dev {

struct TIamServiceParams : TIamEndpoint {
    std::string ServiceId;
    std::string MicroserviceId;
    std::string ResourceId;
    std::string ResourceType;
    std::string TargetServiceAccountId;
};

}
