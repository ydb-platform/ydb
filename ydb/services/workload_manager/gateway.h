#pragma once

#include <ydb/services/workload_manager/query_classifier.h>

#include <util/generic/maybe.h>

#include <memory>


namespace NKikimr::NWorkloadManager {

class IGateway {
public:
    virtual ~IGateway() = default;

    virtual std::shared_ptr<IQueryClassifier> TryCreateQueryClassifier(
        const TString& databaseId, TClassifyContext context) = 0;
};

using TGatewayPtr = std::shared_ptr<IGateway>;

TGatewayPtr TryGetGateway(TMaybe<ui32> nodeId = Nothing());
TGatewayPtr GetGateway(TMaybe<ui32> nodeId = Nothing());

}
