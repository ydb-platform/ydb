#pragma once
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/pq/cm_client/interface/client.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>

namespace NYql {

struct IPqGateway : public TThrRefBase {
    using TPtr = TIntrusivePtr<IPqGateway>;

    virtual NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) = 0;
    virtual void CloseSession(const TString& sessionId) = 0;

    // CM API.
    virtual ::NPq::NConfigurationManager::TAsyncDescribePathResult DescribePath(const TString& sessionId, const TString& cluster, const TString& database, const TString& path, const TString& token) = 0;
 
    virtual void UpdateClusterConfigs( 
        const TString& clusterName, 
        const TString& endpoint, 
        const TString& database, 
        bool secure) = 0; 
};

} // namespace NYql
