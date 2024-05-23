#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

#include <ydb/library/yql/providers/common/gateway/yql_provider_gateway.h>
#include <ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <ydb/library/yql/providers/dq/planner/execution_planner.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <ydb/library/yql/core/yql_udf_resolver.h>
#include <ydb/library/yql/core/yql_execution.h>

#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYql {

namespace NProto {
class TDqConfig;
}

class IDqGateway : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IDqGateway>;
    using TFileResource = Yql::DqsProto::TFile;
    using TDqProgressWriter = std::function<void(const TString&)>;

    struct TFileResourceHash {
        std::size_t operator()(const TFileResource& f) const {
            return std::hash<TString>()(f.GetName());
        }
    };

    struct TFileResourceEqual {
        bool operator()(const TFileResource& a, const TFileResource& b) const {
            return a.GetName() == b.GetName();
        }
    };

    using TUploadList = THashSet<TFileResource, TFileResourceHash, TFileResourceEqual>;

    class TResult: public NCommon::TOperationResult {
    public:
        TString Data;
        bool Fallback = false;
        bool ForceFallback = false;
        bool Retriable = false;
        bool Truncated = false;
        ui64 RowsCount = 0;

        TOperationStatistics Statistics;

        TResult() = default;
    };

    virtual ~IDqGateway() = default;

    virtual NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) = 0;

    // TODO(gritukan): Leave only CloseSessionAsync after Arcadia migration and make it pure virtual.
    virtual void CloseSession(const TString& sessionId) {
        Y_UNUSED(sessionId);
    }

    virtual NThreading::TFuture<void> CloseSessionAsync(const TString& sessionId) {
        Y_UNUSED(sessionId);
        return NThreading::MakeFuture();
    }

    virtual NThreading::TFuture<TResult>
    ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard, ui64 executionTimeout) = 0;

    virtual TString GetVanillaJobPath() {
        return "";
    }

    virtual TString GetVanillaJobMd5() {
        return "";
    }

    virtual void Stop() { }
};

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port);
TIntrusivePtr<IDqGateway> CreateDqGateway(const NProto::TDqConfig& config);

} // namespace NYql
