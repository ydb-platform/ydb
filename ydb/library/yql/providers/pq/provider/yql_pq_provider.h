#pragma once
#include "yql_pq_settings.h"
#include "yql_pq_gateway.h"

#include <ydb/library/yql/core/yql_data_provider.h>
#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
}

namespace NYql {

struct TPqState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TPqState>;

    struct TTopicMeta {
        TPositionHandle Pos;
        bool RawFormat = true;
        TExprNode::TPtr RowSpec;
        TExprNode::TPtr ColumnOrder;
        TMaybe<::NPq::NConfigurationManager::TTopicDescription> Description;
    };

public:
    explicit TPqState(const TString& sessionId)
        : SessionId(sessionId)
    {
    }

    const TTopicMeta* FindTopicMeta(const TString& cluster, const TString& topicPath) const;
    const TTopicMeta* FindTopicMeta(const NNodes::TPqTopic& topic) const {
        return FindTopicMeta(topic.Cluster().StringValue(), topic.Path().StringValue());
    }

    bool IsRtmrMode() const {
        if (!SupportRtmrMode) {
            return false;
        }
        return Configuration->PqReadByRtmrCluster_.Get() != "dq";
    }

public:
    bool SupportRtmrMode = false;
    const TString SessionId;
    THashMap<std::pair<TString, TString>, TTopicMeta> Topics;

    TTypeAnnotationContext* Types = nullptr;
    TPqConfiguration::TPtr Configuration = MakeIntrusive<TPqConfiguration>();
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    IPqGateway::TPtr Gateway;
    THolder<IDqIntegration> DqIntegration;
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> DatabaseIds;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
};

TDataProviderInitializer GetPqDataProviderInitializer(
    IPqGateway::TPtr gateway,
    bool supportRtmrMode = false,
    std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr
);

} // namespace NYql
