#pragma once

#include <ydb/library/yql/providers/common/db_id_async_resolver/database_type.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/pq/expr_nodes/yql_pq_expr_nodes.h>
#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_pos_handle.h>
#include <yql/essentials/core/yql_data_provider.h>

#include <util/generic/ptr.h>

namespace NKikimr::NMiniKQL {

class IFunctionRegistry;

} // namespace NKikimr::NMiniKQL

namespace NYql {

struct TPqConfiguration;

struct TPqState : public TThrRefBase {
    using TPtr = TIntrusivePtr<TPqState>;

    struct TTopicMeta {
        TPositionHandle Pos;
        bool RawFormat = true;
        TExprNode::TPtr RowSpec;
        TExprNode::TPtr ColumnOrder;
        TMaybe<IPqGateway::TDescribeFederatedTopicResult> FederatedTopic;
    };

public:
    explicit TPqState(const TString& sessionId);

    const TTopicMeta* FindTopicMeta(const TString& cluster, const TString& topicPath) const;
    const TTopicMeta* FindTopicMeta(const NNodes::TPqTopic& topic) const {
        return FindTopicMeta(topic.Cluster().StringValue(), topic.Path().StringValue());
    }

    bool IsRtmrMode() const;

public:
    bool SupportRtmrMode = false;
    bool UseActorSystemThreadsInTopicClient = true;
    bool AddTransparentPrefixToTransparentSystemColumns = true;
    bool EnableUserAttributesInTopicQuery = false;
    bool StreamingTopicsReadByDefault = true;
    bool UseYtflowEngine = false;
    bool EnableTopicsPredicatePushdown = false;
    bool EnablePqConstraintsTransformer = false;
    const TString SessionId;
    THashMap<std::pair<TString, TString>, TTopicMeta> Topics;

    TTypeAnnotationContext* Types = nullptr;
    TIntrusivePtr<TPqConfiguration> Configuration;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    IPqGateway::TPtr Gateway;
    THolder<IDqIntegration> DqIntegration;
    THolder<IYtflowIntegration> YtflowIntegration;
    THolder<IYtflowOptimization> YtflowOptimization;
    THashMap<std::pair<TString, NYql::EDatabaseType>, NYql::TDatabaseAuth> DatabaseIds;
    std::shared_ptr<NYql::IDatabaseAsyncResolver> DbResolver;
    NPq::NProto::StreamingDisposition Disposition;
    std::vector<std::pair<TString, TString>> TaskSensorLabels;
    std::vector<ui64> NodeIds;
};

TDataProviderInitializer GetPqDataProviderInitializer(
    IPqGateway::TPtr gateway,
    bool supportRtmrMode = false,
    std::shared_ptr<NYql::IDatabaseAsyncResolver> dbResolver = nullptr,
    const NPq::NProto::StreamingDisposition& disposition = {},
    const std::vector<std::pair<TString, TString>>& taskSensorLabels = {},
    const std::vector<ui64>& nodeIds = {},
    bool useActorSystemThreadsInTopicClient = true,
    bool useYtflowEngine = false,
    bool addTransparentPrefixToTransparentSystemColumns = true
);

TIntrusivePtr<IDataProvider> CreatePqDataSource(TPqState::TPtr state, IPqGateway::TPtr gateway);
TIntrusivePtr<IDataProvider> CreatePqDataSink(TPqState::TPtr state, IPqGateway::TPtr gateway);

} // namespace NYql
