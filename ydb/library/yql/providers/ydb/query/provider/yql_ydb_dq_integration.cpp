#include <ydb/library/yql/providers/ydb/query/common/provider_names.h>
#include "yql_ydb_provider_impl.h"

#include <library/cpp/json/json_value.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/mkql/parser.h>
#include <ydb/library/yql/providers/ydb/query/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/query/proto/source.pb.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/providers/common/dq/yql_dq_integration_impl.h>

namespace NYql::NYdbQuery {
namespace {

using namespace NNodes;

class TDqIntegration final : public TDqIntegrationBase {
public:
    explicit TDqIntegration(TState::TPtr state)
        : State_(std::move(state))
    {
    }

    bool CanRead(const TExprNode& node, TExprContext&, bool) override {
        return TYdbQueryReadTable::Match(&node);
    }

    TMaybe<ui64> EstimateReadSize(ui64, ui32, const TVector<const TExprNode*>& nodes, TExprContext&) override {
        for (const auto* node : nodes) {
            if (!TYdbQueryReadTable::Match(node)) {
                return Nothing();
            }
        }
        return 0;
    }

    TExprNode::TPtr WrapRead(const TExprNode::TPtr& node, TExprContext& ctx, const TWrapReadSettings&) override {
        if (!TYdbQueryReadTable::Match(node.Get())) {
            return node;
        }
        const TYdbQueryReadTable read(node);
        const auto* row = node->GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        TExprNode::TListType columns;
        for (const auto* item : row->GetItems()) {
            columns.emplace_back(ctx.NewAtom(node->Pos(), item->GetName()));
        }
        if (columns.empty()) {
            // COUNT(*) and constant projections still need the number of remote rows.
            // Read a carrier column; RowType remains empty so only block length is exposed.
            const auto& table = State_->Tables.at(TState::TTableKey(read.DataSource().Cluster().StringValue(), read.Table().StringValue()));
            columns.emplace_back(ctx.NewAtom(node->Pos(), table.RowType->GetItems().front()->GetName()));
        }
        return Build<TDqSourceWrap>(ctx, node->Pos())
            .Input<TYdbQuerySourceSettings>()
                .World(read.World())
                .Cluster(read.DataSource().Cluster())
                .Table(read.Table())
                .Token<TCoSecureParam>()
                    .Name().Build(TString("cluster:default_") + read.DataSource().Cluster().StringValue())
                .Build()
                .Columns(ctx.NewList(node->Pos(), std::move(columns)))
            .Build()
            .RowType(ExpandType(node->Pos(), *row, ctx))
            .DataSource(read.DataSource().Cast<TCoDataSource>())
            .Done().Ptr();
    }

    ui64 Partition(const TExprNode& node, TVector<TString>& partitions, TString*, TExprContext&, const TPartitionSettings&) override {
        if (const auto source = TMaybeNode<TDqSource>(&node); source && source.Settings().Maybe<TYdbQuerySourceSettings>()) {
            // A single Query Service stream owns a single snapshot for this split.
            partitions.assign(1, TString());
        }
        return 0;
    }

    void FillSourceSettings(const TExprNode& node, google::protobuf::Any& proto, TString& sourceType, size_t, TExprContext&) override {
        const TDqSource source(&node);
        const auto settings = source.Settings().Cast<TYdbQuerySourceSettings>();
        const auto& clusterName = settings.Cluster().StringValue();
        const auto& cluster = State_->Clusters.at(clusterName);
        const auto& table = State_->Tables.at(TState::TTableKey(clusterName, settings.Table().StringValue()));
        TSource payload;
        payload.SetVersion(1);
        payload.SetEndpoint(cluster.Endpoint);
        payload.SetDatabase(cluster.Database);
        const auto path = settings.Table().StringValue();
        payload.SetTable(path.StartsWith('/') ? path : cluster.Database + "/" + path);
        payload.SetToken(settings.Token().Name().StringValue());
        payload.SetUseTls(cluster.UseTls);
        // Fixed internal budget for the experimental provider, not an EDS option.
        payload.SetReadTimeoutMs(TSource::default_instance().GetReadTimeoutMs());
        for (const auto column : settings.Columns()) {
            auto* target = payload.AddColumns();
            target->SetName(column.StringValue());
            *target->MutableType() = table.ColumnTypes.at(column.StringValue());
        }
        proto.PackFrom(payload);
        sourceType = "Ydb";
    }

    void FillLookupSourceSettings(const TExprNode&, google::protobuf::Any&, TString&) override {
        // The logical optimizer reports this as a query issue. Keep the planner
        // boundary guarded as well instead of reaching TDqIntegrationBase's ENSURE.
        throw yexception() << "Ydb streamlookup joins are not supported";
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        compiler.ChainCallable(TDqSourceWideBlockWrap::CallableName(),
            [](const TExprNode& node, NCommon::TMkqlBuildContext& ctx) {
                const TDqSourceWideBlockWrap wrapper(&node);
                if (wrapper.DataSource().Category().Value() == YdbQueryProviderName) {
                    return *TryWrapWithParserForArrowIPCStreaming(wrapper, ctx);
                }
                return NKikimr::NMiniKQL::TRuntimeNode();
            });
    }

    bool FillSourcePlanProperties(const TExprBase& node, TMap<TString, NJson::TJsonValue>& properties) override {
        const auto source = node.Maybe<TDqSource>();
        if (!source || !source.Settings().Maybe<TYdbQuerySourceSettings>()) {
            return false;
        }
        const auto settings = source.Cast().Settings().Cast<TYdbQuerySourceSettings>();
        properties["SourceType"] = "Ydb";
        properties["Table"] = settings.Table().StringValue();
        properties["Database"] = State_->Clusters.at(settings.Cluster().StringValue()).Database;
        auto& columns = properties["ReadColumns"];
        columns.SetType(NJson::JSON_ARRAY);
        for (const auto column : settings.Columns()) {
            columns.AppendValue(column.StringValue());
        }
        properties["ReadTimeoutMs"] = TSource::default_instance().GetReadTimeoutMs();
        return true;
    }

private:
    const TState::TPtr State_;
};

} // namespace

THolder<IDqIntegration> CreateDqIntegration(TState::TPtr state) {
    return MakeHolder<TDqIntegration>(std::move(state));
}

} // namespace NYql::NYdbQuery
