#include "yql_ydb_dq_integration.h"
#include "yql_ydb_mkql_compiler.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/expr_nodes/yql_ydb_expr_nodes.h>
#include <ydb/library/yql/providers/ydb/proto/range.pb.h>
#include <ydb/library/yql/providers/ydb/proto/source.pb.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYdbDqIntegration: public TDqIntegrationBase {
public:
    TYdbDqIntegration(TYdbState::TPtr state)
        : State_(state)
    {
    }

    ui64 Partition(const TDqSettings& settings, size_t maxPartitions, const TExprNode& node,
        TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        TString cluster, table;
        if (const TMaybeNode<TDqSource> source = &node) {
            cluster = source.Cast().DataSource().Cast<TYdbDataSource>().Cluster().Value();
            table = source.Cast().Settings().Cast<TYdbSourceSettings>().Table().Value();
        } else if (const TMaybeNode<TYdbReadTable> read = &node) {
            cluster = read.Cast().DataSource().Cluster().Value();
            table = read.Cast().Table().Value();
        }

        auto& meta = State_->Tables[std::make_pair(cluster, table)];
        meta.ReadAsync = settings.EnableComputeActor.Get().GetOrElse(false); // TODO: Use special method for get settings.
        auto parts = meta.Partitions;

        if (maxPartitions && parts.size() > maxPartitions) {
            if (const auto extraParts = parts.size() - maxPartitions; extraParts > maxPartitions) {
                const auto dropsPerTask = (parts.size() - 1ULL) / maxPartitions;
                for (auto it = parts.begin(); parts.end() > it;) {
                    auto to = it + std::min(dropsPerTask, std::distance(it, parts.end()) - 1ULL);
                    it->back() = std::move(to->back());
                    it = parts.erase(++it, ++to);
                }
            } else {
                const auto dropEachPart = maxPartitions / extraParts;
                for (auto it = parts.begin(); parts.size() > maxPartitions;) {
                    const auto to = it + dropEachPart;
                    it = to - 1U;
                    it->back() = std::move(to->back());
                    it = parts.erase(to);
                }
            }
        }

        partitions.reserve(parts.size());
        for (const auto& part : parts) {
            NYdb::TKeyRange range;
            range.set_from_key(part.front());
            range.set_to_key(part.back());
            partitions.emplace_back();
            TStringOutput out(partitions.back());
            range.Save(&out);
        }
        return 0;
    }

    bool CanRead(const TExprNode& read, TExprContext&, bool ) override {
        return TYdbReadTable::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TYdbReadTable::Match(val); })) {
            return 0ul; // TODO: return real size
        }
        return Nothing();
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto& maybeYdbReadTable = TMaybeNode<TYdbReadTable>(read)) {
            const auto& ydbReadTable = maybeYdbReadTable.Cast();
            YQL_ENSURE(ydbReadTable.Ref().GetTypeAnn(), "No type annotation for node " << ydbReadTable.Ref().Content());
            const auto& clusterName = ydbReadTable.DataSource().Cluster().Value();
            const auto token = "cluster:default_" + TString(clusterName);
            YQL_CLOG(INFO, ProviderYdb) << "Wrap " << read->Content() << " with token: " << token;

            const auto rowType = ydbReadTable.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
            auto columns = ydbReadTable.Columns().Ptr();
            if (!columns->IsList()) {
                const auto pos = columns->Pos();
                const auto& items = rowType->Cast<TStructExprType>()->GetItems();
                TExprNode::TListType cols;
                cols.reserve(items.size());
                std::transform(items.cbegin(), items.cend(), std::back_inserter(cols), [&](const TItemExprType* item) { return ctx.NewAtom(pos, item->GetName()); });
                columns = ctx.NewList(pos, std::move(cols));
            }

            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TYdbSourceSettings>()
                    .Table(ydbReadTable.Table())
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .Columns(std::move(columns))
                    .Build()
                .RowType(ExpandType(ydbReadTable.Pos(), *rowType, ctx))
                .DataSource(ydbReadTable.DataSource().Cast<TCoDataSource>())
                .Done().Ptr();
        }
        return read;
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t, TExprContext&) override {
        const TDqSource source(&node);
        if (const auto maySettings = source.Settings().Maybe<TYdbSourceSettings>()) {
            const auto settings = maySettings.Cast();

            const auto& cluster = source.DataSource().Cast<TYdbDataSource>().Cluster().StringValue();
            const auto& table = settings.Table().StringValue();
            const auto& token = settings.Token().Name().StringValue();

            const auto& connect =  State_->Configuration->Clusters[cluster];

            NYdb::TSource srcDesc;
            srcDesc.SetTable(table);
            srcDesc.SetDatabase(connect.Database);
            srcDesc.SetEndpoint(connect.Endpoint);
            srcDesc.SetSecure(connect.Secure);
            srcDesc.SetAddBearerToToken(connect.AddBearerToToken);
            srcDesc.SetToken(token);

            const auto& columns = settings.Columns();
            for (auto i = 0U; i < columns.Size(); ++i)
                srcDesc.AddColumns(columns.Item(i).StringValue());

            for (const auto type : State_->Tables[std::make_pair(cluster, table)].KeyTypes)
                srcDesc.AddKeyColumnTypes(type);

            protoSettings.PackFrom(srcDesc);
            sourceType = "YdbSource";
        }
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqYdbMkqlCompilers(compiler, State_);
    }

private:
    const TYdbState::TPtr State_;
};

}

THolder<IDqIntegration> CreateYdbDqIntegration(TYdbState::TPtr state) {
    return MakeHolder<TYdbDqIntegration>(state);
}

}
