#include "yql_clickhouse_dq_integration.h"
#include "yql_clickhouse_mkql_compiler.h"

#include <ydb/library/yql/providers/common/dq/yql_dq_integration_impl.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>
#include <ydb/library/yql/providers/clickhouse/proto/source.pb.h>
#include <ydb/library/yql/providers/clickhouse/proto/range.pb.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

using namespace NNodes;

namespace {

class TClickHouseDqIntegration: public TDqIntegrationBase {
public:
    TClickHouseDqIntegration(TClickHouseState::TPtr state)
        : State_(state)
    {}

    bool CanRead(const TExprNode& read, TExprContext&, bool) override {
        return TClReadTable::Match(&read);
    }

    TMaybe<ui64> EstimateReadSize(ui64 /*dataSizePerJob*/, ui32 /*maxTasksPerStage*/, const TVector<const TExprNode*>& read, TExprContext&) override {
        if (AllOf(read, [](const auto val) { return TClReadTable::Match(val); })) {
            return 0ul; // TODO: return real size
        }
        return Nothing();
    }

    TExprNode::TPtr WrapRead(const TDqSettings&, const TExprNode::TPtr& read, TExprContext& ctx) override {
        if (const auto maybeClReadTable = TMaybeNode<TClReadTable>(read)) {
            const auto clReadTable = maybeClReadTable.Cast();
            const auto token = TString("cluster:default_") += clReadTable.DataSource().Cluster().StringValue();
            YQL_CLOG(INFO, ProviderClickHouse) << "Wrap " << read->Content() << " with token: " << token;

            const auto rowType = clReadTable.Ref().GetTypeAnn()->Cast<TTupleExprType>()->GetItems().back()->Cast<TListExprType>()->GetItemType();
            auto columns = clReadTable.Columns().Ptr();
            if (!columns->IsList()) {
                const auto pos = columns->Pos();
                const auto& items = rowType->Cast<TStructExprType>()->GetItems();
                TExprNode::TListType cols;
                cols.reserve(items.size());
                std::transform(items.cbegin(), items.cend(), std::back_inserter(cols), [&](const TItemExprType* item) { return ctx.NewAtom(pos, item->GetName()); });
                columns = ctx.NewList(pos, std::move(cols));
            }

            return Build<TDqSourceWrap>(ctx, read->Pos())
                .Input<TClSourceSettings>()
                    .Table(clReadTable.Table())
                    .Token<TCoSecureParam>()
                        .Name().Build(token)
                        .Build()
                    .Columns(std::move(columns))
                    .Build()
                .RowType(ExpandType(clReadTable.Pos(), *rowType, ctx))
                .DataSource(clReadTable.DataSource().Cast<TCoDataSource>())
                .Done().Ptr();
        }
        return read;
    }

    ui64 Partition(const TDqSettings&, size_t, const TExprNode&, TVector<TString>& partitions, TString*, TExprContext&, bool) override {
        partitions.clear();
        NCH::TRange range;
//      range.SetRange("limit 42 offset 42 order by ...."); // Possible set range like this.
        partitions.emplace_back();
        TStringOutput out(partitions.back());
        range.Save(&out);
        return 0ULL;
    }

    void FillSourceSettings(const TExprNode& node, ::google::protobuf::Any& protoSettings, TString& sourceType, size_t, TExprContext&) override {
        const TDqSource source(&node);
        if (const auto maySettings = source.Settings().Maybe<TClSourceSettings>()) {
            const auto settings = maySettings.Cast();

            const auto& cluster = source.DataSource().Cast<TClDataSource>().Cluster().StringValue();
            const auto& table = settings.Table().StringValue();
            const auto& token = settings.Token().Name().StringValue();

            const auto& connect =  State_->Configuration->Urls[cluster];

            NCH::TSource srcDesc;
            switch (std::get<EHostScheme>(connect)) {
                case HS_HTTP: srcDesc.SetScheme("http://"); break;
                case HS_HTTPS: srcDesc.SetScheme("https://"); break;
            }
            srcDesc.SetEndpoint(std::get<TString>(connect) + ':' + ToString(std::get<ui16>(connect)));
            srcDesc.SetToken(token);

            TStringBuf db, dbTable;
            if (!TStringBuf(table).TrySplit('.', db, dbTable)) {
                db = "default";
                dbTable = table;
            }

            const auto& columns = settings.Columns();
            TStringBuilder query;
            query << "select " << columns.Item(0).StringValue();
            for (auto i = 1U; i < columns.Size(); ++i)
                query << ',' << columns.Item(i).StringValue();
            query << " from " << db << '.' << dbTable;

            srcDesc.SetQuery(query);
            protoSettings.PackFrom(srcDesc);
            sourceType = "ClickHouseSource";
        }
    }

    void RegisterMkqlCompiler(NCommon::TMkqlCallableCompilerBase& compiler) override {
        RegisterDqClickHouseMkqlCompilers(compiler, State_);
    }

private:
    const TClickHouseState::TPtr State_;
};

}

THolder<IDqIntegration> CreateClickHouseDqIntegration(TClickHouseState::TPtr state) {
    return MakeHolder<TClickHouseDqIntegration>(state);
}

}
