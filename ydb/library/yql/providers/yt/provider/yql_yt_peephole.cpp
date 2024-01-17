#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <ydb/library/yql/providers/yt/common/yql_names.h>

#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtPeepholeTransformer : public TOptimizeTransformerBase {
public:
    TYtPeepholeTransformer(TYtState::TPtr state, const THashMap<TString, TString>& settings)
        : TOptimizeTransformerBase(state ? state->Types : nullptr, NLog::EComponent::ProviderYt, {})
        , State_(state)
        , Settings_(settings)
    {
#define HNDL(name) "Peephole-"#name, Hndl(&TYtPeepholeTransformer::name)
        AddHandler(0, &TYtLength::Match, HNDL(OptimizeLength));
        AddHandler(0, &TYtDqWideWrite::Match, HNDL(OptimizeYtDqWideWrite));
#undef HNDL
    }

private:
    TMaybeNode<TExprBase> OptimizeLength(TExprBase node, TExprContext& ctx) {
        std::optional<size_t> lengthRes;
        const auto& input = node.Cast<TYtLength>().Input();
        if (const auto& out = input.Maybe<TYtOutput>()) {
            if (const auto& info = TYtTableBaseInfo::Parse(out.Cast()); info->Stat) {
                lengthRes = info->Stat->RecordsCount;
            }
        } else if (const auto& read = input.Maybe<TYtReadTable>()) {
            lengthRes = 0ULL;
            for (auto path: read.Cast().Input().Item(0).Paths()) {
                if (const auto& info = TYtTableBaseInfo::Parse(path.Table()); info->Stat && info->Meta && !info->Meta->IsDynamic && path.Ranges().Maybe<TCoVoid>())
                    lengthRes = *lengthRes + info->Stat->RecordsCount;
                else {
                    lengthRes = std::nullopt;
                    break;
                }
            }
        }

        if (lengthRes) {
            return Build<TCoUint64>(ctx, node.Pos())
                .Literal().Value(ToString(*lengthRes), TNodeFlags::Default).Build()
                .Done();
        }

        return node;
    }

    TMaybeNode<TExprBase> OptimizeYtDqWideWrite(TExprBase node, TExprContext& ctx) {
        if (Settings_.empty()) {
            return node;
        }

        auto cluster = Settings_.at("yt_cluster");
        auto server = Settings_.at("yt_server");
        auto table = Settings_.at("yt_table");
        auto tableName = Settings_.at("yt_tableName");
        auto tableType = Settings_.at("yt_tableType");
        auto writeOptions = Settings_.at("yt_writeOptions");
        auto outSpec = Settings_.at("yt_outSpec");
        auto tx = Settings_.at("yt_tx");

        // Check that we optimize only single YtDqWideWrite
        if (auto setting = GetSetting(TYtDqWideWrite(&node.Ref()).Settings().Ref(), "table")) {
            YQL_ENSURE(setting->Child(1)->Content() == table);
            return node;
        }

        TMaybeNode<TCoSecureParam> secParams;
        if (State_->Configuration->Auth.Get().GetOrElse(TString())) {
            secParams = Build<TCoSecureParam>(ctx, node.Pos()).Name().Build(TString("cluster:default_").append(cluster)).Done();
        }

        auto settings = Build<TCoNameValueTupleList>(ctx, node.Pos())
            .Add()
                .Name().Value("table", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(table).Build()
            .Build()
            .Add()
                .Name().Value("server", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(server).Build()
            .Build()
            .Add()
                .Name().Value("outSpec", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(outSpec).Build()
            .Build()
            .Add()
                .Name().Value("secureParams", TNodeFlags::Default).Build()
                .Value(secParams)
            .Build()
            .Add()
                .Name().Value("writerOptions", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(writeOptions).Build()
            .Build()
            .Add()
                .Name().Value("tx", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(tx).Build()
            .Build()
            .Add() // yt_file gateway specific
                .Name().Value("tableName", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(tableName).Build()
            .Build()
            .Add() // yt_file gateway specific
                .Name().Value("tableType", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(tableType).Build()
            .Build()
            .Done().Ptr();

        return ctx.ChangeChild(node.Ref(), TYtDqWideWrite::idx_Settings, std::move(settings));
    }

    const TYtState::TPtr State_;
    const THashMap<TString, TString> Settings_;
};

}

THolder<IGraphTransformer> CreateYtPeepholeTransformer(TYtState::TPtr state, const THashMap<TString, TString>& settings) {
    return MakeHolder<TYtPeepholeTransformer>(std::move(state), settings);
}

} // namespace NYql

