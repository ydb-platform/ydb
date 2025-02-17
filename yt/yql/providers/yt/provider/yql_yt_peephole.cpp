#include "yql_yt_provider_impl.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_optimize.h>
#include <yt/yql/providers/yt/common/yql_configuration.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>
#include <yt/yql/providers/yt/common/yql_names.h>

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
        AddHandler(0, &TYtDqWrite::Match, HNDL(OptimizeYtDqWrite));
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

    TMaybeNode<TExprBase> OptimizeYtDqWrite(TExprBase node, TExprContext& ctx) {
        const auto write = node.Cast<TYtDqWrite>();

        const auto& items = GetSeqItemType(write.Input().Ref().GetTypeAnn())->Cast<TStructExprType>()->GetItems();
        auto expand = ctx.Builder(write.Pos())
            .Callable("ExpandMap")
                .Add(0, write.Input().Ptr())
                .Lambda(1)
                    .Param("item")
                    .Do([&](TExprNodeBuilder& lambda) -> TExprNodeBuilder& {
                        ui32 i = 0U;
                        for (const auto& item : items) {
                            lambda.Callable(i++, "Member")
                                .Arg(0, "item")
                                .Atom(1, item->GetName())
                            .Seal();
                        }
                        return lambda;
                    })
                .Seal()
            .Seal().Build();

        TYtDqWideWrite wideWrite = Build<TYtDqWideWrite>(ctx, write.Pos())
            .Input(std::move(expand))
            .Settings(write.Settings())
            .Done();

        if (Settings_.empty()) {
            return Build<TCoDiscard>(ctx, write.Pos())
                .Input(wideWrite)
                .Done();
        }

        auto cluster = Settings_.at("yt_cluster");
        auto server = Settings_.at("yt_server");
        auto table = Settings_.at("yt_table");
        auto tableName = Settings_.at("yt_tableName");
        auto tableType = Settings_.at("yt_tableType");
        auto writeOptions = Settings_.at("yt_writeOptions");
        auto outSpec = Settings_.at("yt_outSpec");
        auto tx = Settings_.at("yt_tx");

        YQL_ENSURE(!HasSetting(wideWrite.Settings().Ref(), "table"));

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

        return Build<TCoDiscard>(ctx, write.Pos())
            .Input(ctx.ChangeChild(wideWrite.Ref(), TYtDqWideWrite::idx_Settings, std::move(settings)))
            .Done();
    }

    const TYtState::TPtr State_;
    const THashMap<TString, TString> Settings_;
};

}

THolder<IGraphTransformer> CreateYtPeepholeTransformer(TYtState::TPtr state, const THashMap<TString, TString>& settings) {
    return MakeHolder<TYtPeepholeTransformer>(std::move(state), settings);
}

} // namespace NYql

