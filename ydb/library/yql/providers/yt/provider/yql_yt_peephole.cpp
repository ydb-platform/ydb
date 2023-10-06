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
    TYtPeepholeTransformer(TYtState::TPtr state, const TYtExtraPeepHoleSettings& settings)
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
        if (!Settings_.TmpTable) {
            return node;
        }

        if (GetSetting(TYtDqWideWrite(&node.Ref()).Settings().Ref(), "outTable")) {
            return node;
        }

        auto server = State_->Gateway->GetClusterServer(Settings_.CurrentCluster);
        YQL_ENSURE(server, "Invalid YT cluster: " << Settings_.CurrentCluster);

        NYT::TRichYPath realTable = State_->Gateway->GetWriteTable(State_->SessionId, Settings_.CurrentCluster,
            Settings_.TmpTable->Name().StringValue(), Settings_.TmpFolder);
        realTable.Append(true);
        YQL_ENSURE(realTable.TransactionId_.Defined(), "Expected TransactionId");

        NYT::TNode spec;
        TYqlRowSpecInfo(Settings_.TmpTable->RowSpec()).FillCodecNode(spec[YqlRowSpecAttribute]);
        NYT::TNode outSpec = NYT::TNode::CreateMap()(TString{YqlIOSpecTables}, NYT::TNode::CreateList().Add(spec));
        NYT::TNode writerOptions = NYT::TNode::CreateMap();

        if (Settings_.Config->MaxRowWeight.Get(Settings_.CurrentCluster)) {
            auto maxRowWeight = Settings_.Config->MaxRowWeight.Get(Settings_.CurrentCluster)->GetValue();
            writerOptions["max_row_weight"] = static_cast<i64>(maxRowWeight);
        }

        TMaybeNode<TCoSecureParam> secParams;
        if (State_->Configuration->Auth.Get().GetOrElse(TString())) {
            secParams = Build<TCoSecureParam>(ctx, node.Pos()).Name().Build(TString("cluster:default_").append(Settings_.CurrentCluster)).Done();
        }

        auto settings = Build<TCoNameValueTupleList>(ctx, node.Pos())
            .Add()
                .Name().Value("table", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(NYT::NodeToYsonString(NYT::PathToNode(realTable))).Build()
            .Build()
            .Add()
                .Name().Value("server", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(server).Build()
            .Build()
            .Add()
                .Name().Value("outSpec", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(NYT::NodeToYsonString(outSpec)).Build()
            .Build()
            .Add()
                .Name().Value("secureParams", TNodeFlags::Default).Build()
                .Value(secParams)
            .Build()
            .Add()
                .Name().Value("tx", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(GetGuidAsString(*realTable.TransactionId_), TNodeFlags::Default).Build()
            .Build()
            .Add()
                .Name().Value("outTable", TNodeFlags::Default).Build()
                .Value(*Settings_.TmpTable)
            .Build()
            .Add()
                .Name().Value("writerOptions", TNodeFlags::Default).Build()
                .Value<TCoAtom>().Value(NYT::NodeToYsonString(writerOptions)).Build()
            .Build()
            .Done().Ptr();

        return ctx.ChangeChild(node.Ref(), TYtDqWideWrite::idx_Settings, std::move(settings));
    }

    const TYtState::TPtr State_;
    const TYtExtraPeepHoleSettings Settings_;
};

}

THolder<IGraphTransformer> CreateTYtPeepholeTransformer(TYtState::TPtr state, const TYtExtraPeepHoleSettings& settings) {
    return MakeHolder<TYtPeepholeTransformer>(std::move(state), settings);
}

} // namespace NYql

