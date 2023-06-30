#include "yql_yt_provider_impl.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/providers/yt/common/yql_configuration.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

namespace NYql {

using namespace NNodes;

namespace {

class TYtPeepholeTransformer : public TOptimizeTransformerBase {
public:
    TYtPeepholeTransformer(TYtState::TPtr state)
        : TOptimizeTransformerBase(state ? state->Types : nullptr, NLog::EComponent::ProviderYt, {}), State_(state)
    {
#define HNDL(name) "Peephole-"#name, Hndl(&TYtPeepholeTransformer::name)
        AddHandler(0, &TYtLength::Match, HNDL(OptimizeLength));
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

    const TYtState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateTYtPeepholeTransformer(TYtState::TPtr state) {
    return MakeHolder<TYtPeepholeTransformer>(std::move(state));
}

} // namespace NYql

