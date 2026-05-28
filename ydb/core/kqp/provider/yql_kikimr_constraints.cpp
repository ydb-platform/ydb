#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_expr_nodes.h"

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/providers/common/transform/yql_visit.h>

namespace NYql {

using namespace NNodes;

namespace {

class TKiSourceConstraintsTransformer final : public TVisitorTransformerBase {
public:
    explicit TKiSourceConstraintsTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : TVisitorTransformerBase(true)
        , SessionCtx(sessionCtx)
    {
        AddHandler({
            TCoConfigure::CallableName(),
            TKiReadTable::CallableName(),
            TKiReadTableScheme::CallableName(),
            TKiReadTableList::CallableName(),
        }, Hndl(&TKiSourceConstraintsTransformer::HandleDefault));

        if (IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, SessionCtx->Query().Type)) {
            AddHandler({TDqSource::CallableName()}, Hndl(&TKiSourceConstraintsTransformer::CopyAllFrom<1>));
            AddHandler({
                TDqSourceWrap::CallableName(),
                TDqSourceWideWrap::CallableName(),
                TDqSourceWideBlockWrap::CallableName(),
                TDqReadWrap::CallableName(),
                TDqReadWideWrap::CallableName(),
                TDqReadBlockWideWrap::CallableName(),
                TDqLookupSourceWrap::CallableName(),
            }, Hndl(&TKiSourceConstraintsTransformer::CopyAllFrom<0>));
        }
    }

private:
    static TStatus HandleDefault(const TExprNode::TPtr& node, TExprContext& ctx) {
        Y_UNUSED(node, ctx);
        return TStatus::Ok;
    }

    template <size_t Index>
    static TStatus CopyAllFrom(const TExprNode::TPtr& node, TExprContext& ctx) {
        Y_UNUSED(ctx);
        node->CopyConstraints(*node->Child(Index));
        return TStatus::Ok;
    }

    const TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

} // anonymous namespace

std::unique_ptr<IGraphTransformer> CreateKiSourceConstraintsTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx) {
    return std::make_unique<TKiSourceConstraintsTransformer>(sessionCtx);
}

} // namespace NYql
