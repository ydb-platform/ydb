#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

namespace NYql {

using namespace NNodes;

class TPgDataSinkImpl : public TDataProviderBase {
public:
    TPgDataSinkImpl(TPgState::TPtr state)
        : State_(state)
        , TypeAnnotationTransformer_(CreatePgDataSinkTypeAnnotationTransformer(state))
        , ExecutionTransformer_(CreatePgDataSinkExecTransformer(state))
    {}

    TStringBuf GetName() const override {
        return PgProviderName;
    }

    bool CanParse(const TExprNode& node) override {
        return TypeAnnotationTransformer_->CanParse(node);
    }

    bool CanExecute(const TExprNode& node) override {
        return ExecutionTransformer_->CanExec(node);
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    IGraphTransformer& GetCallableExecutionTransformer() override {
        return *ExecutionTransformer_;
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSink::CallableName())) {
            if (!EnsureArgsCount(node, 2, ctx)) {
                return false;
            }

            if (node.Child(0)->Content() == PgProviderName) {
                if (!EnsureAtom(*node.Child(1), ctx)) {
                    return false;
                }

                if (node.Child(1)->Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(node.Child(1)->Pos()), "Empty cluster name"));
                    return false;
                }

                cluster = TString(node.Child(1)->Content());
                return true;
            }
        }

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Pg DataSink parameters"));
        return false;
    }

private:
    TPgState::TPtr State_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<TExecTransformerBase> ExecutionTransformer_;
};

TIntrusivePtr<IDataProvider> CreatePgDataSink(TPgState::TPtr state) {
    return MakeIntrusive<TPgDataSinkImpl>(state);
}

}