#include "yql_pg_provider_impl.h"

#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>

#include <ydb/library/yql/utils/log/log.h>

namespace NYql {

using namespace NNodes;

class TPgDataSourceImpl : public TDataProviderBase {
public:
    TPgDataSourceImpl(TPgState::TPtr state)
        : State_(state)
        , TypeAnnotationTransformer_(CreatePgDataSourceTypeAnnotationTransformer(state))
        , DqIntegration_(CreatePgDqIntegration(State_))
    {}

    TStringBuf GetName() const override {
        return PgProviderName;
    }

    IGraphTransformer& GetTypeAnnotationTransformer(bool instantOnly) override {
        Y_UNUSED(instantOnly);
        return *TypeAnnotationTransformer_;
    }

    bool CanParse(const TExprNode& node) override {
        if (node.IsCallable(TCoRead::CallableName())) {
            return TPgDataSource::Match(node.Child(1));
        }
        return TypeAnnotationTransformer_->CanParse(node);
    }

    bool GetExecWorld(const TExprNode::TPtr& node, TExprNode::TPtr& root) override {
        auto read = TMaybeNode<TPgReadTable>(node);
        if (!read) {
            return false;
        }

        root = read.Cast().World().Ptr();
        return true;
    }

    TExprNode::TPtr RewriteIO(const TExprNode::TPtr& node, TExprContext& ctx) override {
        YQL_CLOG(INFO, ProviderPg) << "RewriteIO";
        if (auto left = TMaybeNode<TCoLeft>(node)) {
            return left.Input().Maybe<TPgRead>().World().Cast().Ptr();
        }

        auto read = TCoRight(node).Input().Cast<TPgRead>();
        auto keyNode = read.FreeArgs().Get(2).Ptr();
        if (keyNode->IsCallable("MrTableConcat")) {
            if (keyNode->ChildrenSize() != 1) {
                ctx.AddError(TIssue(ctx.GetPosition(keyNode->Pos()), TStringBuilder() << "Expected single table name"));
                return nullptr;
            }

            keyNode = keyNode->HeadPtr();
        }

        const auto maybeKey = TExprBase(keyNode).Maybe<TCoKey>();
        if (!maybeKey) {
            ctx.AddError(TIssue(ctx.GetPosition(read.FreeArgs().Get(0).Pos()), TStringBuilder() << "Expected Key"));
            return nullptr;
        }

        const auto& keyArg = maybeKey.Cast().Ref().Head();
        if (!keyArg.IsList() || keyArg.ChildrenSize() != 2U
            || !keyArg.Head().IsAtom("table") || !keyArg.Tail().IsCallable(TCoString::CallableName())) {
            ctx.AddError(TIssue(ctx.GetPosition(keyArg.Pos()), TStringBuilder() << "Expected single table name"));
            return nullptr;
        }

        const auto tableName = TString(keyArg.Tail().Head().Content());
        auto childrenList = read.Ref().ChildrenList();
        childrenList[2] = ctx.NewAtom(childrenList[2]->Pos(), tableName);
        auto newRead = ctx.NewCallable(read.Ref().Pos(), TPgReadTable::CallableName(), std::move(childrenList));

        return Build<TCoRight>(ctx, read.Pos())
            .Input(newRead)
            .Done().Ptr();
    }

    bool ValidateParameters(TExprNode& node, TExprContext& ctx, TMaybe<TString>& cluster) override {
        if (node.IsCallable(TCoDataSource::CallableName())) {
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

        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), "Invalid Pg DataSource parameters"));
        return false;
    }

    IDqIntegration* GetDqIntegration() override {
        return DqIntegration_.Get();
    }

private:
    TPgState::TPtr State_;
    const THolder<TVisitorTransformerBase> TypeAnnotationTransformer_;
    const THolder<IDqIntegration> DqIntegration_;
};

TIntrusivePtr<IDataProvider> CreatePgDataSource(TPgState::TPtr state) {
    return MakeIntrusive<TPgDataSourceImpl>(state);
}

}
