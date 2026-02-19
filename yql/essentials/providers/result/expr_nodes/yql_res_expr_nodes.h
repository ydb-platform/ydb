#pragma once

#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.gen.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NYql::NNodes {

#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.decl.inl.h>

class TResultDataSink: public NGenerated::TResultDataSinkStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TResultDataSink(const TExprNode* node)
        : TResultDataSinkStub(node)
    {
    }

    explicit TResultDataSink(const TExprNode::TPtr& node)
        : TResultDataSinkStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TResultDataSinkStub::Match(node)) {
            return false;
        }

        if (node->Child(0)->Content() != ResultProviderName) {
            return false;
        }

        return true;
    }
};

#include <yql/essentials/providers/result/expr_nodes/yql_res_expr_nodes.defs.inl.h>

template <typename TParent>
class TNodeBuilder<TParent, TResultDataSink>: TNodeBuilderBase {
public:
    typedef std::function<TParent&(const TResultDataSink&)> BuildFuncType;
    typedef std::function<TExprBase(const TStringBuf& arg)> GetArgFuncType;
    typedef TResultDataSink ResultType;

    TNodeBuilder(TExprContext& ctx, TPositionHandle pos, BuildFuncType buildFunc, GetArgFuncType getArgFunc)
        : TNodeBuilderBase(ctx, pos, getArgFunc)
        , BuildFunc_(buildFunc)
    {
    }

    TParent& Build() {
        auto atom = this->Ctx_.NewAtom(this->Pos_, ResultProviderName);
        auto node = this->Ctx_.NewCallable(this->Pos_, "DataSink", {atom});
        return BuildFunc_(TResultDataSink(node));
    }

private:
    BuildFuncType BuildFunc_;
};

} // namespace NYql::NNodes
