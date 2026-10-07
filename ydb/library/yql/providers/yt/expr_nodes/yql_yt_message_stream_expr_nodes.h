#pragma once

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.gen.h>

namespace NYql {
namespace NNodes {

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.decl.inl.h>

class TYtMessageStreamDataSource: public NGenerated::TYtMessageStreamDataSourceStub<TExprBase, TCallable, TCoAtom> {
public:
    explicit TYtMessageStreamDataSource(const TExprNode* node)
        : TYtMessageStreamDataSourceStub(node)
    {
    }

    explicit TYtMessageStreamDataSource(const TExprNode::TPtr& node)
        : TYtMessageStreamDataSourceStub(node)
    {
    }

    static bool Match(const TExprNode* node) {
        if (!TYtMessageStreamDataSourceStub::Match(node)) {
            return false;
        }

        return node->ChildrenSize() == 3 && node->Child(0)->IsAtom(YtProviderName)
            && node->Child(1)->IsAtom() && node->Child(2)->IsAtom("message_stream");
    }
};

#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.defs.inl.h>

} // namespace NNodes
} // namespace NYql
