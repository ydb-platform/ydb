#pragma once

#include <yql/essentials/ast/yql_expr.h>


namespace NYql {

class IYtflowOptimization {
public:
    virtual ~IYtflowOptimization() = default;

    /**
        Apply new members subset for YtflowReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * members - expr list of atoms with new members
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read with applied new members
    */
    virtual TExprNode::TPtr ApplyExtractMembers(
        const TExprNode::TPtr& read, const TExprNode::TPtr& members,
        TExprContext& ctx) = 0;

    /**
        Apply `unordered` setting for YtflowReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read with applied setting
    */
    virtual TExprNode::TPtr ApplyUnordered(
        const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    /**
        Rewrite YtflowWriteWrap's underlying provider specific write callable
        Args:
            * write - provider specific write callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `write`, if no changes
            * new write with trimmed content
    */
    virtual TExprNode::TPtr TrimWriteContent(
        const TExprNode::TPtr& write, TExprContext& ctx) = 0;
};

} // namespace NYql
