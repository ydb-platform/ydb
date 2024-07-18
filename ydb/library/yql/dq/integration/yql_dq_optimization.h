#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

class IDqOptimization {
public:
    virtual ~IDqOptimization() {}

    /**
        Rewrite DqReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read, if any optimizations
    */
    virtual TExprNode::TPtr RewriteRead(const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    /**
        Rewrite DqReadWrap's underlying provider specific read callable for lookup
        Args:
            * read - provider specific read callable
            * ctx - expr context
        Returns of of:
            * empty TPtr, if lookup read is not supported
            * DqLookupSourceWrap callable
    */
    virtual TExprNode::TPtr RewriteLookupRead(const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    /**
        Apply new members subset for DqReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * members - expr list of atoms with new members
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read with applyed new members
    */
    virtual TExprNode::TPtr ApplyExtractMembers(const TExprNode::TPtr& read, const TExprNode::TPtr& members, TExprContext& ctx) = 0;

    /**
        Apply `take` or `skip` setting for DqReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * countBase - `Take`, `Skip` or `Limit` callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read with applyed setting
    */
    virtual TExprNode::TPtr ApplyTakeOrSkip(const TExprNode::TPtr& read, const TExprNode::TPtr& countBase, TExprContext& ctx) = 0;

    /**
        Apply `unordered` setting for DqReadWrap's underlying provider specific read callable
        Args:
            * read - provider specific read callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `read`, if no changes
            * new read with applyed setting
    */
    virtual TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& read, TExprContext& ctx) = 0;

    /**
        Optimize list/stream extend for set of DqReadWrap's underlying provider specific read callable
        Args:
            * listOfRead - expr list of provider specific read callables
            * ordered - `true` for ordered extend (must keep original order of reads); `false`, if reads may be reordred due the optimization
            * ctx - expr context
        Returns one of:
            * empty list on error
            * original `listOfRead`, if no changes
            * new optimized list of reads. Returned list length must not be greater than original `listOfRead` length
    */
    virtual TExprNode::TListType ApplyExtend(const TExprNode::TListType& listOfRead, bool ordered, TExprContext& ctx) = 0;
};

} // namespace NYql
