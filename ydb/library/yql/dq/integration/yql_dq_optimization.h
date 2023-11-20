#pragma once

#include <ydb/library/yql/ast/yql_expr.h>

namespace NYql {

class IDqOptimization {
public:
    virtual ~IDqOptimization() {}

    /**
        Rewrite provider reader under DqReadWrap
        Args:
            * reader - provider specific callable of reader
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `reader` if no changes
            * new reader if any optimizations
    */
    virtual TExprNode::TPtr RewriteRead(const TExprNode::TPtr& reader, TExprContext& ctx) = 0;

    /**
        Apply new members subset for DqReadWrap's underlying provider reader
        Args:
            * reader - provider specific callable of reader
            * members - expr list of atoms with new members
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `reader` if no changes
            * new reader with applyed new members
    */
    virtual TExprNode::TPtr ApplyExtractMembers(const TExprNode::TPtr& reader, const TExprNode::TPtr& members, TExprContext& ctx) = 0;

    /**
        Apply `take` or `skip` setting for DqReadWrap's underlying provider reader
        Args:
            * reader - provider specific callable of reader
            * countBase - `Take`, `Skip` or `Limit` callable
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `reader` if no changes
            * new reader with applyed setting
    */
    virtual TExprNode::TPtr ApplyTakeOrSkip(const TExprNode::TPtr& reader, const TExprNode::TPtr& countBase, TExprContext& ctx) = 0;

    /**
        Apply `unordered` setting for DqReadWrap's underlying provider reader
        Args:
            * reader - provider specific callable of reader
            * ctx - expr context
        Returns one of:
            * empty TPtr on error
            * original `reader` if no changes
            * new reader with applyed setting
    */
    virtual TExprNode::TPtr ApplyUnordered(const TExprNode::TPtr& reader, TExprContext& ctx) = 0;

    /**
        Optimize list/stream extend for set of DqReadWrap's underlying provider readers
        Args:
            * listOfReader - expr list of provider specific readers
            * ordered - `true` for ordered extend (should keep original order of readres), `false` if readers may be reordred after optimization
            * ctx - expr context
        Returns one of:
            * empty list on error
            * original `listOfReader` if no changes
            * new optimized list of readers. Returned list length should not be greater than original `listOfReader` length
    */
    virtual TExprNode::TListType ApplyExtend(const TExprNode::TListType& listOfReader, bool ordered, TExprContext& ctx) = 0;
};

} // namespace NYql
