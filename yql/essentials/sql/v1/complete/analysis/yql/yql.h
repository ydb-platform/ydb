#pragma once

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NSQLComplete {

    struct TYqlContext {
        THashMap<TString, THashSet<TString>> TablesByCluster;

        THashSet<TString> Clusters() const;
    };

    class IYqlAnalysis: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IYqlAnalysis>;

        virtual TYqlContext Analyze(NYql::TExprNode::TPtr root, NYql::TExprContext& ctx) const = 0;
        TMaybe<TYqlContext> Analyze(NYql::TAstNode& root, NYql::TIssues& issues) const;
    };

    IYqlAnalysis::TPtr MakeYqlAnalysis();

} // namespace NSQLComplete
