#include "cluster.h"

#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    TMaybe<TString> ToCluster(const NYql::TExprNode& node) {
        if (!node.IsCallable("DataSource") && !node.IsCallable("DataSink")) {
            return Nothing();
        }
        if (node.ChildrenSize() == 2 && node.Child(1)->IsAtom()) {
            return TString(node.Child(1)->Content());
        }
        return Nothing();
    }

    THashSet<TString> CollectClusters(const NYql::TExprNode& root) {
        THashSet<TString> clusters;
        NYql::VisitExpr(root, [&](const NYql::TExprNode& node) -> bool {
            if (TMaybe<TString> cluster = ToCluster(node)) {
                clusters.emplace(std::move(*cluster));
                return true;
            }
            return true;
        });
        return clusters;
    }

} // namespace NSQLComplete
