#include "table.h"

#include "cluster.h"

#define USE_CURRENT_UDF_ABI_VERSION
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node) {
        if (node.IsCallable("MrTableConcat")) {
            Y_ENSURE(node.ChildrenSize() < 2);
            return ToTablePath(*node.Child(0));
        }

        if (!node.IsCallable("Key") || node.ChildrenSize() < 1) {
            return Nothing();
        }

        const NYql::TExprNode* table = node.Child(0);
        if (!table->IsList() || table->ChildrenSize() < 2) {
            return Nothing();
        }

        TStringBuf kind = table->Child(0)->Content();
        if (kind != "table" && kind != "tablescheme") {
            return Nothing();
        }

        const NYql::TExprNode* string = table->Child(1);
        if (!string->IsCallable("String") || string->ChildrenSize() < 1) {
            return Nothing();
        }

        return TString(string->Child(0)->Content());
    }

    THashMap<TString, THashSet<TString>> CollectTablesByCluster(const NYql::TExprNode& node) {
        THashMap<TString, THashSet<TString>> tablesByCluster;
        NYql::VisitExpr(node, [&](const NYql::TExprNode& node) -> bool {
            if (!node.IsCallable("Read!") && !node.IsCallable("Write!")) {
                return true;
            }
            if (node.ChildrenSize() < 4) {
                return true;
            }

            TString cluster = ToCluster(*node.Child(1)).GetOrElse("");
            TMaybe<TString> table = ToTablePath(*node.Child(2));
            if (table.Empty()) {
                return true;
            }

            tablesByCluster[std::move(cluster)].emplace(std::move(*table));
            return true;
        });
        return tablesByCluster;
    }

} // namespace NSQLComplete
