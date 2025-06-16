#include "table.h"

#include "cluster.h"

#include <yql/essentials/core/yql_expr_optimize.h>

namespace NSQLComplete {

    TMaybe<TString> ToTablePath(const NYql::TExprNode& node) {
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

    THashSet<TString> ToTablePaths(const NYql::TExprNode& node) {
        if (node.IsCallable("MrTableConcat")) {
            THashSet<TString> paths;
            for (const auto& child : node.Children()) {
                if (auto path = ToTablePath(*child)) {
                    paths.emplace(std::move(*path));
                }
            }
            return paths;
        }

        if (auto path = ToTablePath(node)) {
            return {std::move(*path)};
        }

        return {};
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
            for (TString table : ToTablePaths(*node.Child(2))) {
                tablesByCluster[cluster].emplace(std::move(table));
            }

            return true;
        });
        return tablesByCluster;
    }

} // namespace NSQLComplete
