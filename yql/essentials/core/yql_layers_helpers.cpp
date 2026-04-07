#include "yql_layers_helpers.h"
#include "yql_expr_optimize.h"

#include <util/generic/algorithm.h>

namespace NYql {
    TVector<TVector<TString>> ExtractLayersFromExpr(const TExprNode::TPtr& node) {
        TVector<TVector<TString>> result;
        VisitExpr(node, [&](TExprNode::TPtr node) {
            bool isScript = node->IsCallable("ScriptUdf");
            if ((node->IsCallable("Udf") && node->ChildrenSize() == 8) || (isScript && node->ChildrenSize() > 4)) {
                for (const auto& setting: node->Child(isScript ? 4 : 7)->Children()) {
                    YQL_ENSURE(setting->Head().IsAtom());
                    if (setting->Head().Content() == "layers") {
                        YQL_ENSURE(setting->ChildrenSize() > 1);
                        TVector<TString> currOrder;
                        currOrder.reserve(setting->ChildrenSize() - 1);
                        for (size_t i = 1; i < setting->ChildrenSize(); ++i) {
                            YQL_ENSURE(setting->Child(i)->IsAtom());
                            currOrder.emplace_back(setting->Child(i)->Content());
                        }
                        result.emplace_back(std::move(currOrder));
                    }
                }
                return false;
            }
            return true;
        });
        Sort(result.begin(), result.end());
        return result;
    }
}
