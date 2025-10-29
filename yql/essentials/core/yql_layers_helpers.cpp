#include "yql_layers_helpers.h"
#include "yql_expr_optimize.h"

namespace NYql {
    TVector<TVector<TString>> ExtractLayersFromExpr(const TExprNode::TPtr& node) {
        TVector<TVector<TString>> result;
        VisitExpr(node, [&](TExprNode::TPtr node) {
            if (node->IsCallable("Udf") && node->ChildrenSize() == 8) {
                for (const auto& setting: node->Child(7)->Children()) {
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
            if (node->IsCallable("ScriptUdf") && node->ChildrenSize() > 4) {
                for (const auto& setting: node->Child(4)->Children()) {
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
        return result;
    }
}
