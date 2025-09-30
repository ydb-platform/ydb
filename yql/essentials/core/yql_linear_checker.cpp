#include "yql_linear_checker.h"
#include "yql_expr_optimize.h"

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>

namespace NYql {

namespace {

bool ValidateLinearTypeAnn(TPositionHandle pos, const TTypeAnnotationNode& type, TExprContext& ctx) {
    bool hasError = false;
    if (type.IsLinear()) {
        // ok
    } else if (type.GetKind() == ETypeAnnotationKind::Tuple) {
        auto tupleType = type.Cast<TTupleExprType>();
        for (const auto& item : tupleType->GetItems()) {
            if (!item->HasStaticLinear()) {
                continue;
            }

            if (!item->IsLinear()) {
                hasError = true;
                break;
            }
        }
    } else if (type.GetKind() == ETypeAnnotationKind::Struct) {
        auto structType = type.Cast<TStructExprType>();
        for (const auto& item : structType->GetItems()) {
            if (!item->GetItemType()->HasStaticLinear()) {
                continue;
            }

            if (!item->GetItemType()->IsLinear()) {
                hasError = true;
                break;
            }
        }
    } else {
        hasError = true;
    }

    if (hasError) {
        ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() <<
            "Linear types can be used either directly or via Struct/Tuple (non-recursive), but got type " <<
            type << "\nConsider using ToDynamicLinear function"));
    }

    return !hasError;
}

class TUsageVisitor
{
public:
    // length = 1 for Linear, N for Struct/Tuple
    using TUsage = TStackVec<TMaybe<TPositionHandle>, 1>;

    TUsageVisitor(TExprContext& ctx)
        : Ctx_(ctx)
    {}

    void Visit(const TExprNode& node, const TExprNode* parent) {
        auto [it, inserted] = Visited_.emplace(&node, TUsage{});
        if (node.GetTypeAnn()->HasStaticLinear()) {
            if (node.GetTypeAnn()->IsLinear()) {
                it->second.resize(1);
                if (it->second[0]) {
                    AddError(*it->second[0], parent->Pos(), node.Pos());
                    return;
                } else {
                    it->second[0] = parent->Pos();
                }
            } else if (node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = node.GetTypeAnn()->Cast<TTupleExprType>();
                it->second.resize(tupleType->GetSize());
                if (parent->IsCallable("Nth")) {
                    auto index = FromString<ui32>(parent->Tail().Content());
                    if (tupleType->GetItems()[index]->IsLinear()) {
                        if (it->second[index]) {
                            AddError(*it->second[index], parent->Pos(), node.Pos());
                            return;
                        } else {
                            it->second[index] = parent->Pos();
                        }
                    }
                } else {
                    // all items consumed
                    for (ui32 i = 0; i < tupleType->GetSize(); ++i) {
                        if (tupleType->GetItems()[i]->IsLinear()) {
                            if (it->second[i]) {
                                AddError(*it->second[i], parent->Pos(), node.Pos());
                                return;
                            } else {
                                it->second[i] = parent->Pos();
                            }
                        }
                    }
                }
            } else {
                auto structType = node.GetTypeAnn()->Cast<TStructExprType>();
                it->second.resize(structType->GetSize());
                if (parent->IsCallable("Member")) {
                    auto index = structType->FindItem(parent->Tail().Content());
                    YQL_ENSURE(index);
                    if (structType->GetItems()[*index]->GetItemType()->IsLinear()) {
                        if (it->second[*index]) {
                            AddError(*it->second[*index], parent->Pos(), node.Pos());
                            return;
                        } else {
                            it->second[*index] = parent->Pos();
                        }
                    }
                } else {
                    // all items consumed
                    for (ui32 i = 0; i < structType->GetSize(); ++i) {
                        if (structType->GetItems()[i]->GetItemType()->IsLinear()) {
                            if (it->second[i]) {
                                AddError(*it->second[i], parent->Pos(), node.Pos());
                                return;
                            } else {
                                it->second[i] = parent->Pos();
                            }
                        }
                    }
                }
            }
        }

        if (!inserted) {
            return;
        }

        if (node.IsLambda()) {
            // validate arg & bodies
            bool isValid = true;
            for (const auto& arg: node.Head().Children()) {
                if (arg->GetTypeAnn()->HasStaticLinear()) {
                    isValid = false;
                    AddError(arg->Pos(), "An argument of a lambda should not be a linear type");
                }
            }

            for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
                if (node.Child(i)->GetTypeAnn()->HasStaticLinear()) {
                    isValid = false;
                    AddError(node.Child(i)->Pos(), "A lambda body should not be a linear type");
                }
            }

            if (!isValid) {
                return;
            }

            for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
                Visit(*node.Child(i), parent);
            }
        } else {
            for (const auto& child : node.Children()) {
                Visit(*child, &node);
            }
        }
    }

    bool HasErrors() const {
        return HasErrors_;
    }

private:
    void AddError(TPositionHandle firstUsage, TPositionHandle secondUsage, TPositionHandle linear) {
        HasErrors_ = true;
        auto first = MakeIntrusive<TIssue>(Ctx_.GetPosition(firstUsage), "First usage of the value");
        auto second = MakeIntrusive<TIssue>(Ctx_.GetPosition(secondUsage), "Second usage of the value");
        auto main = TIssue(Ctx_.GetPosition(linear), "The linear value has already been used");
        main.AddSubIssue(first);
        main.AddSubIssue(second);
        Ctx_.AddError(main);
    }

    void AddError(TPositionHandle pos, const TString& message) {
        HasErrors_ = true;
        Ctx_.AddError(TIssue(Ctx_.GetPosition(pos), message));
    }

private:
    TExprContext& Ctx_;
    bool HasErrors_ = false;
    TNodeMap<TUsage> Visited_;
};

}

bool ValidateLinearTypes(const TExprNode& root, TExprContext& ctx) {
    bool hasErrors = false;
    THashSet<const TTypeAnnotationNode*> scannedTypes;
    VisitExpr(root, [&](const TExprNode& node) {
        if (node.GetTypeAnn()->HasStaticLinear()) {
            auto [_, inserted] = scannedTypes.emplace(node.GetTypeAnn());
            if (inserted) {
                if (!ValidateLinearTypeAnn(node.Pos(), *node.GetTypeAnn(), ctx)) {
                    hasErrors = true;
                    return false;
                }
            }
        }

        return true;
    });

    YQL_CLOG(INFO, Core) << "Scanned " << scannedTypes.size() << " static linear types";
    if (hasErrors) {
        return false;
    }

    TUsageVisitor visitor(ctx);
    visitor.Visit(root, nullptr);
    return !visitor.HasErrors();
}

}
