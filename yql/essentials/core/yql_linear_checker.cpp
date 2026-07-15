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

    explicit TUsageVisitor(TExprContext& ctx)
        : Ctx_(ctx)
    {}

    void Visit(const TExprNode& node, const TExprNode* parent, const bool noSecondUsageCheck = false) {
        auto [it, inserted] = Visited_.emplace(&node, TUsage{});
        if (node.GetTypeAnn()->HasStaticLinear()) {
            auto scope = node.GetDependencyScope();
            if (scope && parent) {
                auto scopeParent = parent->GetDependencyScope();
                if (scopeParent && scopeParent->first != scope->first) {
                    AddScopeError(node.Pos(), parent->Pos());
                    return;
                }
            }

            if (node.GetTypeAnn()->IsLinear()) {
                it->second.resize(1);
                if (it->second[0]) {
                    if (noSecondUsageCheck) {
                        // Allow re-usage in If nodes, will validate symmetry later
                    } else {
                        AddError(*it->second[0], parent->Pos(), node.Pos());
                        return;
                    }
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
            if (node.IsCallable("If")) {
                Visit(*node.Child(1), &node);
                Visit(*node.Child(2), &node, /*noSecondUsageCheck=*/true);
                Visit(*node.Child(0), &node);
                HandleIfNode(node, parent);
            }
            else {
                for (const auto& child : node.Children()) {
                    Visit(*child, &node, noSecondUsageCheck);
                }
            }
        }
    }

    bool GetLinearObjects(const TExprNode& node, TNodeMap<TUsage>& result) {

        const bool isLiteral = (node.GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple && node.IsList()) ||
                               node.IsCallable("AsStruct");

        if (!isLiteral) {
            return false;
        }

        for (const auto& child : node.Children()) {
            if (child->GetTypeAnn()->IsLinear()) {
                TUsage usage;
                usage.resize(1);
                usage[0] = node.Pos();
                auto [it, inserted] = result.try_emplace(child.Get(), usage);
                if (!inserted) {
                    AddError(*it->second[0], node.Pos(), child.Get()->Pos());
                }
            }
        }

        return true;

    }

    void HandleIfNode(const TExprNode& node, const TExprNode* /*parent*/) {
        auto thenType = node.Child(1)->GetTypeAnn();
        auto elseType = node.Child(2)->GetTypeAnn();

        if (!thenType->HasStaticLinear() && !elseType->HasStaticLinear()) {
            return;
        }

        if ((thenType->IsLinear() || elseType->IsLinear()) &&
            node.Child(1) != node.Child(2)) {
            AddError(node.Pos(), "If expression with a linear result requires identical true and false branches to guarantee exactly-once consumption; consider using DynamicLinear.");
        }

        if (thenType->HasStaticLinear() && !elseType->HasStaticLinear()) {
            AddError(node.Pos(), "The THEN branch of the If expression has a static linear type, whereas the ELSE branch does not. Consider using DynamicLinear.");
        }
        if (elseType->HasStaticLinear() && !thenType->HasStaticLinear()) {
            AddError(node.Pos(), "The ELSE branch of the If expression has a static linear type, whereas the THEN branch does not. Consider using DynamicLinear.");
        }

        TNodeMap<TUsage> linearObjectsInTrue;
        TNodeMap<TUsage> linearObjectsInFalse;
        if (!GetLinearObjects(*node.Child(1), linearObjectsInTrue) ||
            !GetLinearObjects(*node.Child(2), linearObjectsInFalse)) {
            return;
        }

        for (const auto& [obj, _] : linearObjectsInTrue) {
            if (linearObjectsInFalse.find(obj) == linearObjectsInFalse.end()) {
                AddIfError(obj->Pos(), node.Pos());
            }
        }
        for (const auto& [obj, _] : linearObjectsInFalse) {
            if (linearObjectsInTrue.find(obj) == linearObjectsInTrue.end()) {
                AddIfError(obj->Pos(), node.Pos());
            }
        }
    }

    void Finish() {
        for (const auto& [node, usage] : Visited_) {
            if (!node->GetTypeAnn()->HasStaticLinear()) {
                continue;
            }
            if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Tuple) {
                auto tupleType = node->GetTypeAnn()->Cast<TTupleExprType>();
                YQL_ENSURE(tupleType->GetSize() == usage.size());
                for (ui32 i = 0; i < usage.size(); ++i) {
                    if (!tupleType->GetItems()[i]->IsLinear()) {
                        continue;
                    }

                    if (!usage[i].Defined()) {
                        AddError(node->Pos(), TStringBuilder() << "Element #" << i
                            << " is not consumed, type: " << *tupleType->GetItems()[i]);
                    }
                }
            } else if (node->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Struct) {
                auto structType = node->GetTypeAnn()->Cast<TStructExprType>();
                YQL_ENSURE(structType->GetSize() == usage.size());
                for (ui32 i = 0; i < usage.size(); ++i) {
                    if (!structType->GetItems()[i]->GetItemType()->IsLinear()) {
                        continue;
                    }

                    if (!usage[i].Defined()) {
                        AddError(node->Pos(), TStringBuilder() << "Member '" << structType->GetItems()[i]->GetName()
                            << "' is not consumed, type: " << *structType->GetItems()[i]->GetItemType());
                    }
                }
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

    void AddScopeError(TPositionHandle produced, TPositionHandle parent) {
        HasErrors_ = true;
        auto inner = MakeIntrusive<TIssue>(Ctx_.GetPosition(produced), "Linear value is produced here");
        auto main = TIssue(Ctx_.GetPosition(parent), "The linear value changed lambda scope");
        main.AddSubIssue(inner);
        Ctx_.AddError(main);
    }

    void AddIfError(TPositionHandle produced, TPositionHandle parent) {
        HasErrors_ = true;
        auto inner = MakeIntrusive<TIssue>(Ctx_.GetPosition(produced), "Cannot guarantee single use in static analysis, consider using DynamicLinear. Linear objects have to be symmetric used in both branches.");
        auto main = TIssue(Ctx_.GetPosition(parent), "The linear value did not pass static analysis");
        main.AddSubIssue(inner);
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
    visitor.Visit(root, /*parent=*/nullptr);
    visitor.Finish();
    return !visitor.HasErrors();
}

}
