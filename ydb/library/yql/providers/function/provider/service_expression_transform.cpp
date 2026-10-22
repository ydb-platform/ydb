#include "dq_function_provider_impl.h"
#include "service_expression.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <util/string/cast.h>

namespace NYql::NDqFunction {
    namespace {

        TExprNode::TPtr AttachRowDependency(const TExprNode::TPtr& lambda, TExprContext& ctx) {
            THashMap<const TExprNode*, TExprNode::TPtr> visited;
            std::function<TExprNode::TPtr(const TExprNode::TPtr&)> attach = [&](const auto& part) -> TExprNode::TPtr {
                if (part->IsLambda()) {
                    return part;
                }
                if (auto it = visited.find(part.Get()); it != visited.end()) {
                    return it->second;
                }
                auto children = part->ChildrenList();
                bool changed = false;
                for (auto& child : children) {
                    auto next = attach(child);
                    changed |= next != child;
                    child = next;
                }
                if (part->IsCallable("WasmServiceCall") && children.size() == 3) {
                    // Even constant arguments describe a per-row external effect.
                    // Keep the call in its row scope until async lowering; otherwise
                    // a singleton projection can be evaluated as a pure constant.
                    children.push_back(ctx.NewCallable(part->Pos(), "DependsOn", {lambda->Head().HeadPtr()}));
                    changed = true;
                }
                auto result = changed ? ctx.ChangeChildren(*part, std::move(children)) : part;
                visited.emplace(part.Get(), result);
                return result;
            };
            auto body = attach(lambda->TailPtr());
            return body == lambda->TailPtr() ? lambda : ctx.ChangeChild(*lambda, 1, std::move(body));
        }

        class TDiscovery final: public TSyncTransformerBase {
        public:
            explicit TDiscovery(TDqFunctionState::TPtr state)
                : State(std::move(state))
            {
            }

            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
                TOptimizeExprSettings settings(State->Types);
                settings.VisitTuples = true;
                return OptimizeExpr(input, output, [&](const auto& node, auto& exprCtx) {
                    auto rewritten = RewriteServiceExpression(node, *State->GatewayFactory, exprCtx);
                    if (!rewritten) {
                        return TExprNode::TPtr{};
                    }
                    auto children = rewritten->ChildrenList();
                    bool changed = false;
                    // OptimizeExpr visits lambda bodies but invokes this callback
                    // only for callables/tuples. Bind through their lambda children.
                    for (auto& child : children) {
                        if (child->IsLambda() && child->Head().ChildrenSize() == 1) {
                            auto next = AttachRowDependency(child, exprCtx);
                            changed |= next != child;
                            child = next;
                        }
                    }
                    return changed ? exprCtx.ChangeChildren(*rewritten, std::move(children)) : rewritten;
                }, ctx, settings);
            }

            void Rewind() override {
            }

        private:
            TDqFunctionState::TPtr State;
        };

        TString MaskFieldName(const TStructExprType& type) {
            TString name = "_yql_service_enabled";
            while (type.FindItem(name)) {
                name += "_";
            }
            return name;
        }

        TExprNode::TPtr AddPayloadType(const TTypeAnnotationNode* row, TPositionHandle pos, TExprContext& ctx, bool masked = false) {
            auto expanded = ExpandType(pos, *row, ctx);
            auto fields = expanded->ChildrenList();
            fields.push_back(
                ctx.NewList(pos, {ctx.NewAtom(pos, "_yql_service_payload"), ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "String")})}));
            if (masked) {
                fields.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, MaskFieldName(*row->Cast<TStructExprType>())),
                                                   ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Bool")})}));
            }
            return ctx.NewCallable(pos, "StructType", std::move(fields));
        }

        TExprNode::TPtr EmptyArguments(const TStructExprType& type, TPositionHandle pos, TExprContext& ctx) {
            TExprNode::TListType fields;
            for (const auto* field : type.GetItems()) {
                const auto slot = field->GetItemType()->Cast<TDataExprType>()->GetSlot();
                const auto value = slot == EDataSlot::Bool ? "false" : (slot == EDataSlot::String || slot == EDataSlot::Utf8 ? "" : "0");
                fields.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, field->GetName()),
                                                   ctx.NewCallable(pos, NUdf::GetDataTypeInfo(slot).Name, {ctx.NewAtom(pos, value)})}));
            }
            return ctx.NewCallable(pos, "AsStruct", std::move(fields));
        }

        TExprNode::TPtr Lower(const TExprNode::TPtr& node, TExprContext& ctx) {
            if (!node->IsCallable({"Map", "OrderedMap", "FlatMap", "OrderedFlatMap"}) || node->ChildrenSize() != 2 || !node->Tail().IsLambda() ||
                node->Tail().Head().ChildrenSize() != 1) {
                return node;
            }
            const auto lambda = node->TailPtr();
            struct TGuard {
                TExprNode::TPtr Parent;
                ui32 Index;
                bool Positive;
            };
            using TGuards = std::vector<TGuard>;
            TExprNode::TPtr call;
            TGuards guards;
            bool unsupported = false;
            std::function<void(const TExprNode::TPtr&, const TGuards&, bool)> visit = [&](const auto& part, const auto& path, bool invalid) {
                if (call) {
                    return;
                }
                if (part->IsCallable("WasmServiceCall")) {
                    visit(part->ChildPtr(1), path, invalid);
                    if (!call) {
                        call = part;
                        guards = path;
                        unsupported = invalid;
                    }
                    return;
                }
                if (part->IsCallable("If")) {
                    auto branch = path;
                    for (ui32 i = 0; i + 1 < part->ChildrenSize(); i += 2) {
                        visit(part->ChildPtr(i), branch, invalid);
                        branch.push_back({part, i, true});
                        visit(part->ChildPtr(i + 1), branch, invalid);
                        branch.back().Positive = false;
                    }
                    visit(part->TailPtr(), branch, invalid);
                    return;
                }
                for (const auto& child : part->Children()) {
                    visit(child, path, invalid || part->IsLambda() || part->IsCallable({"IfStrict", "IfPresent", "Coalesce", "And", "Or"}));
                }
            };
            visit(lambda->TailPtr(), {}, false);
            if (!call) {
                return node;
            }
            // A node can have several guarded uses. A single invocation serves
            // their union; replacing all references using only the first path
            // would expose inactive placeholders through the remaining uses.
            std::vector<TGuards> paths;
            std::function<void(const TExprNode::TPtr&, const TGuards&, bool)> uses = [&](const auto& part, const auto& path, bool invalid) {
                if (part == call) {
                    paths.push_back(path);
                    unsupported |= invalid;
                    return;
                }
                if (part->IsCallable("If")) {
                    auto branch = path;
                    for (ui32 i = 0; i + 1 < part->ChildrenSize(); i += 2) {
                        uses(part->ChildPtr(i), branch, invalid);
                        branch.push_back({part, i, true});
                        uses(part->ChildPtr(i + 1), branch, invalid);
                        branch.back().Positive = false;
                    }
                    uses(part->TailPtr(), branch, invalid);
                    return;
                }
                for (const auto& child : part->Children()) {
                    uses(child, path, invalid || part->IsLambda() || part->IsCallable({"IfStrict", "IfPresent", "Coalesce", "And", "Or"}));
                }
            };
            uses(lambda->TailPtr(), {}, false);
            const auto same = [](const TGuard& a, const TGuard& b) { return a.Parent == b.Parent && a.Index == b.Index && a.Positive == b.Positive; };
            std::vector<TGuards> minimal;
            for (const auto& path : paths) {
                bool covered = false;
                for (const auto& existing : minimal) {
                    if (existing.size() <= path.size() && std::equal(existing.begin(), existing.end(), path.begin(), same)) {
                        covered = true;
                    }
                }
                if (covered) {
                    continue;
                }
                std::erase_if(minimal, [&](const auto& existing) {
                    return path.size() <= existing.size() && std::equal(path.begin(), path.end(), existing.begin(), same);
                });
                minimal.push_back(path);
            }
            paths = std::move(minimal);
            guards.clear();
            for (const auto& path : paths) {
                for (const auto& guard : path) {
                    if (std::none_of(guards.begin(), guards.end(), [&](const auto& existing) { return existing.Parent == guard.Parent && existing.Index == guard.Index; })) {
                        guards.push_back(guard);
                    }
                }
            }
            if (unsupported) {
                ctx.AddError(TIssue(ctx.GetPosition(call->Pos()), "Service calls require a row lambda; use IF/CASE for conditional calls"));
                return {};
            }
            const auto inputType = node->Head().GetTypeAnn();
            if (!inputType || inputType->GetKind() != ETypeAnnotationKind::List) {
                ctx.AddError(TIssue(ctx.GetPosition(call->Pos()), "Service expressions require a query row sequence"));
                return {};
            }
            const auto rowType = inputType->Cast<TListExprType>()->GetItemType();
            const auto pos = node->Pos();
            auto argument = ctx.NewArgument(pos, "service_input");
            auto input = node->HeadPtr();
            const TTypeAnnotationNode* payloadType = rowType;
            auto originalInput = argument;
            TExprNode::TPtr enabled;
            const auto boolean = [&](bool value) { return ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, value ? "true" : "false")}); };
            if (!guards.empty()) {
                auto row = ctx.NewArgument(pos, "guard_input");
                TExprNode::TListType fields{ctx.NewList(pos, {ctx.NewAtom(pos, "row"), row})};
                TVector<const TItemExprType*> types{ctx.MakeType<TItemExprType>("row", rowType)};
                const auto indexOf = [&](const TGuard& guard) {
                    for (ui32 i = 0; i < guards.size(); ++i) {
                        if (guards[i].Parent == guard.Parent && guards[i].Index == guard.Index) {
                            return i;
                        }
                    }
                    Y_UNREACHABLE();
                };
                TNodeOnNodeOwnedMap rowRemap;
                rowRemap.emplace(&lambda->Head().Head(), row);
                TVector<TExprNode::TPtr> conditions(guards.size());
                const auto selected = [&](TExprNode::TPtr value, bool positive) {
                    return positive ? value : ctx.NewCallable(pos, "Not", {value});
                };
                std::function<TExprNode::TPtr(ui32)> condition = [&](ui32 index) -> TExprNode::TPtr {
                    if (conditions[index]) {
                        return conditions[index];
                    }
                    auto active = boolean(false);
                    for (const auto& path : paths) {
                        if (std::none_of(path.begin(), path.end(), [&](const auto& guard) { return indexOf(guard) == index; })) {
                            continue;
                        }
                        auto prefix = boolean(true);
                        for (const auto& guard : path) {
                            const auto current = indexOf(guard);
                            if (current == index) {
                                active = ctx.NewCallable(pos, "Or", {active, prefix});
                                break;
                            }
                            prefix = ctx.NewCallable(pos, "And", {prefix, selected(condition(current), guard.Positive)});
                        }
                    }
                    auto value = ctx.ReplaceNodes(guards[index].Parent->ChildPtr(guards[index].Index), rowRemap);
                    return conditions[index] = ctx.NewCallable(pos, "If", {active, value, boolean(false)});
                };
                for (ui32 i = 0; i < guards.size(); ++i) {
                    const auto name = "condition" + ToString(i);
                    fields.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, name), condition(i)}));
                    types.push_back(ctx.MakeType<TItemExprType>(name, ctx.MakeType<TDataExprType>(EDataSlot::Bool)));
                }
                enabled = boolean(false);
                for (const auto& path : paths) {
                    auto active = boolean(true);
                    for (const auto& guard : path) {
                        auto cached = ctx.NewCallable(pos, "Member", {argument, ctx.NewAtom(pos, "condition" + ToString(indexOf(guard)))});
                        active = ctx.NewCallable(pos, "And", {active, selected(cached, guard.Positive)});
                    }
                    enabled = ctx.NewCallable(pos, "Or", {enabled, active});
                }
                auto saved = ctx.NewCallable(pos, "AsStruct", std::move(fields));
                input = ctx.NewCallable(pos, "Map", {input, ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(saved))});
                payloadType = ctx.MakeType<TStructExprType>(types);
                originalInput = ctx.NewCallable(pos, "Member", {argument, ctx.NewAtom(pos, "row")});
            }
            TNodeOnNodeOwnedMap inputRemap;
            inputRemap.emplace(&lambda->Head().Head(), originalInput);
            auto args = ctx.ReplaceNodes(call->ChildPtr(1), inputRemap);
            if (enabled) {
                args = ctx.NewCallable(pos, "If", {enabled, args, EmptyArguments(*call->Child(1)->GetTypeAnn()->Cast<TStructExprType>(), pos, ctx)});
                args = ctx.NewCallable(pos, "AddMember", {args, ctx.NewAtom(pos, MaskFieldName(*call->Child(1)->GetTypeAnn()->Cast<TStructExprType>())), enabled});
            }
            auto encoded = ctx.NewCallable(pos, "AddMember", {args, ctx.NewAtom(pos, "_yql_service_payload"), ctx.NewCallable(pos, "Pickle", {argument})});
            auto projected = ctx.NewCallable(pos, "Map", {input, ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(encoded))});
            auto settings = call->Head().Child(2)->ChildrenList();
            for (auto& entry : settings) {
                if (entry->Head().IsAtom("input_type")) {
                    entry = ctx.NewList(pos, {entry->HeadPtr(), AddPayloadType(call->Child(1)->GetTypeAnn(), pos, ctx, bool(enabled))});
                } else if (entry->Head().IsAtom("output_type")) {
                    entry = ctx.NewList(pos, {entry->HeadPtr(), AddPayloadType(call->GetTypeAnn(), pos, ctx)});
                }
            }
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "enrich_rows"), ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "true")})}));
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "service_occurrence"), ctx.NewCallable(pos, "String", {call->ChildPtr(2)})}));
            auto external = ctx.ChangeChild(call->Head(), 2, ctx.NewList(pos, std::move(settings)));
            auto enriched = ctx.NewCallable(pos, "Apply", {external, projected});
            auto result = ctx.NewArgument(pos, "service_output");
            auto payload = ctx.NewCallable(pos, "Member", {result, ctx.NewAtom(pos, "_yql_service_payload")});
            auto saved = ctx.NewCallable(pos, "Unpickle", {ExpandType(pos, *payloadType, ctx), payload});
            auto original = guards.empty() ? saved : ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, "row")});
            auto value = ctx.NewCallable(pos, "RemoveMember", {result, ctx.NewAtom(pos, "_yql_service_payload")});
            // Restore exactly the decisions made before suspension. Do not reevaluate
            // predicates after an external call, or change other uses of a shared predicate.
            std::function<TExprNode::TPtr(const TExprNode::TPtr&)> continuation = [&](const auto& part) -> TExprNode::TPtr {
                if (part.Get() == &lambda->Head().Head()) {
                    return original;
                }
                if (part == call) {
                    return value;
                }
                auto children = part->ChildrenList();
                bool changed = false;
                for (ui32 i = 0; i < children.size(); ++i) {
                    auto child = continuation(children[i]);
                    for (ui32 g = 0; g < guards.size(); ++g) {
                        if (guards[g].Parent == part && guards[g].Index == i) {
                            child = ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, "condition" + ToString(g))});
                            break;
                        }
                    }
                    changed |= child != children[i];
                    children[i] = child;
                }
                return changed ? ctx.ChangeChildren(*part, std::move(children)) : part;
            };
            auto body = continuation(lambda->TailPtr());
            return ctx.NewCallable(pos, node->Content(), {enriched, ctx.NewLambda(pos, ctx.NewArguments(pos, {result}), std::move(body))});
        }

        class TLogical final: public TSyncTransformerBase {
        public:
            explicit TLogical(TDqFunctionState::TPtr state)
                : State(std::move(state))
            {
            }

            TStatus DoTransform(TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) override {
                return OptimizeExpr(input, output, &Lower, ctx, TOptimizeExprSettings(State->Types));
            }

            void Rewind() override {
            }

        private:
            TDqFunctionState::TPtr State;
        };

    } // namespace

    THolder<IGraphTransformer> CreateServiceExpressionDiscovery(TDqFunctionState::TPtr state) {
        return MakeHolder<TDiscovery>(std::move(state));
    }

    THolder<IGraphTransformer> CreateServiceExpressionLowering(TDqFunctionState::TPtr state) {
        return MakeHolder<TLogical>(std::move(state));
    }

} // namespace NYql::NDqFunction
