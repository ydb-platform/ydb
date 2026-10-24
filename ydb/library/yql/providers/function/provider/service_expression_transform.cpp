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

        // Preserve shared argument values, not just the input row: evaluating
        // RandomNumber again in the continuation would change SQL row semantics.
        TExprNode::TListType SharedArguments(const TExprNode::TPtr& body, const TExprNode::TPtr& call) {
            THashSet<const TExprNode*> arguments;
            std::function<void(const TExprNode::TPtr&)> collect = [&](const auto& part) {
                if (part->IsLambda() || !arguments.insert(part.Get()).second) {
                    return;
                }
                for (const auto& child : part->Children()) {
                    collect(child);
                }
            };
            collect(call->ChildPtr(1));
            TExprNode::TListType shared;
            THashSet<const TExprNode*> visited;
            std::function<void(const TExprNode::TPtr&)> find = [&](const auto& part) {
                if (part == call || part->IsLambda() || !visited.insert(part.Get()).second) {
                    return;
                }
                if (part->IsCallable() && arguments.contains(part.Get()) && part->GetTypeAnn() && part->GetTypeAnn()->IsPersistable()) {
                    shared.push_back(part);
                    return;
                }
                for (const auto& child : part->Children()) {
                    find(child);
                }
            };
            find(body);
            return shared;
        }

        TExprNode::TPtr Lower(const TExprNode::TPtr& node, TExprContext& ctx) {
            if (!node->IsCallable({"Map", "OrderedMap", "FlatMap", "OrderedFlatMap"}) || node->ChildrenSize() != 2 ||
                !node->Tail().IsLambda() || node->Tail().Head().ChildrenSize() != 1) {
                return node;
            }
            const auto lambda = node->TailPtr();
            struct TGuard {
                TExprNode::TPtr Parent;
                ui32 Index;
                bool Positive;
            };
            using TGuards = std::vector<TGuard>;
            using TPaths = std::vector<TGuards>;
            TExprNode::TPtr call;
            std::function<void(const TExprNode::TPtr&)> first = [&](const auto& part) {
                if (call) {
                    return;
                }
                if (part->IsCallable("WasmServiceCall")) {
                    first(part->ChildPtr(1));
                    if (!call) {
                        call = part;
                    }
                    return;
                }
                for (const auto& child : part->Children()) {
                    first(child);
                }
            };
            first(lambda->TailPtr());
            if (!call) {
                return node;
            }
            bool unsupported = false;
            const auto usePaths = [&](const TExprNode::TPtr& target) {
                TPaths paths;
                std::function<void(const TExprNode::TPtr&, const TGuards&, bool)> visit = [&](const auto& part, const auto& path, bool invalid) {
                    if (part == target) {
                        paths.push_back(path);
                        if (target == call) {
                            unsupported |= invalid;
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
                const auto same = [](const TGuard& a, const TGuard& b) { return a.Parent == b.Parent && a.Index == b.Index && a.Positive == b.Positive; };
                TPaths minimal;
                for (const auto& path : paths) {
                    if (std::any_of(minimal.begin(), minimal.end(), [&](const auto& other) {
                            return other.size() <= path.size() && std::equal(other.begin(), other.end(), path.begin(), same);
                        })) {
                        continue;
                    }
                    std::erase_if(minimal, [&](const auto& other) {
                        return path.size() <= other.size() && std::equal(path.begin(), path.end(), other.begin(), same);
                    });
                    minimal.push_back(path);
                }
                return minimal;
            };
            const auto paths = usePaths(call);
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
            const auto shared = SharedArguments(lambda->TailPtr(), call);
            std::function<bool(const TExprNode::TPtr&)> hasService = [&](const auto& part) {
                if (part->IsCallable("WasmServiceCall")) {
                    return true;
                }
                for (const auto& child : part->Children()) {
                    if (hasService(child)) {
                        return true;
                    }
                }
                return false;
            };
            // Save only values that are demanded before this invocation. Uses
            // guarded by a later service result are evaluated lazily on resume.
            std::vector<TPaths> capturePaths;
            TPaths allPaths = paths;
            for (const auto& value : shared) {
                auto uses = usePaths(value);
                std::erase_if(uses, [&](const auto& path) {
                    return std::any_of(path.begin(), path.end(), [&](const auto& guard) { return hasService(guard.Parent->ChildPtr(guard.Index)); });
                });
                allPaths.insert(allPaths.end(), uses.begin(), uses.end());
                capturePaths.push_back(std::move(uses));
            }
            TGuards guards;
            for (const auto& path : allPaths) {
                for (const auto& guard : path) {
                    if (std::none_of(guards.begin(), guards.end(), [&](const auto& other) { return other.Parent == guard.Parent && other.Index == guard.Index; })) {
                        guards.push_back(guard);
                    }
                }
            }
            const auto indexOf = [&](const TGuard& guard) {
                for (ui32 i = 0; i < guards.size(); ++i) {
                    if (guards[i].Parent == guard.Parent && guards[i].Index == guard.Index) {
                        return i;
                    }
                }
                Y_UNREACHABLE();
            };
            const auto boolean = [&](bool value) { return ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, value ? "true" : "false")}); };
            const auto selected = [&](TExprNode::TPtr value, bool positive) { return positive ? value : ctx.NewCallable(pos, "Not", {value}); };
            TVector<TExprNode::TPtr> conditions(guards.size()), activeConditions(guards.size());
            std::function<TExprNode::TPtr(ui32)> condition = [&](ui32 index) -> TExprNode::TPtr {
                if (conditions[index]) {
                    return conditions[index];
                }
                auto active = boolean(false);
                for (const auto& path : allPaths) {
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
                activeConditions[index] = active;
                return conditions[index] = ctx.NewCallable(pos, "If", {active, guards[index].Parent->ChildPtr(guards[index].Index), boolean(false)});
            };
            const auto enabledFor = [&](const TPaths& uses) {
                auto enabled = boolean(false);
                for (const auto& path : uses) {
                    auto active = boolean(true);
                    for (const auto& guard : path) {
                        active = ctx.NewCallable(pos, "And", {active, selected(condition(indexOf(guard)), guard.Positive)});
                    }
                    enabled = ctx.NewCallable(pos, "Or", {enabled, active});
                }
                return enabled;
            };
            const bool masked = paths.size() != 1 || !paths.front().empty();
            auto enabled = enabledFor(paths);
            TExprNode::TListType payloadFields{ctx.NewList(pos, {ctx.NewAtom(pos, "row"), lambda->Head().HeadPtr()})};
            TVector<const TItemExprType*> payloadTypes{ctx.MakeType<TItemExprType>("row", rowType)};
            const auto save = [&](TString name, const TExprNode::TPtr& active, const TExprNode::TPtr& value, const TTypeAnnotationNode* type) {
                const auto* optional = ctx.MakeType<TOptionalExprType>(type);
                auto field = ctx.NewCallable(pos, "If", {active, ctx.NewCallable(pos, "Just", {value}), ctx.NewCallable(pos, "Nothing", {ExpandType(pos, *optional, ctx)})});
                payloadFields.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, name), field}));
                payloadTypes.push_back(ctx.MakeType<TItemExprType>(name, optional));
            };
            for (ui32 i = 0; i < guards.size(); ++i) {
                condition(i);
                save("condition" + ToString(i), activeConditions[i], guards[i].Parent->ChildPtr(guards[i].Index), ctx.MakeType<TDataExprType>(EDataSlot::Bool));
            }
            for (ui32 i = 0; i < shared.size(); ++i) {
                save("captured" + ToString(i), enabledFor(capturePaths[i]), shared[i], shared[i]->GetTypeAnn());
            }
            auto args = call->ChildPtr(1);
            if (masked) {
                args = ctx.NewCallable(pos, "If", {enabled, args, EmptyArguments(*call->Child(1)->GetTypeAnn()->Cast<TStructExprType>(), pos, ctx)});
                args = ctx.NewCallable(pos, "AddMember", {args, ctx.NewAtom(pos, MaskFieldName(*call->Child(1)->GetTypeAnn()->Cast<TStructExprType>())), enabled});
            }
            // Build arguments, predicates and saved values in one row scope so
            // common volatile expressions retain their single-row identity.
            const auto* payloadType = ctx.MakeType<TStructExprType>(payloadTypes);
            auto payload = ctx.NewCallable(pos, "Pickle", {ctx.NewCallable(pos, "AsStruct", std::move(payloadFields))});
            auto encoded = ctx.NewCallable(pos, "AddMember", {args, ctx.NewAtom(pos, "_yql_service_payload"), payload});
            auto argument = ctx.NewArgument(pos, "service_input");
            TNodeOnNodeOwnedMap inputRemap;
            inputRemap.emplace(&lambda->Head().Head(), argument);
            encoded = ctx.ReplaceNodes(std::move(encoded), inputRemap);
            auto projected = ctx.NewCallable(pos, "Map", {node->HeadPtr(), ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(encoded))});
            auto settings = call->Head().Child(2)->ChildrenList();
            for (auto& entry : settings) {
                if (entry->Head().IsAtom("input_type")) {
                    entry = ctx.NewList(pos, {entry->HeadPtr(), AddPayloadType(call->Child(1)->GetTypeAnn(), pos, ctx, masked)});
                } else if (entry->Head().IsAtom("output_type")) {
                    entry = ctx.NewList(pos, {entry->HeadPtr(), AddPayloadType(call->GetTypeAnn(), pos, ctx)});
                }
            }
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "enrich_rows"), ctx.NewCallable(pos, "Bool", {ctx.NewAtom(pos, "true")})}));
            settings.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, "service_occurrence"), ctx.NewCallable(pos, "String", {call->ChildPtr(2)})}));
            auto external = ctx.ChangeChild(call->Head(), 2, ctx.NewList(pos, std::move(settings)));
            auto enriched = ctx.NewCallable(pos, "Apply", {external, projected});
            auto result = ctx.NewArgument(pos, "service_output");
            auto saved = ctx.NewCallable(pos, "Unpickle", {ExpandType(pos, *payloadType, ctx), ctx.NewCallable(pos, "Member", {result, ctx.NewAtom(pos, "_yql_service_payload")})});
            auto original = ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, "row")});
            auto value = ctx.NewCallable(pos, "RemoveMember", {result, ctx.NewAtom(pos, "_yql_service_payload")});
            const auto restore = [&](TString name, TExprNode::TPtr fallback) {
                auto cached = ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, name)});
                return ctx.NewCallable(pos, "If", {ctx.NewCallable(pos, "Exists", {cached}), ctx.NewCallable(pos, "Unwrap", {cached}), fallback});
            };
            std::function<TExprNode::TPtr(const TExprNode::TPtr&, bool)> continuation = [&](const auto& part, bool useCapture) -> TExprNode::TPtr {
                if (part.Get() == &lambda->Head().Head()) {
                    return original;
                }
                if (part == call) {
                    return value;
                }
                if (useCapture) {
                    for (ui32 i = 0; i < shared.size(); ++i) {
                        if (shared[i] == part) {
                            return restore("captured" + ToString(i), continuation(part, false));
                        }
                    }
                }
                auto children = part->ChildrenList();
                bool changed = false;
                for (ui32 i = 0; i < children.size(); ++i) {
                    auto child = continuation(children[i], true);
                    for (ui32 g = 0; g < guards.size(); ++g) {
                        if (guards[g].Parent == part && guards[g].Index == i) {
                            child = restore("condition" + ToString(g), child);
                            break;
                        }
                    }
                    changed |= child != children[i];
                    children[i] = child;
                }
                return changed ? ctx.ChangeChildren(*part, std::move(children)) : part;
            };
            return ctx.NewCallable(pos, node->Content(), {enriched, ctx.NewLambda(pos, ctx.NewArguments(pos, {result}), continuation(lambda->TailPtr(), true))});
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
