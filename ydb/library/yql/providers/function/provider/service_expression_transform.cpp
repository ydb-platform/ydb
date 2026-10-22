#include "dq_function_provider_impl.h"
#include "service_expression.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <util/string/cast.h>

namespace NYql::NDqFunction {
    namespace {

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
                    return RewriteServiceExpression(node, *State->GatewayFactory, exprCtx);
                }, ctx, settings);
            }

            void Rewind() override {
            }

        private:
            TDqFunctionState::TPtr State;
        };

        TExprNode::TPtr AddPayloadType(const TTypeAnnotationNode* row, TPositionHandle pos, TExprContext& ctx) {
            auto expanded = ExpandType(pos, *row, ctx);
            auto fields = expanded->ChildrenList();
            fields.push_back(
                ctx.NewList(pos, {ctx.NewAtom(pos, "_yql_service_payload"), ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "String")})}));
            return ctx.NewCallable(pos, "StructType", std::move(fields));
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
            if (!node->IsCallable({"Map", "OrderedMap", "FlatMap", "OrderedFlatMap"}) || node->ChildrenSize() != 2 || !node->Tail().IsLambda() ||
                node->Tail().Head().ChildrenSize() != 1) {
                return node;
            }
            const auto lambda = node->TailPtr();
            TExprNode::TPtr call;
            bool guarded = false, nested = false;
            std::function<void(const TExprNode::TPtr&, bool, bool)> visit = [&](const auto& part, bool condition, bool innerLambda) {
                if (call) {
                    return;
                }
                if (part->IsCallable("WasmServiceCall")) {
                    // Dependencies first: lower the innermost service call in a chain.
                    visit(part->ChildPtr(1), condition, innerLambda);
                    if (!call) {
                        call = part;
                        guarded = condition;
                        nested = innerLambda;
                    }
                    return;
                }
                for (const auto& child : part->Children()) {
                    visit(child, condition || part->IsCallable({"If", "IfStrict", "IfPresent", "Coalesce", "And", "Or"}),
                          innerLambda || part->IsLambda());
                }
            };
            visit(lambda->TailPtr(), false, false);
            if (!call) {
                return node;
            }
            if (guarded || nested) {
                ctx.AddError(TIssue(ctx.GetPosition(call->Pos()), "Service calls in conditional or nested collection lambdas are not supported here"));
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
            const auto shared = SharedArguments(lambda->TailPtr(), call);
            auto input = node->HeadPtr();
            const TTypeAnnotationNode* payloadType = rowType;
            auto originalInput = argument;
            if (!shared.empty()) {
                auto row = ctx.NewArgument(pos, "capture_input");
                TNodeOnNodeOwnedMap remap;
                remap.emplace(&lambda->Head().Head(), row);
                TExprNode::TListType fields{ctx.NewList(pos, {ctx.NewAtom(pos, "row"), row})};
                TVector<const TItemExprType*> types{ctx.MakeType<TItemExprType>("row", rowType)};
                for (ui32 i = 0; i < shared.size(); ++i) {
                    const auto name = "captured" + ToString(i);
                    fields.push_back(ctx.NewList(pos, {ctx.NewAtom(pos, name), ctx.ReplaceNodes(TExprNode::TPtr(shared[i]), remap)}));
                    types.push_back(ctx.MakeType<TItemExprType>(name, shared[i]->GetTypeAnn()));
                }
                input = ctx.NewCallable(pos, "Map", {input, ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), ctx.NewCallable(pos, "AsStruct", std::move(fields)))});
                payloadType = ctx.MakeType<TStructExprType>(types);
                originalInput = ctx.NewCallable(pos, "Member", {argument, ctx.NewAtom(pos, "row")});
            }
            TNodeOnNodeOwnedMap inputRemap;
            inputRemap.emplace(&lambda->Head().Head(), originalInput);
            for (ui32 i = 0; i < shared.size(); ++i) {
                inputRemap.emplace(shared[i].Get(), ctx.NewCallable(pos, "Member", {argument, ctx.NewAtom(pos, "captured" + ToString(i))}));
            }
            auto args = ctx.ReplaceNodes(call->ChildPtr(1), inputRemap);
            auto encoded =
                ctx.NewCallable(pos, "AddMember", {args, ctx.NewAtom(pos, "_yql_service_payload"), ctx.NewCallable(pos, "Pickle", {argument})});
            auto projected = ctx.NewCallable(pos, "Map", {input, ctx.NewLambda(pos, ctx.NewArguments(pos, {argument}), std::move(encoded))});
            auto settings = call->Head().Child(2)->ChildrenList();
            for (auto& entry : settings) {
                if (entry->Head().IsAtom("input_type")) {
                    entry = ctx.NewList(pos, {entry->HeadPtr(), AddPayloadType(call->Child(1)->GetTypeAnn(), pos, ctx)});
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
            auto original = shared.empty() ? saved : ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, "row")});
            auto value = ctx.NewCallable(pos, "RemoveMember", {result, ctx.NewAtom(pos, "_yql_service_payload")});
            TNodeOnNodeOwnedMap outputRemap;
            outputRemap.emplace(&lambda->Head().Head(), original);
            outputRemap.emplace(call.Get(), value);
            for (ui32 i = 0; i < shared.size(); ++i) {
                outputRemap.emplace(shared[i].Get(), ctx.NewCallable(pos, "Member", {saved, ctx.NewAtom(pos, "captured" + ToString(i))}));
            }
            auto body = ctx.ReplaceNodes(lambda->TailPtr(), outputRemap);
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
