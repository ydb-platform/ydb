#include "kqp_rbo_test_helpers.h"

#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_aggregation_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_convertion_utils.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_union_all_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_source_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_lookup_join_builder.h>
#include <ydb/core/kqp/opt/rbo/kqp_olap_expr_inspection.h>
#include <ydb/core/kqp/opt/rbo/kqp_plan_conversion_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_transformer.h>

#include <yql/essentials/core/yql_expr_optimize.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpRboIdLowering) {
    Y_UNIT_TEST(UpsertKeepsDefaultColumnsThroughLowering) {
        NTests::TIdTestContext f;
        auto& ctx = f.ExprCtx;
        const auto pos = f.Pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        const auto* type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        constant->TailPtr()->SetTypeAnn(type);
        auto columns = ctx.NewList(pos, {ctx.NewAtom(pos, "value")});
        auto input = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {
            ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {empty, ctx.NewAtom(pos, "value"), constant, ctx.NewAtom(pos, "false")})
        })});
        input->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("value", type)})));
        auto root = ctx.NewCallable(pos, "KqpOpRoot", {input, columns});
        const auto table = NNodes::Build<NNodes::TKqpTable>(ctx, pos)
            .Path().Build("/Root/table").PathId().Build("1:1")
            .SysView().Build("").Version().Build("1").Done();
        const auto upsert = NNodes::Build<NNodes::TKqlUpsertRows>(ctx, pos)
            .Table(table)
            .Input(root)
            .Columns(columns)
            .ReturningColumns().Build()
            .IsBatch().Build("false")
            .DefaultColumns(columns)
            .Settings().Build()
            .Done();

        const auto rewritten = RewriteTableEffect(upsert.Ptr(), ctx, f.KqpCtx);
        auto plan = PlanConverter(f.TypeCtx, ctx).ConvertRoot(rewritten, nullptr);
        auto& effect = CastOperator<TOpTableEffect>(*plan->GetInput());
        UNIT_ASSERT_VALUES_EQUAL(effect.GetExplainName(), "UpsertRows");
        const NNodes::TKqpTableSinkSettings settings(effect.BuildSettings(ctx));
        UNIT_ASSERT_VALUES_EQUAL(settings.DefaultColumns().Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(settings.DefaultColumns().Item(0).StringValue(), "value");
    }

    Y_UNIT_TEST(TypeAnnotationAllowsSharingOnlyThroughDistinctReplicatePorts) {
        // Extra local references are harmless. Reusing an ordinary subtree or
        // one output port in two plan slots violates the binding invariant.
        enum class EShape {
            SameSubtree,
            DistinctPorts,
            SamePort,
            PortAndNakedSource,
            PortsOfTwoReplicates,
            NestedDistinctPorts,
        };
        for (const bool packed : {false, true}) {
            for (const auto shape : {EShape::SameSubtree, EShape::DistinctPorts, EShape::SamePort,
                     EShape::PortAndNakedSource, EShape::PortsOfTwoReplicates, EShape::NestedDistinctPorts}) {
                NTests::TIdTestContext f;
                auto source = MakeIntrusive<TOpEmptySource>(f.Pos);
                auto hub = TReplicate::Create(source, f.Pos, f.Props.InfoUnitRegistry);
                auto port = hub->AddOutput();
                TIntrusivePtr<IOperator> left = source, right = source;
                switch (shape) {
                    case EShape::SameSubtree:
                        break;
                    case EShape::DistinctPorts:
                        left = port;
                        right = hub->AddOutput();
                        break;
                    case EShape::SamePort:
                        left = right = port;
                        break;
                    case EShape::PortAndNakedSource:
                        left = port; // A naked use may not bypass the binding.
                        break;
                    case EShape::PortsOfTwoReplicates:
                        left = port;
                        right = TReplicate::Create(source, f.Pos, f.Props.InfoUnitRegistry)->AddOutput();
                        break;
                    case EShape::NestedDistinctPorts: {
                        auto nested = TReplicate::Create(port, f.Pos, f.Props.InfoUnitRegistry);
                        left = nested->AddOutput();
                        right = nested->AddOutput();
                        break;
                    }
                }
                TIntrusivePtr<IOperator> plan = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Cross", TJoinIUs{});
                if (packed) {
                    plan = MakeIntrusive<TOpCBOTree>(plan, f.Pos);
                }
                auto root = f.Root(plan, {});
                const auto status = root->ComputeTypes(f.RboCtx);
                const bool valid = shape == EShape::DistinctPorts || shape == EShape::NestedDistinctPorts;
                UNIT_ASSERT_C(status == (valid ? IGraphTransformer::TStatus::Ok : IGraphTransformer::TStatus::Error),
                    f.ExprCtx.IssueManager.GetIssues().ToString());
            }
        }

        // Two subplan bindings, or a subplan and the main plan, must not own
        // the same ordinary subtree either.
        for (const bool shareWithMain : {false, true}) {
            NTests::TIdTestContext f;
            const auto callA = f.Id(), callB = f.Id();
            auto source = MakeIntrusive<TOpEmptySource>(f.Pos);
            f.Props.Subplans.Add(callA, source, ESubplanType::EXISTS);
            f.Props.Subplans.Add(callB, source, ESubplanType::EXISTS);
            TIntrusivePtr<IOperator> plan = shareWithMain ? source : MakeIntrusive<TOpEmptySource>(f.Pos);
            plan = MakeIntrusive<TOpFilter>(plan, f.Pos, f.Column(callA));
            if (!shareWithMain) {
                plan = MakeIntrusive<TOpFilter>(plan, f.Pos, f.Column(callB));
            }
            auto root = f.Root(plan, {});
            UNIT_ASSERT(root->ComputeTypes(f.RboCtx) == IGraphTransformer::TStatus::Error);
            UNIT_ASSERT_C(f.ExprCtx.IssueManager.GetIssues().ToString().Contains("sharing requires distinct ports of the same Replicate"),
                f.ExprCtx.IssueManager.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ErrorScopePreservesLeafIssuesAndNamesBindings) {
        NTests::TIdTestContext f;
        const auto result = f.Id("before");
        TMapIUs definitions;
        definitions.Add(result, f.Constant());
        auto root = f.Root(MakeIntrusive<TOpMap>(MakeIntrusive<TOpEmptySource>(f.Pos),
            f.Pos, std::move(definitions)), {{result, "result"}});

        auto transformer = CreateFunctorTransformer([](TExprNode::TPtr input, TExprNode::TPtr& output, TExprContext& ctx) {
            output = input;
            ctx.AddError(TIssue(ctx.GetPosition(input->Pos()), "Original YQL leaf diagnostic"));
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext ctx(f.KqpCtx, f.ExprCtx, f.TypeCtx, *transformer, *f.FuncRegistry);
        UNIT_ASSERT_EXCEPTION_CONTAINS(root->ComputeTypes(ctx), yexception, "Cannot type annotate lambda");
        const auto issues = f.ExprCtx.IssueManager.GetIssues().ToString();
        UNIT_ASSERT_C(issues.Contains("Original YQL leaf diagnostic"), issues);
        UNIT_ASSERT_C(issues.Contains("While annotating RBO Map"), issues);
        UNIT_ASSERT_C(issues.Contains("%0[before]"), issues);
    }

    void CheckSubquerySharing(bool sharedCallNode, bool correlated) {
        TExprContext ctx;
        TTypeAnnotationContext typeCtx;
        const TPositionHandle pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        const auto* type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        constant->TailPtr()->SetTypeAnn(type);
        auto source = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {
            ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {empty, ctx.NewAtom(pos, "payload"), constant, ctx.NewAtom(pos, "False")})
        })});
        source->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("payload", type)})));
        auto outer = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        if (correlated) {
            outer = ctx.NewCallable(pos, "KqpOpMap", {outer, ctx.NewList(pos, {
                ctx.NewCallable(pos, "KqpOpMapElementLambda",
                    {outer, ctx.NewAtom(pos, "zouter"), constant, ctx.NewAtom(pos, "False")})
            })});
            auto typeNode = ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Uint64")});
            typeNode->SetTypeAnn(ctx.MakeType<TTypeExprType>(type));
            source = ctx.NewCallable(pos, "KqpInfuseDependents", {source,
                ctx.NewList(pos, {ctx.NewAtom(pos, "zouter")}), ctx.NewList(pos, {typeNode})});
            source->SetTypeAnn(ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
                TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("payload", type),
                    ctx.MakeType<TItemExprType>("zouter", type)})));
        }
        auto sharedCall = ctx.NewCallable(pos, "KqpExprSublink", {source});
        TExprNode::TListType elements;
        for (const TString name : {"first", "second"}) {
            auto call = sharedCallNode ? sharedCall : ctx.NewCallable(pos, "KqpExprSublink", {source});
            call->SetTypeAnn(type);
            auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {ctx.NewArgument(pos, "row")}), std::move(call));
            elements.push_back(ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {outer, ctx.NewAtom(pos, name), std::move(lambda), ctx.NewAtom(pos, "False")}));
        }
        PlanConverter converter(typeCtx, ctx);
        auto plan = converter.ExprNodeToOperator(ctx.NewCallable(pos, "KqpOpMap", {outer, ctx.NewList(pos, std::move(elements))}));
        const TReplicate* shared = nullptr;
        TUnorderedIUs captures;
        size_t calls = 0;
        for (const auto& [id, entry] : converter.PlanProps.Subplans) {
            IOperator* producer = entry.Plan.get();
            if (correlated) {
                for (const auto& item : IterateSubtree(producer)) {
                    if (item.Current->Kind == EOperator::AddDependencies) {
                        auto& dependencies = CastOperator<TOpAddDependencies>(*item.Current);
                        UNIT_ASSERT(!captures.HasAny(dependencies.GetDependencies().Keys()));
                        captures.UnionWith(dependencies.GetDependencies().Keys());
                        producer = dependencies.GetInput().Get();
                        break;
                    }
                }
            }
            auto& port = CastOperator<TOpReplicate>(*producer);
            if (shared) {
                UNIT_ASSERT(shared == &port.GetReplicate());
                UNIT_ASSERT(!port.IsPrimary());
            } else {
                shared = &port.GetReplicate();
            }
            UNIT_ASSERT(entry.Plan->GetOutputIUs().Contains(*entry.ResultIU));
            ++calls;
        }
        UNIT_ASSERT_VALUES_EQUAL(calls, 2);
        UNIT_ASSERT_VALUES_EQUAL(captures.Size(), correlated ? 2 : 0);
    }

    Y_UNIT_TEST(CaptureFreeSubtreesShareTheirProducerAcrossCallSites) {
        for (const bool sharedCallNode : {false, true}) {
            for (const bool correlated : {false, true}) {
                CheckSubquerySharing(sharedCallNode, correlated);
            }
        }
    }

    Y_UNIT_TEST(AnnotationReferencesDoNotCreateReplicateConsumers) {
        TExprContext ctx;
        TTypeAnnotationContext typeCtx;
        const TPositionHandle pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        constant->TailPtr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        auto element = ctx.NewCallable(pos, "KqpOpMapElementLambda",
            {empty, ctx.NewAtom(pos, "payload"), constant, ctx.NewAtom(pos, "False")});
        auto map = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {element})});
        auto row = ctx.NewArgument(pos, "row");
        auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "payload")});
        auto sortKey = ctx.NewCallable(pos, "KqpOpSortElement", {map, ctx.NewAtom(pos, "asc"),
            ctx.NewAtom(pos, "first"), ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(member))});
        auto sort = ctx.NewCallable(pos, "KqpOpSort", {map, ctx.NewList(pos, {sortKey})});
        PlanConverter converter(typeCtx, ctx);
        auto plan = converter.ExprNodeToOperator(sort);
        UNIT_ASSERT(plan->Kind == EOperator::Sort);
        UNIT_ASSERT(plan->GetChild(0)->Kind == EOperator::Map);
        UNIT_ASSERT(plan->GetChild(0)->GetChild(0)->Kind == EOperator::EmptySource);
    }

    Y_UNIT_TEST(RepeatedPlanInputCreatesDisjointReplicatePorts) {
        TExprContext ctx;
        TTypeAnnotationContext typeCtx;
        const TPositionHandle pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        const auto* type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
        constant->TailPtr()->SetTypeAnn(type);
        auto map = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {
            ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {empty, ctx.NewAtom(pos, "payload"), constant, ctx.NewAtom(pos, "false")})
        })});
        auto setOp = ctx.NewCallable(pos, "KqpOpSetOp", {map, map, ctx.NewAtom(pos, "union_all")});
        const auto* row = ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>("payload", type)});
        map->SetTypeAnn(ctx.MakeType<TListExprType>(row));
        setOp->SetTypeAnn(map->GetTypeAnn());
        PlanConverter converter(typeCtx, ctx);
        auto plan = converter.ExprNodeToOperator(setOp);
        auto& merge = CastOperator<TOpUnionAll>(*plan);
        auto& left = CastOperator<TOpReplicate>((*merge.GetChild(0)));
        auto& right = CastOperator<TOpReplicate>((*merge.GetChild(1)));
        UNIT_ASSERT(&left.GetReplicate() == &right.GetReplicate());
        UNIT_ASSERT(left.IsPrimary());
        UNIT_ASSERT(!right.IsPrimary());
        UNIT_ASSERT(!left.GetOutputIUs().HasAny(right.GetOutputIUs()));
        const auto output = *merge.GetColumns().Keys().begin();
        const auto& inputs = merge.GetColumns().Find(output)->Inputs;
        UNIT_ASSERT(left.GetOutputIUs().Contains(inputs[0]));
        UNIT_ASSERT(right.GetOutputIUs().Contains(inputs[1]));
        UNIT_ASSERT(output != inputs[0] && output != inputs[1]);
        UNIT_ASSERT(left.GetReplicate().GetInput()->GetChild(0)->Kind == EOperator::EmptySource);
    }

    Y_UNIT_TEST(PositionalRenamePreservesProjectionAcrossSharedInputs) {
        for (const TString kind : {"union_all", "union"}) {
            TExprContext ctx;
            TTypeAnnotationContext typeCtx;
            const TPositionHandle pos;
            const auto* type = ctx.MakeType<TDataExprType>(EDataSlot::Uint64);
            const auto rowType = [&](const TString& prefix) {
                return ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
                    TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>(prefix + "a", type),
                        ctx.MakeType<TItemExprType>(prefix + "z", type)}));
            };
            auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
            TExprNode::TListType elements;
            for (const TString name : {"z", "a"}) {
                auto constant = MakeConstant("Uint64", name == "z" ? "1" : "2", pos, &ctx).Node;
                constant->TailPtr()->SetTypeAnn(type);
                elements.push_back(ctx.NewCallable(pos, "KqpOpMapElementLambda",
                    {empty, ctx.NewAtom(pos, name), constant, ctx.NewAtom(pos, "false")}));
            }
            auto map = ctx.NewCallable(pos, "KqpOpMap",
                {empty, ctx.NewList(pos, std::move(elements)), ctx.NewAtom(pos, "true")});
            map->SetTypeAnn(rowType(""));
            auto alias = ctx.NewCallable(pos, "KqpOpReplaceAlias", {map, ctx.NewAtom(pos, "s")});
            alias->SetTypeAnn(rowType("s."));
            const auto rename = [&]() {
                auto result = ctx.NewCallable(pos, "KqpOpReplaceColumns", {alias,
                    ctx.NewList(pos, {ctx.NewAtom(pos, "z"), ctx.NewAtom(pos, "a")})});
                result->SetTypeAnn(rowType(""));
                return result;
            };
            auto setOp = ctx.NewCallable(pos, "KqpOpSetOp", {rename(), rename(), ctx.NewAtom(pos, kind)});
            setOp->SetTypeAnn(rowType(""));
            auto root = ctx.NewCallable(pos, "KqpOpReplaceColumns", {setOp,
                ctx.NewList(pos, {ctx.NewAtom(pos, "first"), ctx.NewAtom(pos, "second")})});

            PlanConverter converter(typeCtx, ctx);
            auto plan = converter.ExprNodeToOperator(root);
            const auto& registry = converter.PlanProps.InfoUnitRegistry;
            const auto checkRename = [&](const TOpMap& op, const TString& prefix, bool outer) {
                for (const auto& [id, element] : op.GetMapElements().Items()) {
                    const auto input = element.GetColumnAccess();
                    const auto& name = registry.Get(id).GetColumnName();
                    const auto expected = outer ? (name == "first" ? "z" : "a") : name;
                    UNIT_ASSERT_VALUES_EQUAL(registry.Get(input).GetFullName(), prefix + expected);
                    UNIT_ASSERT(op.GetChild(0)->GetOutputIUs().Contains(input));
                }
            };
            checkRename(CastOperator<TOpMap>(*plan), "", true);
            auto* merge = plan->GetChild(0).Get();
            if (kind == "union") {
                merge = merge->GetChild(0).Get();
            }
            UNIT_ASSERT(merge->Kind == EOperator::UnionAll);
            for (ui32 i = 0; i < 2; ++i) {
                checkRename(CastOperator<TOpMap>(*merge->GetChild(i)), "s.", false);
            }
            UNIT_ASSERT(!merge->GetChild(0)->GetChild(0)->GetOutputIUs().HasAny(
                merge->GetChild(1)->GetChild(0)->GetOutputIUs()));
        }
    }

    Y_UNIT_TEST(RootKeepsOptionalQueryHintsSeparateFromOutputNames) {
        TExprContext ctx;
        TTypeAnnotationContext typeCtx;
        const TPositionHandle pos;
        auto empty = ctx.NewCallable(pos, "KqpOpEmptySource", {});
        auto constant = MakeConstant("Uint64", "1", pos, &ctx).Node;
        constant->TailPtr()->SetTypeAnn(ctx.MakeType<TDataExprType>(EDataSlot::Uint64));
        auto map = ctx.NewCallable(pos, "KqpOpMap", {empty, ctx.NewList(pos, {
            ctx.NewCallable(pos, "KqpOpMapElementLambda",
                {empty, ctx.NewAtom(pos, "payload"), constant, ctx.NewAtom(pos, "false")})
        })});
        auto root = ctx.NewCallable(pos, "KqpOpRoot", {map, ctx.NewList(pos, {ctx.NewAtom(pos, "payload")})});
        for (const bool withHints : {false, true}) {
            PlanConverter converter(typeCtx, ctx);
            auto hints = ctx.NewList(pos, withHints
                ? TExprNode::TListType{ctx.NewAtom(pos, "display")} : TExprNode::TListType{});
            auto plan = converter.ConvertRoot(root, hints);
            UNIT_ASSERT_VALUES_EQUAL(plan->GetColumns().Items().front().second, "payload");
            UNIT_ASSERT_VALUES_EQUAL(plan->GetQueryColumns().size(), withHints ? 1 : 0);
            if (withHints) {
                UNIT_ASSERT_VALUES_EQUAL(plan->GetQueryColumns().front(), "display");
            }
        }
    }

    Y_UNIT_TEST(WideExpressionPreservesNestedFieldsAndLambdaArguments) {
        TExprContext ctx;
        const TPositionHandle pos;
        auto row = ctx.NewArgument(pos, "row");
        auto local = ctx.NewArgument(pos, "local");
        auto field = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "7")});
        auto nestedField = ctx.NewCallable(pos, "Member", {field, ctx.NewAtom(pos, "user_field")});
        auto localField = ctx.NewCallable(pos, "Member", {local, ctx.NewAtom(pos, "7")});
        auto nestedBody = ctx.NewList(pos, {field, localField});
        auto nestedLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {local}), std::move(nestedBody));
        auto body = ctx.NewList(pos, {nestedField, nestedLambda});
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
        auto wide = ctx.NewArgument(pos, "wide");

        const auto lowered = NPhysicalConvertionUtils::LowerRowLambdaBody(lambda, {{7, wide}}, ctx);
        UNIT_ASSERT(lowered->Child(0)->HeadPtr() == wide);
        UNIT_ASSERT_VALUES_EQUAL(lowered->Child(0)->Tail().Content(), "user_field");
        UNIT_ASSERT(lowered->Child(1)->Tail().ChildPtr(0) == wide);
        const auto& inner = *lowered->Child(1);
        const auto& untouched = *inner.Tail().Child(1);
        UNIT_ASSERT(&untouched.Head() == &inner.Head().Head());
        UNIT_ASSERT_VALUES_EQUAL(untouched.Tail().Content(), "7");
    }

    Y_UNIT_TEST(JoinFilterSkipsPhysicalKeyTemporaries) {
        TExprContext ctx;
        const TPositionHandle pos;
        TPlanProps props;
        auto left = MakeColumnAccess(7, pos, &ctx, &props);
        auto right = MakeColumnAccess(8, pos, &ctx, &props);
        auto filter = MakeConjunction({left, right});
        // Slots 1 and 3 are cast keys, not logical bindings.
        const auto lowered = NPhysicalConvertionUtils::ConvertToWideJoinFilter(
            filter.Node, {{7, 0}, {8, 2}}, TUnorderedIUs{8}, 4, ctx);
        UNIT_ASSERT_VALUES_EQUAL(lowered->Head().ChildrenSize(), 4);
        const auto& conjunction = lowered->Tail().Head(); // Coalesce(And(...), false).
        UNIT_ASSERT(conjunction.IsCallable("And"));
        UNIT_ASSERT(&conjunction.Head() == lowered->Head().Child(0));
        UNIT_ASSERT(conjunction.Tail().IsCallable("Unwrap"));
        UNIT_ASSERT(&conjunction.Tail().Head() == lowered->Head().Child(2));
    }

    Y_UNIT_TEST(OlapExportRenamesBindingsNotLiteralsOrNestedFields) {
        TExprContext ctx;
        const TPositionHandle pos;
        auto atom = [&](TStringBuf value) { return ctx.NewAtom(pos, value); };
        auto nested = ctx.NewCallable(pos, "StructType", {
            ctx.NewList(pos, {atom("0"), ctx.NewCallable(pos, "DataType", {atom("Utf8")})})});
        auto rowType = ctx.NewCallable(pos, "StructType", {ctx.NewList(pos, {atom("0"), nested})});
        auto column = ctx.NewCallable(pos, "KqpOlapApplyColumnArg", {rowType, atom("0")});
        auto value = ctx.NewArgument(pos, "value");
        auto kernel = ctx.NewLambda(pos, ctx.NewArguments(pos, {value}),
            ctx.NewCallable(pos, "Member", {value, atom("0")}));
        auto apply = ctx.NewCallable(pos, "KqpOlapApply", {kernel, ctx.NewList(pos, {column}), atom("0")});
        auto projection = ctx.NewCallable(pos, "KqpOlapProjection", {apply, atom("0")});
        auto row = ctx.NewArgument(pos, "row");
        auto literal = ctx.NewCallable(pos, "Utf8", {atom("0")});
        auto condition = ctx.NewList(pos, {atom("eq"), atom("0"), literal});
        auto filter = ctx.NewCallable(pos, "KqpOlapFilter", {row, condition});
        auto body = ctx.NewCallable(pos, "KqpOlapProjections", {filter, ctx.NewList(pos, {projection})});
        auto lambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(body));
        auto exported = NOpt::TOlapFilterInspector::RenameColumns(lambda, {{"0", "payload"}}, ctx);

        const auto& renamedCondition = exported->Tail().Head().Tail();
        UNIT_ASSERT_VALUES_EQUAL(renamedCondition.Child(1)->Content(), "payload");
        UNIT_ASSERT(renamedCondition.ChildPtr(2) == literal);
        const auto& renamedProjection = exported->Tail().Tail().Head();
        UNIT_ASSERT_VALUES_EQUAL(renamedProjection.Tail().Content(), "payload");
        const auto& renamedApply = renamedProjection.Head();
        UNIT_ASSERT(renamedApply.HeadPtr() == kernel);
        UNIT_ASSERT_VALUES_EQUAL(renamedApply.Tail().Content(), "0"); // Kernel name.
        const auto& renamedColumn = renamedApply.Child(1)->Head();
        UNIT_ASSERT_VALUES_EQUAL(renamedColumn.Tail().Content(), "payload");
        UNIT_ASSERT_VALUES_EQUAL(renamedColumn.Head().Head().Head().Content(), "payload");
        UNIT_ASSERT(renamedColumn.Head().Head().TailPtr() == nested);
        UNIT_ASSERT_VALUES_EQUAL(projection->Tail().Content(), "0"); // Original IR untouched.
    }

    Y_UNIT_TEST(UnionAllPreservesBindingsOrderAndEmptyRows) {
        NTests::TIdTestContext f;
        const auto a = f.Id(), b = f.Id(), first = f.Id(), second = f.Id();
        const TPhysicalNames names(f.Props.InfoUnitRegistry);
        for (const bool ordered : {false, true}) {
            for (const bool empty : {false, true}) {
                auto merge = f.Union(f.Read({a}), f.Read({b}),
                    {{first, a, b}, {second, a, b}}, ordered);
                const auto outputs = empty ? TUnorderedIUs{} : TUnorderedIUs{first, second};
                merge->PruneOutputs(outputs, f.ExprCtx);
                merge->Props.Analysis.LiveOut = outputs;
                merge->Props.Analysis.LiveInByChild = empty
                    ? TVector<TUnorderedIUs>(2) : TVector<TUnorderedIUs>{{a}, {b}};
                const auto ast = TPhysicalUnionAllBuilder(*merge, f.ExprCtx, f.Pos, names).BuildPhysicalOp(
                    {f.ExprCtx.NewArgument(f.Pos, "left"), f.ExprCtx.NewArgument(f.Pos, "right")});
                UNIT_ASSERT(ast->IsCallable(ordered ? "OrderedExtend" : "Extend"));
                UNIT_ASSERT_VALUES_EQUAL(ast->ChildrenSize(), 2);
                for (ui32 child = 0; child < 2; ++child) {
                    const auto& map = ast->Child(child);
                    UNIT_ASSERT(map->IsCallable(ordered ? "OrderedMap" : "Map"));
                    const auto& fields = map->Tail().Tail();
                    UNIT_ASSERT(fields.IsCallable("AsStruct"));
                    UNIT_ASSERT_VALUES_EQUAL(fields.ChildrenSize(), empty ? 0 : 2);
                    if (!empty) {
                        for (ui32 index = 0; index < 2; ++index) {
                            const auto& field = *fields.Child(index);
                            UNIT_ASSERT_VALUES_EQUAL(field.Head().Content(), names.Get(index ? second : first));
                            UNIT_ASSERT_VALUES_EQUAL(field.Tail().Tail().Content(), names.Get(child ? b : a));
                        }
                    }
                }
            }
        }
    }

    Y_UNIT_TEST(ReadSeparatesStorageNamesFromPhysicalIds) {
        TExprContext ctx;
        const TPositionHandle pos;
        TInfoUnitRegistry registry;
        const auto z = registry.Add(TInfoUnit("t", "z"));
        const auto a = registry.Add(TInfoUnit("t", "a"));
        const auto anotherZ = registry.Add(TInfoUnit("other", "z"));
        const TPhysicalNames names(registry);
        const auto table = Build<TKqpTable>(ctx, pos)
            .Path().Build("/Root/table").PathId().Build("1:1")
            .SysView().Build("").Version().Build("1").Done().Ptr();
        for (const auto storage : {NYql::EStorageType::RowStorage, NYql::EStorageType::ColumnStorage}) {
            TOpRead read("t", {z, a, anotherZ}, storage, table, nullptr, nullptr,
                std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
            if (storage == NYql::EStorageType::ColumnStorage) {
                auto row = ctx.NewArgument(pos, "row");
                row->SetTypeAnn(ctx.MakeType<TFlowExprType>(ctx.MakeType<TStructExprType>(
                    TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>(
                        ctx.GetIndexAsString(z), ctx.MakeType<TDataExprType>(EDataSlot::Uint64))})));
                auto condition = ctx.NewList(pos, {ctx.NewAtom(pos, z)});
                auto filter = ctx.NewCallable(pos, "KqpOlapFilter", {row, condition});
                read.OlapFilterLambda = ctx.NewLambda(pos, ctx.NewArguments(pos, {row}), std::move(filter));
            }
            const auto result = TPhysicalSourceBuilder(read, ctx, pos, names, registry, "stage").BuildPhysicalOp();
            TExprNode::TPtr storageRead;
            VisitExpr(result, [&](const TExprNode::TPtr& node) {
                if (TKqpReadRangesSourceSettings::Match(node.Get()) || TKqpBlockReadOlapTableRanges::Match(node.Get())) {
                    storageRead = node;
                }
                return true;
            });
            UNIT_ASSERT(storageRead);
            if (storage == NYql::EStorageType::ColumnStorage) {
                const auto process = TKqpBlockReadOlapTableRanges(storageRead).Process();
                UNIT_ASSERT(!process.Args().Arg(0).Ref().GetTypeAnn());
                UNIT_ASSERT(read.OlapFilterLambda->Head().Head().GetTypeAnn());
                UNIT_ASSERT_VALUES_EQUAL(process.Body().Ref().Tail().Head().Content(), "z");
            }
            const auto columns = storage == NYql::EStorageType::RowStorage
                ? TKqpReadRangesSourceSettings(storageRead).Columns()
                : TKqpBlockReadOlapTableRanges(storageRead).Columns();
            UNIT_ASSERT_VALUES_EQUAL(columns.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(columns.Item(0).Value(), "a");
            UNIT_ASSERT_VALUES_EQUAL(columns.Item(1).Value(), "z");

            const auto map = storage == NYql::EStorageType::RowStorage
                ? TDqPhyStage(result).Program().Body().Ptr() : result->HeadPtr();
            const auto& lambda = map->Tail();
            const auto& fields = lambda.Tail();
            UNIT_ASSERT_VALUES_EQUAL(fields.ChildrenSize(), 3);
            UNIT_ASSERT_VALUES_EQUAL(fields.Child(0)->Head().Content(), names.Get(z));
            UNIT_ASSERT_VALUES_EQUAL(fields.Child(1)->Head().Content(), names.Get(a));
            UNIT_ASSERT_VALUES_EQUAL(fields.Child(2)->Head().Content(), names.Get(anotherZ));
            if (storage == NYql::EStorageType::ColumnStorage) {
                UNIT_ASSERT(&fields.Child(0)->Tail() == lambda.Head().Child(1));
                UNIT_ASSERT(&fields.Child(1)->Tail() == lambda.Head().Child(0));
                UNIT_ASSERT(&fields.Child(2)->Tail() == lambda.Head().Child(1));
            }
        }
    }

    Y_UNIT_TEST(EmptyOlapReadUsesOnlyAPhysicalCarrier) {
        NTests::TIdTestContext f;
        const auto table = f.Read({})->TableCallable;
        TOpRead read("t", {}, NYql::EStorageType::ColumnStorage, table, nullptr, nullptr,
            std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, f.Pos);
        const TPhysicalNames names(f.Props.InfoUnitRegistry);
        const auto ast = TPhysicalSourceBuilder(read, f.ExprCtx, f.Pos, names,
            f.Props.InfoUnitRegistry, "stage", "key").BuildPhysicalOp();
        const auto source = FindNode(ast, [](const auto& node) {
            return TKqpBlockReadOlapTableRanges::Match(node.Get());
        });
        const auto columns = TKqpBlockReadOlapTableRanges(source).Columns();
        UNIT_ASSERT_VALUES_EQUAL(columns.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(columns.Item(0).Value(), "key");
        const auto& narrow = ast->Head();
        UNIT_ASSERT(narrow.IsCallable("NarrowMap"));
        UNIT_ASSERT_VALUES_EQUAL(narrow.Tail().Head().ChildrenSize(), 1);
        UNIT_ASSERT(narrow.Tail().Tail().IsCallable("AsStruct"));
        UNIT_ASSERT_VALUES_EQUAL(narrow.Tail().Tail().ChildrenSize(), 0);
        UNIT_ASSERT(read.GetOutputIUs().Empty());
    }

    Y_UNIT_TEST(AggregationCollapsesRepeatedKeysButPreservesDistinctOutputBindings) {
        for (const bool distinct : {false, true}) {
            for (const bool prune : {false, true}) {
                NTests::TIdTestContext f;
                const auto a = f.Id(), b = f.Id(), x = f.Id(), y = f.Id(), z = f.Id();
                TAggregationIUs traits;
                TOrderedIUs<TString> outputs{{a, "a"}, {b, "b"}};
                if (distinct) {
                    traits.Add(x, TOpAggregationTraits{a, "distinct"});
                    traits.Add(y, TOpAggregationTraits{a, "distinct"});
                    traits.Add(z, TOpAggregationTraits{b, "distinct"});
                    outputs = {{x, "x"}, {y, "y"}, {z, "z"}};
                }
                auto aggregate = MakeIntrusive<TOpAggregate>(f.Read({a, b}), std::move(traits),
                    TOrderedIUs<>{a, a, b}, EOpPhase::Intermediate, distinct, f.Pos);
                f.SetType(*aggregate->GetInput());
                f.SetType(*aggregate);
                auto* op = aggregate.get();
                auto root = f.Root(std::move(aggregate), std::move(outputs));
                ComputePlanLiveness(*root);
                const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
                const auto ast = TPhysicalAggregationBuilder(*op, f.ExprCtx, f.Pos, names, prune)
                    .BuildPhysicalOp(f.ExprCtx.NewArgument(f.Pos, "input"), std::nullopt);
                const auto combiner = FindNode(ast, [](const auto& node) {
                    return node->IsCallable("DqPhyHashCombine");
                });
                UNIT_ASSERT(combiner);
                UNIT_ASSERT_VALUES_EQUAL(combiner->Child(2)->ChildrenSize(), 3); // Args + two unique keys.
                const auto& finish = *combiner->Child(5);
                UNIT_ASSERT_VALUES_EQUAL(finish.ChildrenSize(), distinct ? 4 : 3);
                if (distinct) {
                    UNIT_ASSERT(finish.Child(1) == finish.Child(2));
                    UNIT_ASSERT(finish.Child(1) != finish.Child(3));
                }
            }
        }
    }

    Y_UNIT_TEST(ReplicateRebindsSparsePortsBeforeConnectionsAndLookupOnlyChangesItsBranch) {
        TExprContext ctx;
        const TPositionHandle pos;
        TInfoUnitRegistry registry;
        const auto sourceId = registry.Add(TInfoUnit("key"));
        TMapIUs columns;
        columns.Add(sourceId, MakeConstant("Uint64", "1", pos, &ctx));
        auto hub = TReplicate::Create(MakeIntrusive<TOpMap>(
            MakeIntrusive<TOpEmptySource>(pos), pos, std::move(columns)), pos, registry);
        auto removed = hub->AddOutput();
        auto first = hub->AddOutput();
        auto second = hub->AddOutput();
        removed.Reset(); // Logical ordinals are now 1 and 2, physical outputs 0 and 1.
        const auto firstId = *first->GetOutputIUs().begin();
        const auto secondId = *second->GetOutputIUs().begin();
        first->Props.StageOutputIndex = 0;
        second->Props.StageOutputIndex = 1;
        first->Props.Analysis.LiveOut = TUnorderedIUs{firstId};
        second->Props.Analysis.LiveOut = TUnorderedIUs{secondId};
        // Visit the secondary ordinal first; lowering still orders by the
        // assigned physical indices, not traversal order or reference counts.
        TOpRoot root(MakeIntrusive<TOpJoin>(second, first, pos, "Cross", TJoinIUs{}), pos, {});
        root.ComputeParents();
        const TPhysicalNames names(registry);
        const auto stream = ctx.NewArgument(pos, "input");
        const auto fanout = NPhysicalConvertionUtils::BuildSwitch(stream, *hub, names, ctx);
        UNIT_ASSERT(fanout->IsCallable("Switch"));
        UNIT_ASSERT_VALUES_EQUAL(fanout->ChildrenSize(), 6);
        for (ui32 index = 0; index < 2; ++index) {
            const auto& fields = fanout->Child(3 + 2 * index)->Tail().Tail().Tail();
            UNIT_ASSERT_VALUES_EQUAL(fields.Head().Head().Content(), names.Get(index ? secondId : firstId));
            UNIT_ASSERT_VALUES_EQUAL(fields.Head().Tail().Tail().Content(), names.Get(sourceId));
        }

        second->Type = ctx.MakeType<TListExprType>(ctx.MakeType<TStructExprType>(
            TVector<const TItemExprType*>{ctx.MakeType<TItemExprType>(ctx.GetIndexAsString(secondId),
                ctx.MakeType<TDataExprType>(EDataSlot::Uint64))}));
        TOpTableLookup lookup(std::move(second), pos, nullptr, {}, {{secondId, "storage_key"}});
        const auto keys = NLookupJoinBuilder::BuildLookupKeys(lookup, fanout, ctx, names);
        UNIT_ASSERT(keys.InputStage->HeadPtr() == stream);
        UNIT_ASSERT(keys.InputStage->ChildPtr(3) == fanout->ChildPtr(3));
        UNIT_ASSERT(keys.InputStage->ChildPtr(5) != fanout->ChildPtr(5));
        UNIT_ASSERT_VALUES_EQUAL(keys.InputStage->Child(5)->Tail().Tail().Tail().Head().Head().Content(), "storage_key");
    }

    Y_UNIT_TEST(StageAssignmentAndLivenessDistinguishReplicateEdgesToOneJoin) {
        NTests::TIdTestContext f;
        const auto a = f.Id("left"), b = f.Id("right");
        auto hub = TReplicate::Create(f.Read({a, b}), f.Pos, f.Props.InfoUnitRegistry);
        auto discarded = hub->AddOutput();
        auto left = hub->AddOutput(), right = hub->AddOutput();
        discarded.Reset();
        auto* leftPort = left.get();
        auto* rightPort = right.get();
        const auto leftKey = *left->GetRebindings().Find(a);
        const auto rightKey = *right->GetRebindings().Find(b);
        auto root = f.Root(MakeIntrusive<TOpJoin>(std::move(left), std::move(right), f.Pos, "Inner",
            TPairedIUs{{leftKey, rightKey}}), {});
        root->GetInput()->Props.JoinAlgo = EJoinAlgoType::GraceJoin;
        root->GetInput()->Props.UseBlockHashJoin = false;
        TAssignStagesStage().RunStage(*root, f.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(*leftPort->Props.StageOutputIndex, 0);
        UNIT_ASSERT_VALUES_EQUAL(*rightPort->Props.StageOutputIndex, 1);
        UNIT_ASSERT_VALUES_EQUAL(root->PlanProps.StageGraph.StageIds.size(), 2);
        ComputePlanLiveness(*root);
        UNIT_ASSERT(GetLiveOut(leftPort) == TUnorderedIUs{leftKey});
        UNIT_ASSERT(GetLiveOut(rightPort) == TUnorderedIUs{rightKey});
        UNIT_ASSERT(GetLiveOut(hub->GetInput().Get()) == (TUnorderedIUs{a, b}));

        for (const auto& item : *root) {
            item.Current->Props.Statistics.emplace();
            item.Current->Props.Cost = 0;
        }
        const auto& connections = root->PlanProps.StageGraph.GetConnections(
            *leftPort->Props.StageId, *root->GetInput()->Props.StageId);
        UNIT_ASSERT_VALUES_EQUAL(connections.size(), 2);
        for (const auto& connection : connections) {
            static_cast<TShuffleConnection&>(*connection).HashFuncType = NYql::NDq::EHashShuffleFuncType::HashV2;
        }
        ui64 nodeId = 0;
        ui32 operatorIndex = 0;
        THashMap<IOperator*, ui32> operatorIds;
        const auto json = root->GetExecutionJson(nodeId, operatorIndex, operatorIds);
        const auto& inputs = json["Plans"].GetArraySafe().front()["Plans"].GetArraySafe();
        UNIT_ASSERT_VALUES_EQUAL(inputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(inputs[0]["KeyColumns"].GetArraySafe().front().GetStringSafe(), root->PlanProps.InfoUnitRegistry.GetDisplayName(leftKey));
        UNIT_ASSERT_VALUES_EQUAL(inputs[1]["KeyColumns"].GetArraySafe().front().GetStringSafe(), root->PlanProps.InfoUnitRegistry.GetDisplayName(rightKey));
        UNIT_ASSERT(inputs[0]["PlanNodeId"].GetIntegerSafe() != inputs[1]["PlanNodeId"].GetIntegerSafe());
    }
}

} // namespace NKikimr::NKqp
