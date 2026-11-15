#include <ydb/core/kqp/opt/rbo/frontend_lowering.h>
#include <ydb/core/kqp/opt/physical/predicate_collector.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

Y_UNIT_TEST_SUITE(KqpNativeOlapPolicy) {
    Y_UNIT_TEST(NullableStringPredicatesKeepKernelNullSemantics) {
        using namespace NFrontend;
        for (const auto op : {EOpcode::Contains, EOpcode::StartsWith, EOpcode::EndsWith}) {
            TExpressionArena arena;
            const auto input = arena.Column(1, {ETypeKind::String, true});
            const auto pattern = arena.Literal({ETypeKind::String}, std::string{"a"});
            const auto match = arena.Binary(op, input, pattern);
            const auto negated = arena.Unary(EOpcode::Not, match);
            const auto yes = arena.Literal({ETypeKind::Bool}, true);
            const TExprId alternatives[]{match, yes};
            for (const auto predicate : {negated, arena.Coalesce(alternatives)}) {
                UNIT_ASSERT(CanPushNativeOlapPredicate(arena, predicate, false, false));
                NYql::TExprContext ctx;
                const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
                UNIT_ASSERT(NYql::FindNodes(lowered, [](const auto& node) {
                    return node->IsCallable("KqpOlapApply");
                }).empty());
                for (const auto& node : NYql::FindNodes(lowered, [](const auto& node) {
                    return node->IsCallable("KqpOlapNot");
                })) {
                    const auto& truth = node->Head();
                    UNIT_ASSERT(truth.IsList() && truth.Head().IsAtom("??"));
                    UNIT_ASSERT(truth.Child(2)->IsCallable("Bool"));
                    UNIT_ASSERT_VALUES_EQUAL(truth.Child(2)->Head().Content(), "true");
                }
            }
            // A value-sensitive parent observes NULL from NOT, so it retains
            // scalar evaluation instead of the filter-only truth adaptation.
            const TExprId scalarAlternatives[]{negated, yes};
            const auto scalar = arena.Coalesce(scalarAlternatives);
            UNIT_ASSERT(!CanLowerNativeOlapPredicate(arena, scalar));
            UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, scalar, true));
        }
    }

    Y_UNIT_TEST(MixedConjunctionKeepsScalarApplyLocalToItsColumns) {
        using namespace NFrontend;
        using O = EOpcode;
        using K = ETypeKind;
        TExpressionArena arena;
        const auto text = arena.Column(1, {K::String, true});
        const auto stringPredicate = arena.Binary(O::Equal, arena.Unary(O::Lower, text),
            arena.Literal({K::String}, std::string{"abc"}));
        const auto numericPredicate = arena.Binary(O::Greater, arena.Column(2, {K::Int32, true}),
            arena.Literal({K::Int32}, std::int64_t{0}));
        for (const bool reverse : {false, true}) {
            const auto predicate = arena.Binary(O::And, reverse ? numericPredicate : stringPredicate,
                reverse ? stringPredicate : numericPredicate);
            NYql::TExprContext ctx;
            const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
            const auto calls = NYql::FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); });
            UNIT_ASSERT_VALUES_EQUAL(calls.size(), 1);
            const auto columns = NYql::FindNodes(calls.front(), [](const auto& node) {
                return node->IsCallable("KqpOlapApplyColumnArg");
            });
            UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(columns.front()->Tail().Content(), "1");
            UNIT_ASSERT_VALUES_EQUAL(NYql::FindNodes(lowered, [](const auto& node) {
                return node->IsList() && node->ChildrenSize() && node->Head().IsAtom("gt");
            }).size(), 1);
        }
    }

    Y_UNIT_TEST(ColumnNullChecksMatchExistingCollector) {
        using namespace NFrontend;
        using namespace NYql;
        for (const auto name : {"Bool", "Int32", "Uint64", "Double", "String", "Utf8", "Uuid",
                "Decimal(12,2)", "Json", "JsonDocument", "Date", "Timestamp64"})
        for (const bool nullable : {false, true}) for (const bool exists : {false, true}) {
            TExpressionArena arena;
            const auto type = nullable ? ParseType(name).Optional() : ParseType(name);
            const auto predicate = arena.Unary(exists ? EOpcode::IsNotNull : EOpcode::IsNull, arena.Column(0, type));
            TExprContext ctx;
            const TPositionHandle pos;
            const auto* fieldType = ExportNativeType(type, ctx);
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                ctx.MakeType<TItemExprType>("x", fieldType)});
            const auto row = ctx.NewArgument(pos, "row"); row->SetTypeAnn(rowType);
            auto member = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, "x")});
            member->SetTypeAnn(fieldType);
            const auto* boolType = ctx.MakeType<TDataExprType>(NUdf::EDataSlot::Bool);
            auto legacy = ctx.NewCallable(pos, "Exists", {member}); legacy->SetTypeAnn(boolType);
            if (!exists) {
                legacy = ctx.NewCallable(pos, "Not", {legacy}); legacy->SetTypeAnn(boolType);
            }
            for (const bool apply : {false, true}) {
                const auto description = TStringBuilder() << type.ToString() << " exists=" << exists << " scalarApply=" << apply;
                NOpt::TOLAPPredicateNode collected;
                NOpt::CollectPredicates(NNodes::TExprBase(legacy), collected, row.Get(), rowType, {apply, false});
                UNIT_ASSERT_C(apply ? collected.CanBePushedApply : collected.CanBePushed, description);
                UNIT_ASSERT_C(CanPushNativeOlapPredicate(arena, predicate, apply), description);
                const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, pos);
                const auto& operation = lowered->Tail().Tail().Head();
                UNIT_ASSERT_C(operation.IsList(), description);
                UNIT_ASSERT_VALUES_EQUAL(operation.Head().Content(), exists ? "exists" : "empty");
                UNIT_ASSERT_VALUES_EQUAL(operation.Tail().Content(), "0");
                UNIT_ASSERT(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).empty());
            }
        }
    }

    Y_UNIT_TEST(ColumnNullChecksDoNotAdmitUnsupportedValueUses) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto document = arena.Column(0, {ETypeKind::JsonDocument, true});
        const auto check = arena.Unary(EOpcode::IsNotNull, document);
        const auto valueCheck = arena.Unary(EOpcode::IsNotNull, arena.JsonValue(document, "$.a"));
        const auto predicate = arena.Binary(EOpcode::And, check, valueCheck);
        for (const bool apply : {false, true}) {
            UNIT_ASSERT(CanPushNativeOlapPredicate(arena, check, apply));
            UNIT_ASSERT(!CanLowerNativeOlapPredicate(arena, valueCheck, apply));
            UNIT_ASSERT(!CanPushNativeOlapPredicate(arena, predicate, apply));
        }
    }

    Y_UNIT_TEST(ComparisonEligibilityMatchesExistingCollector) {
        using namespace NFrontend;
        using namespace NYql;
        const std::pair<EOpcode, TStringBuf> comparisons[]{
            {EOpcode::Equal, "=="}, {EOpcode::NotEqual, "!="}, {EOpcode::Less, "<"},
            {EOpcode::LessEqual, "<="}, {EOpcode::Greater, ">"}, {EOpcode::GreaterEqual, ">="},
        };
        TString mismatches;
        for (const auto name : {"Bool", "Int8", "Uint8", "Int16", "Uint16", "Int32", "Uint32",
                "Int64", "Uint64", "Float", "Double", "String", "Utf8", "Uuid", "Decimal(12,2)",
                "Date", "Datetime", "Timestamp", "Interval", "Date32", "Datetime64", "Timestamp64", "Interval64"})
        for (const bool nullable : {false, true}) for (const bool same : {false, true})
        for (const auto& [op, callable] : comparisons) {
            TExpressionArena arena;
            const auto type = nullable ? ParseType(name).Optional() : ParseType(name);
            const auto predicate = arena.Binary(op, arena.Column(0, type), arena.Column(same ? 0 : 1, type));
            // Construct the legacy scalar boundary independently: typed row
            // members and a comparison, without going through native emission.
            TExprContext ctx;
            const TPositionHandle pos;
            const auto* fieldType = ExportNativeType(type, ctx);
            const auto* rowType = ctx.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                ctx.MakeType<TItemExprType>("x", fieldType), ctx.MakeType<TItemExprType>("y", fieldType)});
            const auto row = ctx.NewArgument(pos, "row"); row->SetTypeAnn(rowType);
            const auto member = [&](TStringBuf field) {
                auto node = ctx.NewCallable(pos, "Member", {row, ctx.NewAtom(pos, field)});
                node->SetTypeAnn(fieldType);
                return node;
            };
            const auto legacy = ctx.NewCallable(pos, callable, {member("x"), member(same ? "x" : "y")});
            legacy->SetTypeAnn(ExportNativeType(arena[predicate].Type, ctx));
            for (const bool apply : {false, true}) {
                NOpt::TOLAPPredicateNode collected;
                NOpt::CollectPredicates(NNodes::TExprBase(legacy), collected, row.Get(), rowType, {apply, false});
                const bool expected = apply ? collected.CanBePushedApply : collected.CanBePushed;
                const bool actual = CanPushNativeOlapPredicate(arena, predicate, apply);
                UNIT_ASSERT_VALUES_EQUAL_C(CanLowerNativeOlapPredicate(arena, predicate, apply), actual,
                    type.ToString() << " " << callable << " scalarApply=" << apply);
                if (actual != expected) mismatches += TStringBuilder() << type.ToString() << " " << callable
                    << " same=" << same << " scalarApply=" << apply << " native=" << actual << " legacy=" << expected << '\n';
                if (actual && expected) {
                    const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
                    const bool scalar = !FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).empty();
                    UNIT_ASSERT_VALUES_EQUAL_C(scalar, !collected.CanBePushed, type.ToString() << " " << callable);
                }
            }
        }
        UNIT_ASSERT_C(mismatches.empty(), mismatches);
    }
}
} // namespace NKikimr::NKqp
