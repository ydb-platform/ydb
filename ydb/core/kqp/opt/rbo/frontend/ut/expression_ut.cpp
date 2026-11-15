#include <ydb/core/kqp/opt/rbo/frontend/expression.h>
#include <ydb/core/kqp/opt/rbo/frontend/predicate.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>
#include <cmath>
#include <limits>
#include <tuple>

using namespace NKikimr::NKqp::NFrontend;

namespace {

using K = ETypeKind;
using O = EOpcode;

TExprId Integer(TExpressionArena& arena, std::int64_t value) {
    return arena.Literal({K::Int64}, value);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpNativeExpressions) {
    Y_UNIT_TEST(NullComparisonFoldingRequiresTotalOperands) {
        for (const auto op : {O::Equal, O::NotEqual, O::Less, O::LessEqual, O::Greater, O::GreaterEqual}) {
            for (const bool nullable : {false, true}) for (const bool reverse : {false, true}) {
                TExpressionArena arena;
                const TType type{K::Int64, nullable};
                const auto input = arena.Column(0, type);
                const auto absent = arena.Literal(type.Optional(), {});
                const auto compare = [&](TExprId value) {
                    return reverse ? arena.Binary(op, absent, value) : arena.Binary(op, value, absent);
                };
                const auto root = compare(input);
                const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
                UNIT_ASSERT(arena[folded].Op == O::Literal);
                UNIT_ASSERT(arena[folded].Type == arena[root].Type);
                TEvaluationContext context;
                context.Column = [&](TColumnId) { return TValue{type, std::int64_t{7}}; };
                UNIT_ASSERT(Evaluate(arena, root, context).IsNull());
                UNIT_ASSERT(Evaluate(arena, folded, context).IsNull());
                const auto checked = compare(arena.Ensure(input, arena.Literal({K::Bool}, false), "comparison check"));
                const auto checkedFolded = arena.FoldConstants(std::span(&checked, 1)).front();
                UNIT_ASSERT(arena[checkedFolded].Op != O::Literal);
                UNIT_ASSERT_EXCEPTION(Evaluate(arena, checked, context), TFrontendError);
                UNIT_ASSERT_EXCEPTION(Evaluate(arena, checkedFolded, context), TFrontendError);
            }
        }
    }

    Y_UNIT_TEST(NullComparisonFoldingPreservesVolatileAndNullSafeEvaluation) {
        TExpressionArena arena;
        const auto random = arena.Call("RandomNumber", {});
        const auto absent = arena.Literal({K::Uint64, true}, {});
        std::size_t calls = 0;
        TEvaluationContext context;
        context.RandomNumber = [&] { ++calls; return std::uint64_t{7}; };
        for (const auto root : {arena.Binary(O::Equal, random, absent), arena.Binary(O::Equal, absent, random)}) {
            const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
            UNIT_ASSERT(arena[folded].Op != O::Literal);
            const auto before = calls;
            UNIT_ASSERT(Evaluate(arena, root, context).IsNull());
            UNIT_ASSERT(Evaluate(arena, folded, context).IsNull());
            UNIT_ASSERT_VALUES_EQUAL(calls, before + 2);
        }
        const auto column = arena.Column(0, {K::Uint64, true});
        const auto root = arena.Binary(O::IsNotDistinct, column, absent);
        const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
        for (const bool missing : {false, true}) {
            context.Column = [&](TColumnId) { return TValue{{K::Uint64, true}, missing ? TScalar{} : TScalar{std::uint64_t{7}}}; };
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, root, context).Data), missing);
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded, context).Data), missing);
        }
    }

    Y_UNIT_TEST(NullRemainsItsOwnNullableTypeThroughTupleAccess) {
        TExpressionArena arena;
        const auto null = arena.Null();
        const TType nullType{K::Null};
        UNIT_ASSERT(arena[null].Type == nullType);
        UNIT_ASSERT(nullType.Optional() == nullType);
        UNIT_ASSERT(CommonType(nullType, nullType) == nullType);
        const TType fields[]{TType{K::Int32}, nullType};
        const auto tupleType = arena.TupleType(fields);
        for (const bool optional : {false, true}) {
            const auto tuple = arena.Parameter("tuple", optional ? tupleType.Optional() : tupleType);
            const auto value = arena.TupleElement(tuple, 1);
            UNIT_ASSERT(arena[value].Type == nullType);
            UNIT_ASSERT(arena[arena.FoldConstants(std::span(&value, 1)).front()].Type == nullType);
        }
    }

    Y_UNIT_TEST(TupleTypeNamesPreserveDecimalParametersAndOptionality) {
        TExpressionArena arena;
        const auto type = arena.ParseTypeName("Tuple<Int64,Decimal(12,2),String?>?");
        UNIT_ASSERT_VALUES_EQUAL(type.OptionalDepth, 1);
        const auto fields = arena.TupleItems(type);
        UNIT_ASSERT_VALUES_EQUAL(fields.size(), 3);
        UNIT_ASSERT(fields[0] == TType{K::Int64});
        UNIT_ASSERT(fields[1] == (TType{K::Decimal, false, 12, 2}));
        UNIT_ASSERT(fields[2] == (TType{K::String, true}));
        for (const auto* invalid : {"Tuple<Int64,>", "Tuple<Decimal(12,2>", "Tuple<Int64),String>", "Tuple<List<Int64>"})
            UNIT_ASSERT_EXCEPTION(arena.ParseTypeName(invalid), TFrontendError);
        const TExprId items[]{arena.Literal({K::String}, std::string{"bad"})};
        const TType required[]{TType{K::Int32}};
        UNIT_ASSERT_EXCEPTION(arena.Cast(arena.Collection(items, false), arena.TupleType(required)), TUnsupportedFeature);
    }

    Y_UNIT_TEST(BitCastPreservesWrappingAndNullsThroughNativeTransforms) {
        for (const auto kind : {K::Int8, K::Int16, K::Int32, K::Int64, K::Uint8, K::Uint16, K::Uint32, K::Uint64}) {
            TExpressionArena arena;
            const auto column = arena.Column(0, {K::Int64, true});
            const auto root = arena.BitCast(column, {kind});
            const auto literal = arena.Literal({K::Int64, true}, std::int64_t{-1});
            const auto rewritten = arena.RewriteColumns(root, [&](auto, auto, auto, auto) { return literal; });
            const auto value = Evaluate(arena, rewritten);
            UNIT_ASSERT(value.Type == (TType{kind, true}));
            if (value.Type.IsSigned()) UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(value.Data), -1);
            else UNIT_ASSERT(std::get<std::uint64_t>(value.Data) > 0);
            const auto folded = arena.FoldConstants(std::span{&rewritten, 1}).front();
            UNIT_ASSERT(arena[folded].Op == O::Literal);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(value));
            TExpressionArena copy;
            const auto imported = copy.Import(arena, root, [&](auto, auto type, auto pos) {
                return copy.Literal(type, std::int64_t{-1}, pos);
            });
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copy, imported)), FormatValue(value));
            UNIT_ASSERT(Evaluate(arena, arena.BitCast(arena.Literal({K::Int64, true}, {}), {kind})).IsNull());
        }
        TExpressionArena arena;
        const auto one = arena.Literal({K::Int32}, std::int64_t{1});
        UNIT_ASSERT_EXCEPTION(arena.BitCast(one, {K::Uint32, true}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.BitCast(one, {K::Double}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.BitCast(arena.Literal({K::Double}, 1.0), {K::Int64}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.BitCast(arena.Null(), {K::Int64}), TFrontendError);
    }

    Y_UNIT_TEST(CheckedArithmeticFoldingAndRewritingPreserveFailures) {
        for (const auto op : {O::Add, O::Subtract, O::Multiply, O::Divide, O::Modulo}) {
            TExpressionArena arena;
            const auto x = arena.Column(0, {K::Int64}), y = arena.Column(1, {K::Uint64});
            const auto root = arena.CheckedBinary(op, x, y);
            const auto rewritten = arena.RewriteColumns(root, [&](auto, auto column, auto type, auto pos) {
                return arena.Literal(type, column ? TScalar{std::numeric_limits<std::uint64_t>::max()} : TScalar{std::int64_t{0}}, pos);
            });
            UNIT_ASSERT(Evaluate(arena, rewritten).IsNull());
            const auto folded = arena.FoldConstants(std::span{&rewritten, 1}).front();
            UNIT_ASSERT(arena[folded].Op == O::Literal);
            UNIT_ASSERT(Evaluate(arena, folded).IsNull());
            TExpressionArena copy;
            const auto imported = copy.Import(arena, rewritten, {});
            UNIT_ASSERT(copy[imported].Type == arena[root].Type);
            UNIT_ASSERT(Evaluate(copy, imported).IsNull());
        }
    }

    Y_UNIT_TEST(BitwiseWidthsShiftsAndNullsSurviveNativeTransforms) {
        for (const auto kind : {K::Int8, K::Int16, K::Int32, K::Int64, K::Uint8, K::Uint16, K::Uint32, K::Uint64}) {
            TExpressionArena arena;
            const TType input{kind, true};
            const auto column = arena.Column(0, input);
            const auto count = arena.Literal({K::Uint8}, std::uint64_t{1});
            for (const auto op : {O::ShiftLeft, O::ShiftRight}) {
                const auto root = arena.Binary(op, column, count);
                UNIT_ASSERT(arena[root].Type.IsIntegral() && !arena[root].Type.IsSigned());
                UNIT_ASSERT_VALUES_EQUAL(arena[root].Type.OptionalDepth, 1);
                const auto literal = arena.Literal(input, input.IsSigned() ? TScalar{std::int64_t{-1}} : TScalar{std::uint64_t{255}});
                const auto rewritten = arena.RewriteColumns(root, [&](auto, auto, auto, auto) { return literal; });
                const auto value = Evaluate(arena, rewritten);
                const auto folded = arena.FoldConstants(std::span{&rewritten, 1}).front();
                UNIT_ASSERT(arena[folded].Op == O::Literal);
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(value));
                TExpressionArena other;
                const auto imported = other.Import(arena, root, [&](auto, auto type, auto pos) {
                    return other.Literal(type, arena[literal].Value, pos);
                });
                UNIT_ASSERT(other[imported].Type == arena[root].Type);
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(other, imported)), FormatValue(value));
                const auto absent = arena.Binary(op, arena.Literal(input, {}), count);
                UNIT_ASSERT(Evaluate(arena, absent).IsNull());
                const auto overwide = arena.Binary(op, literal, arena.Literal({K::Uint8}, std::uint64_t{64}));
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena, overwide).Data), 0);
            }
            if (!input.IsSigned()) {
                const auto one = arena.Literal(input, std::uint64_t{1});
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena,
                    arena.Binary(O::ShiftLeft, one, count)).Data), 2);
                const auto three = arena.Literal({K::Uint64}, std::uint64_t{3});
                for (const auto& [op, expected] : {std::pair{O::BitAnd, 1u}, {O::BitOr, 3u}, {O::BitXor, 2u}}) {
                    const auto root = arena.Binary(op, one, three);
                    UNIT_ASSERT(arena[root].Type == (TType{K::Uint64, true}));
                    UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena, root).Data), expected);
                }
            }
        }
        TExpressionArena arena;
        const auto signedValue = arena.Literal({K::Int32}, std::int64_t{-1});
        const auto unsignedValue = arena.Literal({K::Uint32}, std::uint64_t{1});
        UNIT_ASSERT_EXCEPTION(arena.Unary(O::BitNot, signedValue), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Binary(O::BitAnd, signedValue, unsignedValue), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Binary(O::ShiftLeft, unsignedValue, arena.Parameter("n", {K::Uint32})), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Binary(O::ShiftLeft, unsignedValue, arena.Literal({K::Uint32}, std::uint64_t{256})), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Binary(O::ShiftLeft, unsignedValue, arena.Parameter("n", {K::Uint8, true})), TFrontendError);
    }

    Y_UNIT_TEST(NullSafeEqualityRetainsNullAndUnorderedSemantics) {
        for (const auto kind : {K::Float, K::Double, K::Decimal}) {
            TExpressionArena arena;
            const TType type = kind == K::Decimal ? TType{kind, true, 12, 2} : TType{kind, true};
            const TScalar nan = kind == K::Decimal ? TScalar{TDecimal{TDecimal::NaN()}} : TScalar{std::numeric_limits<double>::quiet_NaN()};
            const auto a = arena.Column(0, type), b = arena.Column(1, type);
            const auto root = arena.Binary(O::IsNotDistinct, a, b);
            UNIT_ASSERT(arena[root].Type == TType{K::Bool});
            for (const bool leftPresent : {false, true}) for (const bool rightPresent : {false, true}) {
                const auto bound = arena.RewriteColumns(root, [&](auto, auto column, auto inputType, auto) {
                    return arena.Literal(inputType, (column == 0 ? leftPresent : rightPresent) ? nan : TScalar{});
                });
                UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, bound).Data), leftPresent == rightPresent);
                const auto folded = arena.FoldConstants(std::span{&bound, 1}).front();
                UNIT_ASSERT(arena[folded].Op == O::Literal);
                UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded).Data), leftPresent == rightPresent);
                TExpressionArena other;
                const auto imported = other.Import(arena, bound, {});
                UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(other, imported).Data), leftPresent == rightPresent);
            }
        }
        TExpressionArena arena;
        const auto f = arena.Literal({K::Float, true}, std::numeric_limits<double>::quiet_NaN());
        const auto d = arena.Literal({K::Double, true}, std::numeric_limits<double>::quiet_NaN());
        UNIT_ASSERT(!std::get<bool>(Evaluate(arena, arena.Binary(O::IsNotDistinct, f, d)).Data));
        UNIT_ASSERT(std::get<bool>(Evaluate(arena, arena.Binary(O::IsNotDistinct, arena.Null(), arena.Null())).Data));
        UNIT_ASSERT(!std::get<bool>(Evaluate(arena, arena.Binary(O::IsNotDistinct, arena.Null(), d)).Data));
    }

    Y_UNIT_TEST(ListMetadataAndScalarExtremaSurviveRewriteAndImport) {
        TExpressionArena arena;
        const auto begin = arena.Column(0, {K::Int32});
        const std::array rangeArgs{begin, arena.Literal({K::Int32}, std::int64_t{5})};
        const auto range = arena.Call("ListFromRange", rangeArgs);
        const auto size = arena.Unary(O::ListLength, range);
        const auto present = arena.Unary(O::ListHasItems, range);
        const std::array extremaArgs{size, arena.Literal({K::Uint32}, std::uint64_t{1})};
        const auto maximum = arena.Call("MaxOf", extremaArgs);
        const auto minimum = arena.Call("MinOf", extremaArgs);
        const TExprId roots[]{size, present, maximum, minimum};
        const std::string_view expected[]{"3", "true", "3", "1"};
        for (std::size_t i = 0; i < std::size(roots); ++i) {
            const auto rewritten = arena.RewriteColumns(roots[i], [&](auto, auto, auto, auto) {
                return arena.Literal({K::Int32}, std::int64_t{2});
            });
            TExpressionArena copy;
            copy.ListType({K::Bool});
            const auto imported = copy.Import(arena, rewritten, [&](auto, auto, auto) -> TExprId {
                UNIT_FAIL("constant expression unexpectedly depends on a column");
                return 0;
            });
            const auto folded = copy.FoldConstants(std::span{&imported, 1}).front();
            UNIT_ASSERT(copy[folded].Op == O::Literal);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copy, folded)), expected[i]);
        }
        UNIT_ASSERT_EXCEPTION(arena.Call("MinOf", {}), TFrontendError);
        const std::array bad{arena.Null()};
        UNIT_ASSERT_EXCEPTION(arena.Call("ListHasItems", bad), TFrontendError);
        const std::array notList{Integer(arena, 1)};
        UNIT_ASSERT_EXCEPTION(arena.Call("ListLength", notList), TFrontendError);
        UNIT_ASSERT_VALUES_EQUAL(arena.Call("MinOf", notList), notList[0]);
        const std::array json{arena.Parameter("json", {K::Json})};
        UNIT_ASSERT_EXCEPTION(arena.Call("MinOf", json), TFrontendError);
        const std::array collection{range};
        UNIT_ASSERT_EXCEPTION(arena.Call("MaxOf", collection), TUnsupportedFeature);
    }

    Y_UNIT_TEST(ListOperationsPreserveShapesOnImportRewriteAndFold) {
        TExpressionArena arena;
        const auto from = arena.Column(3, {K::Int32});
        const std::array bounds{from, arena.Literal({K::Int32}, std::int64_t{5})};
        const auto range = arena.Call("ListFromRange", bounds);
        const auto strings = arena.Cast(range, arena.ListType({K::String}));
        const std::array args{strings, arena.Literal({K::String}, std::string{","})};
        const auto joined = arena.Call("ListConcat", args);
        TEvaluationContext context;
        context.Column = [](TColumnId id) { return TValue{{id == 3 ? K::Int32 : K::Int64}, std::int64_t{2}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, joined, context)), "2,3,4");
        const auto rewritten = arena.RewriteColumns(joined, [&](auto, auto, auto, auto) { return arena.Column(7, {K::Int64}); });
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, rewritten, context)), "2,3,4");
        TExpressionArena imported;
        imported.ListType({K::Bool}); // Deliberately different local shape IDs.
        const auto copied = imported.Import(arena, rewritten, [&](auto id, auto type, auto pos) { return imported.Column(id, type, pos); });
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(imported, copied, context)), "2,3,4");
        const auto constant = arena.RewriteColumns(joined, [&](auto, auto, auto, auto) { return arena.Literal({K::Int32}, std::int64_t{2}); });
        const auto folded = arena.FoldConstants(std::span(&constant, 1)).front();
        UNIT_ASSERT(arena[folded].Op == O::Literal);
        UNIT_ASSERT(arena[folded].Type == arena[joined].Type);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), "2,3,4");
    }

    Y_UNIT_TEST(ListRangesRespectIntegerEdgesAndMaterializationBudget) {
        TExpressionArena arena;
        const auto begin = Integer(arena, std::numeric_limits<std::int64_t>::max());
        const auto end = Integer(arena, std::numeric_limits<std::int64_t>::min());
        const std::array bounds{begin, end, end};
        const auto range = arena.Call("ListFromRange", bounds);
        const auto value = Evaluate(arena, range);
        const auto& items = std::get<TTuple>(value.Data)->Items;
        UNIT_ASSERT_VALUES_EQUAL(items.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(items[0]), "9223372036854775807");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(items[1]), "-1");
        const std::array hugeBounds{arena.Literal({K::Uint64}, std::uint64_t{0}),
            arena.Literal({K::Uint64}, std::numeric_limits<std::uint64_t>::max())};
        const auto huge = arena.Call("ListFromRange", hugeBounds);
        const auto folded = arena.FoldConstants(std::span(&huge, 1), 1024).front();
        UNIT_ASSERT(arena[folded].Op == O::ListFromRange);
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, huge), TResourceLimitError);
        const auto a = arena.Literal({K::String}, std::string{"a"});
        const std::array many{a, a, a, a, a, a, a, a};
        const auto list = arena.Collection(many, true);
        const std::array concatArgs{list, arena.Literal({K::String}, std::string(1000, '-'))};
        const auto concat = arena.Call("ListConcat", concatArgs);
        const auto limited = arena.FoldConstants(std::span(&concat, 1), 2048).front();
        UNIT_ASSERT(arena[limited].Op == O::ListConcat);
    }

    Y_UNIT_TEST(BooleanIdentitiesPreserveTypesNullsAndErrors) {
        for (const bool optional : {false, true}) for (const bool literalOptional : {false, true})
        for (const auto op : {O::And, O::Or}) for (const bool left : {false, true}) {
            TExpressionArena arena;
            const TType type{K::Bool, optional};
            const auto input = arena.Column(0, type);
            const auto literal = arena.Literal({K::Bool, literalOptional}, op == O::And);
            const auto root = arena.Binary(op, left ? literal : input, left ? input : literal);
            const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
            UNIT_ASSERT(arena[folded].Type == arena[root].Type);
            for (const TScalar& value : {TScalar{}, TScalar{false}, TScalar{true}}) {
                if (!optional && std::holds_alternative<std::monostate>(value)) continue;
                TEvaluationContext ctx;
                unsigned reads = 0;
                ctx.Column = [&](auto) { ++reads; return TValue{type, value}; };
                const auto expected = Evaluate(arena, root, ctx);
                UNIT_ASSERT_VALUES_EQUAL(reads, 1);
                reads = 0;
                const auto actual = Evaluate(arena, folded, ctx);
                UNIT_ASSERT(actual.Type == expected.Type);
                UNIT_ASSERT(actual.Data == expected.Data);
                UNIT_ASSERT_VALUES_EQUAL(reads, 1);
            }
            const auto checked = arena.Ensure(input, arena.Literal({K::Bool}, false), "must evaluate");
            // A right-hand constant must never suppress a checked left operand,
            // whether it is neutral or decides the Boolean result.
            for (const bool constant : {false, true}) {
                const auto failure = arena.Binary(op, checked, arena.Literal({K::Bool}, constant));
                const auto result = arena.FoldConstants(std::span(&failure, 1)).front();
                TEvaluationContext ctx; ctx.Column = [&](auto) { return TValue{type, true}; };
                UNIT_ASSERT_EXCEPTION(Evaluate(arena, result, ctx), TFrontendError);
            }
        }
        TExpressionArena arena;
        for (const bool optional : {false, true}) for (const auto op : {O::IsNull, O::IsNotNull}) {
            const auto root = arena.Unary(op, arena.Column(0, {K::Int32, optional}));
            const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
            UNIT_ASSERT_VALUES_EQUAL(arena[folded].Op == O::Literal, !optional);
            if (!optional) UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded).Data), op == O::IsNotNull);
        }
        const auto number = arena.Column(0, {K::Int32});
        const auto sum = arena.Binary(O::Add, number, arena.Literal({K::Int32}, std::int64_t{1}));
        const auto exists = arena.Unary(O::IsNotNull, sum);
        UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, arena.FoldConstants(std::span(&exists, 1)).front()).Data), true);
        const auto checked = arena.Ensure(number, arena.Literal({K::Bool}, false), "must evaluate");
        const auto checkedExists = arena.Unary(O::IsNotNull, checked);
        const auto kept = arena.FoldConstants(std::span(&checkedExists, 1)).front();
        TEvaluationContext context;
        context.Column = [](auto) { return TValue{{K::Int32}, std::int64_t{1}}; };
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, kept, context), TFrontendError);
        const auto random = arena.Call("RandomNumber", {});
        const auto randomPositive = arena.Binary(O::Greater, random, arena.Literal({K::Double}, 0.0));
        const auto positive = arena.Binary(O::Greater, number, arena.Literal({K::Int32}, std::int64_t{0}));
        for (const auto op : {O::And, O::Or}) {
            const auto decisive = arena.Literal({K::Bool}, op == O::Or);
            const auto pure = arena.Binary(op, positive, decisive);
            UNIT_ASSERT(arena[arena.FoldConstants(std::span(&pure, 1)).front()].Op == O::Literal);
            const auto observable = arena.Binary(op, randomPositive, decisive);
            const auto folded = arena.FoldConstants(std::span(&observable, 1)).front();
            unsigned calls = 0;
            context.RandomNumber = [&] { ++calls; return 0.5; };
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded, context).Data), op == O::Or);
            UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        }
    }

    Y_UNIT_TEST(FilterPreparationPreservesTruthAndScalarEvaluationOrder) {
        TExpressionArena arena;
        const auto p = arena.Parameter("p", {K::Bool, true});
        const auto q = arena.Parameter("q", {K::Bool, true});
        const auto r = arena.Parameter("r", {K::Bool, true});
        const auto yes = arena.Literal({K::Bool}, true), no = arena.Literal({K::Bool}, false);
        const auto missing = arena.Literal({K::Bool, true}, {});
        const auto otherwise = [&](TExprId value, TExprId fallback) {
            const std::array args{value, fallback}; return arena.Coalesce(args);
        };
        const auto checked = arena.Ensure(r, arena.Unary(O::IsNotNull, q), "checked");
        const auto unwrapped = arena.Unary(O::Unwrap, q);
        const auto random = arena.Call("RandomUuid", {});
        const auto randomMatch = arena.Binary(O::Equal, random, arena.Literal({K::Uuid}, std::string(16, 'x')));
        const std::vector roots{
            arena.Unary(O::OptionalWrap, unwrapped),
            arena.Unary(O::OptionalWrap, randomMatch),
            otherwise(p, no),
            arena.Binary(O::Or, otherwise(p, no), q),
            arena.Binary(O::And, otherwise(p, no), checked),
            arena.Binary(O::And, p, otherwise(q, no)),
            arena.If(p, missing, q),
            arena.If(p, q, no),
            arena.If(p, otherwise(q, no), otherwise(r, no)),
            arena.Unary(O::Not, otherwise(p, no)),
            arena.Binary(O::Equal, otherwise(p, no), q),
            otherwise(arena.If(p, missing, q), yes),
            arena.Binary(O::And, arena.If(p, missing, q), checked),
            arena.Binary(O::Or, arena.If(p, missing, q), checked),
            arena.If(p, missing, unwrapped),
            arena.If(p, missing, arena.Binary(O::And, randomMatch, otherwise(q, no))),
            arena.Binary(O::Or, randomMatch, arena.If(p, missing, randomMatch)),
        };
        const std::array<TScalar, 3> values{TScalar{}, TScalar{false}, TScalar{true}};
        for (const auto root : roots) {
            const auto prepared = PrepareFilterPredicate(arena, root);
            for (const auto& a : values) for (const auto& b : values) for (const auto& c : values) {
                std::vector<std::string> calls;
                TEvaluationContext context;
                context.Parameter = [&](std::string_view name) {
                    calls.emplace_back(name);
                    return TValue{{K::Bool, true}, name == "p" ? a : name == "q" ? b : c};
                };
                context.RandomUuid = [&] { calls.emplace_back("random"); return std::string(16, 'x'); };
                const auto run = [&](TExprId expression) {
                    calls.clear();
                    try {
                        const auto value = Evaluate(arena, expression, context);
                        return value.IsNull() || !std::get<bool>(value.Data) ? 0 : 1;
                    } catch (const TFrontendError&) { return -1; }
                };
                const auto expected = run(root);
                const auto expectedCalls = calls;
                UNIT_ASSERT_VALUES_EQUAL_C(run(prepared), expected, root);
                UNIT_ASSERT_C(calls == expectedCalls, root);
            }
        }
    }

    Y_UNIT_TEST(FilterPreparationKeepsLegacyMembershipDistinctFromConjunctions) {
        TExpressionArena arena;
        const auto key = arena.Column(1, {K::Uint64, true});
        const auto a = arena.Parameter("a", {K::Uint64}), b = arena.Parameter("b", {K::Uint64});
        const std::array items{a, b};
        const auto scalar = arena.InCollection(key, arena.Collection(items, true), false);
        const auto predicate = PrepareFilterPredicate(arena, scalar);
        UNIT_ASSERT(arena[scalar].Op == O::InResult);
        UNIT_ASSERT_VALUES_EQUAL(predicate, scalar);
        TEvaluationContext context;
        context.Column = [](TColumnId) { return TValue{{K::Uint64, true}, {}}; };
        context.Parameter = [](std::string_view) { UNIT_FAIL("NULL guard must skip membership items"); return TValue{}; };
        UNIT_ASSERT(Evaluate(arena, scalar, context).IsNull());
        UNIT_ASSERT(Evaluate(arena, predicate, context).IsNull());
    }

    Y_UNIT_TEST(FilterTruthDropsRejectedOrOperandsWithoutSkippingEvaluation) {
        TExpressionArena arena;
        const auto value = arena.Parameter("value", {K::Bool, true});
        const auto check = arena.Parameter("check", {K::Bool});
        const auto observed = arena.Ensure(value, check, "observed check");
        const auto no = arena.Literal({K::Bool}, false);
        const auto absent = arena.Literal({K::Bool, true}, {});
        std::vector<std::string> calls;
        TEvaluationContext context;
        for (const auto& supplied : std::vector<TScalar>{false, true, std::monostate{}})
        for (const bool pass : {false, true}) {
            context.Parameter = [&](std::string_view name) -> TValue {
                calls.emplace_back(name);
                return name == "value" ? TValue{{K::Bool, true}, supplied} : TValue{{K::Bool}, pass};
            };
            const auto run = [&](TExprId id) {
                calls.clear();
                try {
                    const auto result = Evaluate(arena, id, context);
                    return std::holds_alternative<bool>(result.Data) && std::get<bool>(result.Data) ? std::string{"true"} : std::string{"rejected"};
                } catch (const TFrontendError& error) { return std::string{error.what()}; }
            };
            for (const auto rejected : {no, absent}) for (const bool reverse : {false, true}) {
                const auto root = reverse ? arena.Binary(O::Or, rejected, observed) : arena.Binary(O::Or, observed, rejected);
                const auto prepared = PrepareFilterPredicate(arena, root);
                UNIT_ASSERT_VALUES_EQUAL(prepared, observed);
                const auto expected = run(root);
                const auto expectedCalls = calls;
                UNIT_ASSERT_VALUES_EQUAL(run(prepared), expected);
                UNIT_ASSERT(calls == expectedCalls);
                const auto scalar = arena.Unary(O::Not, root);
                UNIT_ASSERT_VALUES_EQUAL(PrepareFilterPredicate(arena, scalar), scalar);
                const auto guarded = arena.Binary(O::And, root, arena.Ensure(no, no, "right check"));
                UNIT_ASSERT_VALUES_EQUAL(PrepareFilterPredicate(arena, guarded), guarded);
            }
        }
    }

    Y_UNIT_TEST(NegatedNullTestsKeepNativeMeaningAndChecks) {
        for (const bool nullable : {false, true}) {
            TExpressionArena arena;
            const TType type{K::Int64, nullable};
            const auto input = arena.Parameter("input", type);
            const auto checked = arena.Ensure(input, arena.Literal({K::Bool}, false), "null check");
            for (const auto value : {input, checked}) for (const bool exists : {false, true}) {
                const auto root = arena.Unary(O::Not, arena.Unary(exists ? O::IsNull : O::IsNotNull, value));
                UNIT_ASSERT(arena[root].Op == (exists ? O::IsNotNull : O::IsNull));
                TEvaluationContext context;
                context.Parameter = [&](std::string_view) { return TValue{type, std::int64_t{7}}; };
                if (value == checked) UNIT_ASSERT_EXCEPTION(Evaluate(arena, root, context), TFrontendError);
                else UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, root, context).Data), exists);
                if (nullable) {
                    context.Parameter = [&](std::string_view) { return TValue{type, {}}; };
                    if (value == checked) UNIT_ASSERT_EXCEPTION(Evaluate(arena, root, context), TFrontendError);
                    else UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, root, context).Data), !exists);
                }
            }
        }
    }

    Y_UNIT_TEST(FilterPreparationRetainsOrdinaryConditionalStructure) {
        TExpressionArena arena;
        const auto condition = arena.Parameter("condition", {K::Bool, true});
        const auto predicate = arena.Parameter("predicate", {K::Bool, true});
        const auto no = arena.Literal({K::Bool}, false);
        const auto absent = arena.Literal({K::Bool, true}, {});
        for (const auto rejected : {no, absent}) {
            for (const auto root : {arena.If(condition, predicate, rejected), arena.If(condition, rejected, predicate)}) {
                const auto prepared = PrepareFilterPredicate(arena, root);
                UNIT_ASSERT(arena[prepared].Op == O::If);
                UNIT_ASSERT_VALUES_EQUAL(arena[prepared].Arguments[0], condition);
                const std::array<TScalar, 3> values{TScalar{}, TScalar{false}, TScalar{true}};
                for (const auto& test : values) for (const auto& value : values) {
                    std::vector<std::string> calls;
                    TEvaluationContext context;
                    context.Parameter = [&](std::string_view name) {
                        calls.emplace_back(name);
                        return TValue{{K::Bool, true}, name == "condition" ? test : value};
                    };
                    const auto expected = Evaluate(arena, root, context);
                    const auto expectedCalls = calls;
                    calls.clear();
                    UNIT_ASSERT(Evaluate(arena, prepared, context).Data == expected.Data);
                    UNIT_ASSERT(calls == expectedCalls);
                }
                const auto negated = arena.Unary(O::Not, root);
                UNIT_ASSERT_VALUES_EQUAL(PrepareFilterPredicate(arena, negated), negated);
            }
        }
    }

    Y_UNIT_TEST(FilterBooleanConditionalPreservesTruthAndCheckedEvaluation) {
        TExpressionArena arena;
        const auto value = arena.Parameter("value", {K::Bool, true});
        const auto guard = arena.Parameter("guard", {K::Bool});
        const auto condition = arena.Ensure(value, guard, "checked condition");
        const auto yes = arena.Literal({K::Bool}, true);
        const auto no = arena.Literal({K::Bool}, false);
        const auto absent = arena.Literal({K::Bool, true}, {});
        const std::array<TScalar, 3> values{TScalar{}, TScalar{false}, TScalar{true}};
        for (const auto rejected : {no, absent}) {
            const auto root = arena.If(condition, yes, rejected);
            const auto prepared = PrepareFilterPredicate(arena, root);
            UNIT_ASSERT_VALUES_EQUAL(prepared, condition);
            const auto negated = arena.Unary(O::Not, root);
            UNIT_ASSERT_VALUES_EQUAL(PrepareFilterPredicate(arena, negated), negated);
            for (const auto& input : values) for (const bool valid : {false, true}) {
                std::vector<std::string> calls;
                TEvaluationContext context;
                context.Parameter = [&](std::string_view name) {
                    calls.emplace_back(name);
                    return name == "value" ? TValue{{K::Bool, true}, input} : TValue{{K::Bool}, valid};
                };
                const auto selects = [&](TExprId id) {
                    const auto result = Evaluate(arena, id, context);
                    return std::holds_alternative<bool>(result.Data) && std::get<bool>(result.Data);
                };
                if (valid) {
                    const bool expected = selects(root);
                    const auto expectedCalls = calls;
                    calls.clear();
                    UNIT_ASSERT_VALUES_EQUAL(selects(prepared), expected);
                    UNIT_ASSERT(calls == expectedCalls);
                } else {
                    UNIT_ASSERT_EXCEPTION(selects(root), TFrontendError);
                    const auto expectedCalls = calls;
                    calls.clear();
                    UNIT_ASSERT_EXCEPTION(selects(prepared), TFrontendError);
                    UNIT_ASSERT(calls == expectedCalls);
                }
            }
        }
    }

    Y_UNIT_TEST(InResultPreservesNullsErrorsAndObservableEvaluationOrder) {
        TExpressionArena arena;
        const auto lookup = arena.Parameter("lookup", {K::Int32, true});
        const auto match = arena.Parameter("match", {K::Bool, true});
        const auto checked = arena.Ensure(match, arena.Parameter("check", {K::Bool}), "match check");
        const auto random = arena.Call("RandomUuid", {});
        const auto randomMatch = arena.Binary(O::Equal, random, arena.Literal({K::Uuid}, std::string(16, 'x')));
        const auto no = arena.Literal({K::Bool}, false);
        for (const auto input : {match, checked, arena.Binary(O::And, randomMatch, checked)}) {
            const auto root = arena.InResult(lookup, input, false);
            const auto expected = arena.If(arena.Unary(O::IsNull, lookup), arena.Literal({K::Bool, true}, {}),
                arena.Coalesce(std::array{input, no}));
            UNIT_ASSERT(arena[root].Op == O::InResult);
            UNIT_ASSERT_VALUES_EQUAL(arena.InResult(lookup, input, true), input);
            const auto prepared = PrepareFilterPredicate(arena, root);
            const auto negated = arena.Unary(O::Not, root);
            UNIT_ASSERT_VALUES_EQUAL(PrepareFilterPredicate(arena, negated), negated);
            const auto folded = arena.FoldConstants(std::array{root}).front();
            const std::array<TScalar, 3> matches{TScalar{}, TScalar{false}, TScalar{true}};
            for (const bool missing : {false, true}) for (const auto& value : matches) for (const bool pass : {false, true}) {
                std::vector<std::string> calls;
                TEvaluationContext context;
                context.Parameter = [&](std::string_view name) {
                    calls.emplace_back(name);
                    if (name == "lookup") return TValue{{K::Int32, true}, missing ? TScalar{} : TScalar{std::int64_t{7}}};
                    if (name == "check") return TValue{{K::Bool}, pass};
                    return TValue{{K::Bool, true}, value};
                };
                context.RandomUuid = [&] { calls.emplace_back("random"); return std::string(16, 'x'); };
                const auto run = [&](TExprId id, bool filter) {
                    calls.clear();
                    try {
                        const auto result = Evaluate(arena, id, context);
                        if (filter && result.IsNull()) return std::string{"false"};
                        return FormatValue(result);
                    } catch (const TFrontendError& error) { return std::string{error.what()}; }
                };
                for (const auto& [actual, filter] : std::array{std::pair{root, false}, std::pair{folded, false}, std::pair{prepared, true}}) {
                    const auto expectedValue = run(expected, filter);
                    const auto expectedCalls = calls;
                    UNIT_ASSERT_VALUES_EQUAL(run(actual, filter), expectedValue);
                    UNIT_ASSERT(calls == expectedCalls);
                }
            }
        }
        const auto required = arena.Parameter("required", {K::Int32});
        const auto yes = arena.Literal({K::Bool}, true);
        UNIT_ASSERT_VALUES_EQUAL(arena.InResult(required, yes, false), yes);
        UNIT_ASSERT_EXCEPTION(arena.InResult(lookup, required, false), TFrontendError);
        const auto knownNull = arena.Ensure(arena.Null(), no, "statically Null lookup");
        const auto nullResult = arena.InResult(knownNull, checked, false);
        UNIT_ASSERT(arena[nullResult].Op == O::Literal);
        UNIT_ASSERT(Evaluate(arena, nullResult).IsNull());
    }

    Y_UNIT_TEST(InResultEvaluatesAllLookupFieldsBeforeTestingForNull) {
        TExpressionArena arena;
        const auto key = arena.Parameter("key", {K::Int32, true});
        const auto second = arena.Ensure(arena.Parameter("second", {K::Int32}), arena.Parameter("check", {K::Bool}), "lookup check");
        const auto tuple = arena.Collection(std::array{key, second}, false);
        const auto match = arena.Parameter("match", {K::Bool});
        const auto root = arena.InResult(tuple, match, false);
        const auto expected = arena.If(arena.Unary(O::HasNull, tuple), arena.Literal({K::Bool, true}, {}), match);
        const auto prepared = PrepareFilterPredicate(arena, root);
        for (const bool missing : {false, true}) for (const bool pass : {false, true}) {
            std::vector<std::string> calls;
            TEvaluationContext context;
            context.Parameter = [&](std::string_view name) {
                calls.emplace_back(name);
                if (name == "key") return TValue{{K::Int32, true}, missing ? TScalar{} : TScalar{std::int64_t{7}}};
                if (name == "second") return TValue{{K::Int32}, std::int64_t{8}};
                return TValue{{K::Bool}, name == "check" ? pass : true};
            };
            const auto run = [&](TExprId id, bool filter) {
                calls.clear();
                try {
                    const auto result = Evaluate(arena, id, context);
                    return filter && result.IsNull() ? std::string{"false"} : FormatValue(result);
                } catch (const TFrontendError& error) { return std::string{error.what()}; }
            };
            for (const auto& [actual, filter] : std::array{std::pair{root, false}, std::pair{prepared, true}}) {
                const auto expectedValue = run(expected, filter);
                const auto expectedCalls = calls;
                UNIT_ASSERT_VALUES_EQUAL(run(actual, filter), expectedValue);
                UNIT_ASSERT(calls == expectedCalls);
            }
            const std::vector<std::string> expectedCalls = !pass ? std::vector<std::string>{"key", "check"} : missing
                ? std::vector<std::string>{"key", "check", "second"}
                : std::vector<std::string>{"key", "check", "second", "match"};
            UNIT_ASSERT(calls == expectedCalls);
        }
    }

    Y_UNIT_TEST(InResultImportRebindingAndFoldingRetainGuardedErrors) {
        TExpressionArena arena;
        const auto lookup = arena.Column(1, {K::Int32, true});
        const auto match = arena.Ensure(arena.Literal({K::Bool}, true), arena.Literal({K::Bool}, false), "deferred membership");
        const auto root = arena.InResult(lookup, match, false);
        TExpressionArena copy;
        const auto imported = copy.Import(arena, root, [&](auto id, auto type, auto pos) { return copy.Column(id + 1, type, pos); });
        UNIT_ASSERT(copy[imported].Op == O::InResult);
        for (const bool missing : {false, true}) {
            const auto rewritten = copy.RewriteColumns(imported, [&](auto, auto id, auto type, auto pos) {
                UNIT_ASSERT_VALUES_EQUAL(id, 2);
                return copy.Literal(type, missing ? TScalar{} : TScalar{std::int64_t{7}}, pos);
            });
            const auto folded = copy.FoldConstants(std::array{rewritten}).front();
            UNIT_ASSERT(copy[folded].Type == (TType{K::Bool, true}));
            if (missing) {
                UNIT_ASSERT(copy[folded].Op == O::Literal);
                UNIT_ASSERT(Evaluate(copy, folded).IsNull());
            } else UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(copy, folded), TFrontendError, "deferred membership");
        }
    }

    Y_UNIT_TEST(FixedInCollectionsRetainEmptyAndShortCircuitContracts) {
        for (const bool ansi : {false, true}) {
            TExpressionArena arena;
            const auto lookup = arena.Parameter("key", {K::Int32, true});
            const auto item = arena.Ensure(arena.Literal({K::Int32}, std::int64_t{9}), arena.Literal({K::Bool}, false), "later item");
            const auto first = arena.Literal({K::Int32, true}, std::int64_t{7});
            const auto null = arena.Literal({K::Int32, true}, {});
            const auto empty = arena.InCollection(lookup, arena.Collection({}, true), ansi);
            const auto match = arena.InCollection(lookup, arena.Collection(std::array{null, first, item}, true), ansi);
            unsigned calls = 0;
            TEvaluationContext context;
            context.Parameter = [&](auto) { ++calls; return TValue{{K::Int32, true}, {}}; };
            const auto emptyValue = Evaluate(arena, empty, context);
            UNIT_ASSERT_VALUES_EQUAL(emptyValue.IsNull(), !ansi);
            UNIT_ASSERT_VALUES_EQUAL(calls, ansi ? 0 : 1);
            if (!ansi) UNIT_ASSERT(Evaluate(arena, match, context).IsNull());
            context.Parameter = [&](auto) { ++calls; return TValue{{K::Int32, true}, std::int64_t{7}}; };
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, match, context)), "true");
            context.Parameter = [&](auto) { ++calls; return TValue{{K::Int32, true}, std::int64_t{8}}; };
            UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, match, context), TFrontendError, "later item");
        }
    }


    Y_UNIT_TEST(LikeCaseModeSurvivesNativeRebindingAndFolding) {
        for (const bool caseSensitive : {false, true}) {
            TExpressionArena arena;
            const auto input = arena.Column(1, {K::String, true});
            const auto pattern = arena.Literal({K::String}, std::string{"A#_%"});
            const auto original = arena.Like(input, pattern, "#", {}, caseSensitive);
            const auto rewritten = arena.RewriteColumns(original, [&](auto, auto id, auto type, auto pos) {
                return arena.Column(id + 1, type, pos);
            });
            TExpressionArena copy;
            const auto imported = copy.Import(arena, rewritten, [&](auto id, auto type, auto pos) {
                return copy.Column(id + 1, type, pos);
            });
            const auto folded = copy.FoldConstants(std::span(&imported, 1)).front();
            UNIT_ASSERT(copy[folded].Op == (caseSensitive ? O::Like : O::ILike));
            UNIT_ASSERT(copy[folded].Type == TType{K::Bool});
            TEvaluationContext context;
            context.Column = [](auto id) {
                UNIT_ASSERT_VALUES_EQUAL(id, 3);
                return TValue{{K::String, true}, std::string{"a_b"}};
            };
            context.Like = [&](const TValue& value, std::string_view text, std::string_view escape, bool mode) {
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(value.Data), "a_b");
                UNIT_ASSERT_VALUES_EQUAL(text, "A#_%");
                UNIT_ASSERT_VALUES_EQUAL(escape, "#");
                UNIT_ASSERT_VALUES_EQUAL(mode, caseSensitive);
                return !caseSensitive;
            };
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(copy, folded, context).Data), !caseSensitive);
        }
    }

    Y_UNIT_TEST(RegexModeAndDependenciesSurviveRebindingAndFolding) {
        for (const bool fullMatch : {false, true}) {
            TExpressionArena arena;
            const auto input = arena.Column(1, {K::String, true});
            const auto pattern = arena.Parameter("pattern", {K::String});
            const auto root = arena.Regex(input, pattern, fullMatch);
            const auto rewritten = arena.RewriteColumns(root, [&](auto, auto id, auto type, auto pos) {
                return arena.Column(id + 1, type, pos);
            });
            TExpressionArena copy;
            const auto imported = copy.Import(arena, rewritten, [&](auto id, auto type, auto pos) {
                return copy.Column(id + 1, type, pos);
            });
            const auto folded = copy.FoldConstants(std::array{imported}).front();
            UNIT_ASSERT(copy[folded].Op == (fullMatch ? O::RegexMatch : O::RegexGrep));
            TEvaluationContext context;
            context.Column = [](auto id) {
                UNIT_ASSERT_VALUES_EQUAL(id, 3);
                return TValue{{K::String, true}, std::string{"abc"}};
            };
            context.Parameter = [](auto name) {
                UNIT_ASSERT_VALUES_EQUAL(name, "pattern");
                return TValue{{K::String}, std::string{"b"}};
            };
            context.Regex = [&](const TValue& value, std::string_view text, bool mode) {
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(value.Data), "abc");
                UNIT_ASSERT_VALUES_EQUAL(text, "b");
                UNIT_ASSERT_VALUES_EQUAL(mode, fullMatch);
                return !fullMatch;
            };
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(copy, folded, context).Data), !fullMatch);
        }
    }

    Y_UNIT_TEST(ListIndexDistinguishesAbsentRowsFromPresentNullItems) {
        TExpressionArena arena;
        const auto present = arena.Literal({K::Int32, true}, std::int64_t{7});
        const auto absent = arena.Literal({K::Int32, true}, {});
        const TExprId values[]{present, absent};
        const auto list = arena.Collection(values, true);
        for (const std::int64_t key : {-1, 0, 1, 2, 100}) {
            const auto root = arena.ListElement(list, Integer(arena, key));
            UNIT_ASSERT(arena[root].Type == (TType{K::Int32, 2}));
            for (const auto id : {root, arena.FoldConstants(std::span(&root, 1)).front()}) {
                const auto result = Evaluate(arena, id);
                UNIT_ASSERT_VALUES_EQUAL(result.IsNull(), key < 0 || key >= 2);
                if (!result.IsNull()) {
                    const auto& item = *std::get<TOptionalValue>(result.Data);
                    UNIT_ASSERT_VALUES_EQUAL(item.IsNull(), key == 1);
                    if (!item.IsNull()) UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(item.Data), 7);
                }
            }
        }
        const auto index = arena.Parameter("index", {K::Uint32, true});
        const auto root = arena.ListElement(list, index);
        TEvaluationContext context; context.Parameter = [](auto) { return TValue{{K::Uint32, true}, {}}; };
        UNIT_ASSERT(Evaluate(arena, root, context).IsNull());
        TExpressionArena copied;
        const auto imported = copied.Import(arena, root, [](auto, auto, auto) -> TExprId { UNIT_FAIL("unexpected column"); return 0; });
        UNIT_ASSERT(Evaluate(copied, imported, context).IsNull());
    }

    Y_UNIT_TEST(CalendarMinuteAndSecondsPreserveTypesAndNulls) {
        TExpressionArena arena;
        const TType types[]{{K::Date}, {K::Datetime}, {K::Timestamp}, {K::Date32}, {K::Datetime64}, {K::Timestamp64}};
        for (const auto type : types) {
            for (const bool nullable : {false, true}) {
                const auto inputType = nullable ? type.Optional() : type;
                const TExprId arg[]{arena.Column(0, inputType)};
                const auto seconds = arena.Call("DateTime::ToSeconds", arg);
                UNIT_ASSERT(arena[seconds].Type == (TType{type.IsExtendedTemporal() ? K::Int64 : K::Uint32, nullable}));
                const bool days = type.Kind == K::Date || type.Kind == K::Date32;
                const bool micros = type.Kind == K::Timestamp || type.Kind == K::Timestamp64;
                for (const std::int64_t value : {-1, 0, 1, 86399, 86400, 86401}) {
                    if (!type.IsExtendedTemporal() && value < 0) continue;
                    TEvaluationContext context;
                    const auto payload = days ? value / 86400 : micros ? value * 1'000'000 + (value < 0 ? -123456 : 123456) : value;
                    context.Column = [&](auto) { return TValue{inputType, type.IsExtendedTemporal() ? TScalar{payload} : TScalar{std::uint64_t(payload)}}; };
                    const auto actual = Evaluate(arena, seconds, context);
                    const auto expected = days ? value / 86400 * 86400 : value;
                    if (type.IsExtendedTemporal()) UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(actual.Data), expected);
                    else UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(actual.Data), expected);
                    if (!type.IsExtendedTemporal()) {
                        const auto minute = arena.Call("DateTime::GetMinute", arg);
                        UNIT_ASSERT(arena[minute].Type == (TType{K::Uint8, nullable}));
                        UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena, minute, context).Data), days ? 0 : value / 60 % 60);
                    }
                }
                if (nullable) {
                    TEvaluationContext context; context.Column = [&](auto) { return TValue{inputType, {}}; };
                    UNIT_ASSERT(Evaluate(arena, seconds, context).IsNull());
                }
            }
        }
    }

    Y_UNIT_TEST(UrlFunctionsMatchExistingByteSemantics) {
        TExpressionArena arena;
        const std::pair<const char*, const char*> cases[]{
            {"https://www.example.org:443/path?x=1", "example.org"},
            {"WWW.Example.org/path", "Example.org"},
            {"localhost:80/path", "localhost"},
            {"", nullptr}, {"http:///path", nullptr}, {"www.", nullptr},
        };
        for (const auto& [input, expected] : cases) {
            const TExprId arg[]{arena.Literal({K::String}, std::string(input))};
            const TExprId host[]{arena.Call("Url::GetHost", arg)};
            const auto root = arena.Call("Url::CutWWW", host);
            UNIT_ASSERT(arena[root].Type == (TType{K::String, true}));
            for (auto id : {root, arena.FoldConstants(std::span(&root, 1)).front()}) {
                const auto actual = Evaluate(arena, id);
                UNIT_ASSERT_VALUES_EQUAL(actual.IsNull(), !expected);
                if (expected) UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(actual.Data), expected);
            }
        }
        const TExprId null[]{arena.Literal({K::Null}, {})};
        UNIT_ASSERT(Evaluate(arena, arena.Call("Url::GetHost", null)).IsNull());
    }

    Y_UNIT_TEST(TupleElementPreservesPayloadAndStateNullability) {
        TExpressionArena arena;
        for (const TType payloadType : {TType{K::Int32}, TType{K::Int32, true}, TType{K::Null}}) {
            for (const bool missing : {false, true}) {
                const auto payload = arena.Literal(payloadType, payloadType.Kind == K::Null ? TScalar{} : TScalar{std::int64_t{7}});
                const TExprId items[]{Integer(arena, 3), payload};
                const auto tuple = arena.Collection(items, false);
                for (const bool optional : {false, true}) {
                    if (missing && !optional) continue;
                    const auto state = missing ? arena.Literal(arena[tuple].Type.Optional(), {}) :
                        optional ? arena.Convert(tuple, arena[tuple].Type.Optional()) : tuple;
                    const auto root = arena.TupleElement(state, 1);
                    const auto expected = payloadType.Kind == K::Null || missing;
                    UNIT_ASSERT_VALUES_EQUAL(Evaluate(arena, root).IsNull(), expected);
                    UNIT_ASSERT_VALUES_EQUAL(Evaluate(arena, arena.FoldConstants(std::span(&root, 1)).front()).IsNull(), expected);
                    if (!expected) UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(arena, root).Data), 7);
                    TExpressionArena copy;
                    const auto imported = copy.Import(arena, root, [](auto, auto, auto) -> TExprId { UNIT_FAIL("unexpected column"); return 0; });
                    UNIT_ASSERT_VALUES_EQUAL(Evaluate(copy, imported).IsNull(), expected);
                }
            }
        }
    }

    Y_UNIT_TEST(DecimalDictionaryMembershipHandlesNullAndSpecialKeys) {
        TExpressionArena arena;
        const TType key{K::Decimal, true, 1, 0};
        const auto listType = arena.ListType(key);
        for (const bool empty : {false, true}) {
            auto values = std::make_shared<TTupleValue>();
            if (!empty) values->Items = {{key, {}}, {key, TDecimal{1}}, {key, TDecimal{TDecimal::NaN()}}};
            const auto list = arena.Literal(listType, TTuple(values));
            for (const bool ansi : {false, true}) for (const TScalar& value : {TScalar{}, TScalar{TDecimal{1}}, TScalar{TDecimal{2}}, TScalar{TDecimal{TDecimal::NaN()}}}) {
                const auto root = arena.InCollection(arena.Literal(key, value), list, ansi);
                const auto actual = Evaluate(arena, root);
                const bool nullLookup = std::holds_alternative<std::monostate>(value);
                const bool match = !nullLookup && std::get<TDecimal>(value).Coefficient != 2 && !empty;
                const bool unknown = nullLookup ? (!ansi || !empty) : ansi && !empty && !match;
                UNIT_ASSERT_VALUES_EQUAL(actual.IsNull(), unknown);
                if (!unknown) UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(actual.Data), match);
            }
        }
    }


    Y_UNIT_TEST(DecimalComparisonsDoNotRequireAnOversizedCommonValue) {
        TExpressionArena arena;
        __int128 big = 1;
        for (unsigned i = 0; i < 26; ++i) big *= 10;
        const TType low{K::Decimal, false, 35, 2}, high{K::Decimal, false, 35, 9};
        for (const __int128 whole : {__int128{-100}, __int128{-1}, __int128{0}, __int128{1}, __int128{100}, big - 1, 1 - big}) {
            const auto coarse = arena.Literal(low, TDecimal{whole * 100});
            for (const std::int64_t remainder : {-9999999, -5000000, -1, 0, 1, 5000000, 9999999}) {
                const auto fine = arena.Literal(high, TDecimal{whole * 1000000000 + remainder});
                for (const auto [op, expected] : {std::pair{O::Equal, remainder == 0}, {O::NotEqual, remainder != 0},
                    {O::Less, remainder < 0}, {O::LessEqual, remainder <= 0}, {O::Greater, remainder > 0}, {O::GreaterEqual, remainder >= 0}}) {
                    const auto result = arena.Binary(op, fine, coarse);
                    UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, result).Data), expected);
                    const auto folded = arena.FoldConstants(std::span(&result, 1)).front();
                    UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded).Data), expected);
                }
            }
        }
    }

    Y_UNIT_TEST(EnsurePreservesNullablePredicateAndLazyValueEvaluation) {
        TExpressionArena arena;
        const auto value = arena.Parameter("value", {K::Int32});
        const auto message = arena.Literal({K::String}, std::string("rejected"));
        for (const TScalar& predicate : {TScalar{true}, TScalar{false}, TScalar{}}) {
            const TExprId args[]{value, arena.Literal({K::Bool, true}, predicate), message};
            const auto checked = arena.Call("Ensure", args);
            unsigned calls = 0;
            TEvaluationContext context;
            context.Parameter = [&](std::string_view) { ++calls; return TValue{{K::Int32}, std::int64_t{42}}; };
            if (std::holds_alternative<bool>(predicate) && std::get<bool>(predicate)) {
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, checked, context)), "42");
                UNIT_ASSERT_VALUES_EQUAL(calls, 1);
            } else {
                UNIT_ASSERT_EXCEPTION(Evaluate(arena, checked, context), TFrontendError);
                UNIT_ASSERT_VALUES_EQUAL(calls, 0);
            }
        }
    }

    Y_UNIT_TEST(UuidValuesPreserveBytesAndCanonicalText) {
        TExpressionArena arena;
        const auto text = arena.Literal({ETypeKind::String}, std::string("01234567-89ab-CDEF-0123-456789abcdef"));
        const auto uuid = arena.Cast(text, {ETypeKind::Uuid});
        const auto value = Evaluate(arena, uuid);
        UNIT_ASSERT(!value.IsNull());
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(value.Data).size(), 16);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(value), "01234567-89ab-cdef-0123-456789abcdef");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(uuid, {ETypeKind::Utf8}))), FormatValue(value));
        for (const auto* invalid : {"", "0123456789abcdef0123456789abcdef", "01234567889ab-cdef-0123-456789abcdef", "01234567-89ab-cdef-0123-456789abcdeg"}) {
            UNIT_ASSERT(Evaluate(arena, arena.Cast(arena.Literal({ETypeKind::String}, std::string(invalid)), {ETypeKind::Uuid})).IsNull());
        }
        const auto dynamic = arena.Cast(arena.Parameter("text", {ETypeKind::String}), {ETypeKind::Uuid});
        TEvaluationContext context;
        context.Parameter = [](std::string_view) { return TValue{{ETypeKind::String}, std::string("01234567-89ab-cdef-0123-456789abcdef")}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, dynamic, context)), FormatValue(value));
    }

    Y_UNIT_TEST(VolatileUuidIsNeverFoldedFactoredOrEvaluatedTwice) {
        TExpressionArena arena;
        const auto random = arena.Call("RandomUuid", {});
        UNIT_ASSERT(!arena[random].Repeatable);
        const auto asText = arena.Cast(random, {ETypeKind::Utf8});
        UNIT_ASSERT(!arena[asText].Repeatable);
        const TExprId roots[]{asText};
        UNIT_ASSERT_VALUES_EQUAL(arena.FoldConstants(roots).front(), asText);
        unsigned calls = 0;
        TEvaluationContext context;
        context.RandomUuid = [&] { ++calls; return std::string(16, '\0'); };
        const auto same = arena.Binary(EOpcode::Equal, random, random);
        UNIT_ASSERT(std::get<bool>(Evaluate(arena, same, context).Data));
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        const auto another = arena.Call("RandomUuid", {});
        const auto other = arena.Binary(EOpcode::Equal, random, another);
        (void)Evaluate(arena, other, context);
        UNIT_ASSERT_VALUES_EQUAL(calls, 3);
        const auto disjunction = arena.Binary(EOpcode::Or, same, other);
        UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(disjunction), disjunction);
        TExpressionArena imported;
        const auto copied = imported.Import(arena, asText, [](TColumnId, TType, TSourcePosition) -> TExprId { throw std::logic_error("unexpected column"); });
        UNIT_ASSERT(!imported[copied].Repeatable);
        const auto skip = arena.If(arena.Literal({ETypeKind::Bool}, false), random, arena.Literal({ETypeKind::Uuid}, std::string(16, '\0')));
        (void)Evaluate(arena, skip, context);
        UNIT_ASSERT_VALUES_EQUAL(calls, 3);
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, random), TFrontendError);
    }

    Y_UNIT_TEST(VolatileNumberRetainsDependenciesAndSharing) {
        TExpressionArena arena;
        const auto one = arena.Literal({K::Uint64}, std::uint64_t{1});
        const auto dependency = arena.Binary(EOpcode::Add, one, one);
        const auto random = arena.Call("RandomNumber", std::span(&dependency, 1));
        UNIT_ASSERT(arena[random].Type == TType{K::Uint64});
        UNIT_ASSERT(!arena[random].Repeatable);
        const auto folded = arena.FoldConstants(std::span(&random, 1)).front();
        UNIT_ASSERT(arena[folded].Op == EOpcode::RandomNumber);
        UNIT_ASSERT(!arena[folded].Repeatable);
        UNIT_ASSERT(arena[arena[folded].Arguments.front()].Op == EOpcode::Literal);
        unsigned calls = 0;
        TEvaluationContext context;
        context.RandomNumber = [&] { return std::uint64_t{++calls}; };
        const auto same = arena.Binary(EOpcode::Equal, folded, folded);
        UNIT_ASSERT(std::get<bool>(Evaluate(arena, same, context).Data));
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        const auto other = arena.Binary(EOpcode::Equal, folded, arena.Call("RandomNumber", {}));
        UNIT_ASSERT(!std::get<bool>(Evaluate(arena, other, context).Data));
        UNIT_ASSERT_VALUES_EQUAL(calls, 3);
        const auto disjunction = arena.Binary(EOpcode::Or, same, other);
        UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(disjunction), disjunction);
        TExpressionArena imported;
        const auto copied = imported.Import(arena, random, [](TColumnId, TType, TSourcePosition) -> TExprId {
            throw std::logic_error("unexpected column");
        });
        UNIT_ASSERT(!imported[copied].Repeatable);
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(imported, copied, context).Data), 4);
        const auto skip = arena.If(arena.Literal({K::Bool}, false), random, one);
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena, skip, context).Data), 1);
        UNIT_ASSERT_VALUES_EQUAL(calls, 4);
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, random), TFrontendError);
    }

    Y_UNIT_TEST(ExtendedTemporalValuesKeepSignedRanges) {
        for (const auto [kind, minimum, maximum] : {
                std::tuple{K::Date32, -53375809LL, 53375807LL},
                std::tuple{K::Datetime64, -4611669897600LL, 4611669811199LL},
                std::tuple{K::Timestamp64, -4611669897600000000LL, 4611669811199999999LL},
                std::tuple{K::Interval64, -9223339708799999999LL, 9223339708799999999LL}}) {
            TExpressionArena arena;
            const TType type{kind};
            UNIT_ASSERT(ParseType(type.ToString()) == type);
            for (const std::int64_t value : {minimum, -1LL, 0LL, 1LL, maximum}) {
                const auto literal = arena.Literal(type, value);
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(arena, literal).Data), value);
                const auto cast = arena.Cast(Integer(arena, value), type);
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(arena, cast).Data), value);
                const auto equal = arena.Binary(O::Equal, literal, cast);
                UNIT_ASSERT(std::get<bool>(Evaluate(arena, equal).Data));
                const auto folded = arena.FoldConstants(std::span(&equal, 1)).front();
                UNIT_ASSERT(arena[folded].Op == O::Literal);
            }
            for (const std::int64_t value : {minimum - 1, maximum + 1}) {
                UNIT_ASSERT_EXCEPTION(arena.Literal(type, value), TFrontendError);
                UNIT_ASSERT(Evaluate(arena, arena.Cast(Integer(arena, value), type)).IsNull());
            }
        }
        TExpressionArena arena;
        const auto timestamp = arena.Literal({K::Timestamp64}, std::int64_t{-1000001});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(timestamp, {K::Datetime64}))), "-1");
        UNIT_ASSERT(Evaluate(arena, arena.Cast(timestamp, {K::Date})).IsNull());
        const auto interval = arena.Literal({K::Interval64}, std::int64_t{2000001});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Add, timestamp, interval))), "1000000");
        UNIT_ASSERT(CommonType({K::Date}, {K::Timestamp64}) == (TType{K::Timestamp64}));
    }

    Y_UNIT_TEST(CollectionMembershipPreservesNullAndCheckedKeySemantics) {
        for (bool ansi : {false, true}) for (bool empty : {false, true}) {
            TExpressionArena arena;
            const TType item{K::Int32, true};
            const auto listType = arena.ListType(item);
            auto items = std::make_shared<TTupleValue>();
            if (!empty) items->Items = {{item, std::int64_t{2}}, {item, {}}};
            const auto list = arena.Literal(listType, TTuple(items));
            for (const auto value : {2.0, 2.5, 2147483648.0}) {
                const auto root = arena.InCollection(arena.Literal({K::Double}, value), list, ansi);
                const auto result = Evaluate(arena, root);
                const bool found = !empty && value == 2.0;
                UNIT_ASSERT_VALUES_EQUAL(result.IsNull(), ansi && !empty && !found);
                if (!result.IsNull()) UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(result.Data), found);
                const auto folded = arena.FoldConstants(std::span(&root, 1)).front();
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(result));
                TExpressionArena copied;
                const auto imported = copied.Import(arena, root, {});
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copied, imported)), FormatValue(result));
            }
            const auto nullRoot = arena.InCollection(arena.Literal({K::Int64, true}, {}), list, ansi);
            const auto result = Evaluate(arena, nullRoot);
            UNIT_ASSERT_VALUES_EQUAL(result.IsNull(), !ansi || !empty);
            if (!result.IsNull()) UNIT_ASSERT(!std::get<bool>(result.Data));
        }
        TExpressionArena arena;
        const auto listType = arena.ListType({K::Int64});
        auto values = std::make_shared<TTupleValue>();
        values->Items = {{{K::Int64}, std::int64_t{9007199254740993LL}}};
        const auto lookup = arena.Literal({K::Double}, 9007199254740992.0);
        const auto runtime = arena.InCollection(lookup, arena.Literal(listType, TTuple(values)), false);
        const std::array fixed{Integer(arena, 9007199254740993LL)};
        const auto literal = arena.InCollection(lookup, arena.Collection(fixed, true), false);
        UNIT_ASSERT(!std::get<bool>(Evaluate(arena, runtime).Data));
        UNIT_ASSERT(std::get<bool>(Evaluate(arena, literal).Data));
        auto invalid = arena.TupleType(std::array{TType{K::Int32}, TType{K::Int64}}); invalid.Kind = K::List;
        UNIT_ASSERT_EXCEPTION(arena.ValidateType(invalid), TFrontendError);
    }

    Y_UNIT_TEST(TupleComparisonsPreserveNullAndLexicographicOrder) {
        TExpressionArena arena;
        const TType type{K::Int32, true};
        const std::array left{arena.Column(0, type), arena.Column(1, type)};
        const std::array right{arena.Column(2, type), arena.Column(3, type)};
        const auto a = arena.Collection(left, false), b = arena.Collection(right, false);
        const std::array<TScalar, 3> values{std::int64_t{-1}, std::int64_t{1}, std::monostate{}};
        for (const auto op : {O::Equal, O::NotEqual, O::Less, O::LessEqual, O::Greater, O::GreaterEqual}) {
            const auto root = arena.Binary(op, a, b);
            for (const auto& a0 : values) for (const auto& a1 : values)
                for (const auto& b0 : values) for (const auto& b1 : values) {
                    const std::array<TScalar, 4> row{a0, a1, b0, b1};
                    std::optional<bool> expected;
                    if (op == O::Equal || op == O::NotEqual) {
                        bool missing = false, different = false;
                        for (size_t i = 0; i < 2; ++i) {
                            const auto* x = std::get_if<std::int64_t>(&row[i]);
                            const auto* y = std::get_if<std::int64_t>(&row[i + 2]);
                            if (!x || !y) missing = true;
                            else different |= *x != *y;
                        }
                        if (different || !missing) expected = op == O::Equal ? !different : different;
                    } else {
                        int order = 0;
                        bool missing = false;
                        for (size_t i = 0; i < 2; ++i) {
                            const auto* x = std::get_if<std::int64_t>(&row[i]);
                            const auto* y = std::get_if<std::int64_t>(&row[i + 2]);
                            if (!x || !y) { missing = true; break; }
                            if (*x != *y) { order = *x < *y ? -1 : 1; break; }
                        }
                        if (!missing) expected = op == O::Less ? order < 0 : op == O::LessEqual ? order <= 0 :
                            op == O::Greater ? order > 0 : order >= 0;
                    }
                    TEvaluationContext context;
                    context.Column = [&](TColumnId id) { return TValue{type, row.at(id)}; };
                    const auto result = Evaluate(arena, root, context);
                    UNIT_ASSERT_VALUES_EQUAL(result.IsNull(), !expected.has_value());
                    if (expected) UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(result.Data), *expected);
                }
        }
    }

    Y_UNIT_TEST(CommonConjunctsPreserveThreeValuedLogicAndTypes) {
        const std::array<TScalar, 3> values{false, true, std::monostate{}};
        for (const bool nullable : {false, true}) for (const bool absorption : {false, true}) {
            TExpressionArena arena;
            const auto column = [&](TColumnId id) { return arena.Column(id, {K::Bool, id != 0 || nullable}); };
            const auto left = arena.Binary(O::And, column(0), column(1));
            auto right = absorption ? arena.Binary(O::And, column(0), column(1)) : column(0);
            right = arena.Binary(O::And, right, column(2));
            const auto root = arena.Binary(O::Or, left, right);
            const auto original = arena[root].Arguments;
            const auto factored = arena.FactorCommonConjuncts(root);
            UNIT_ASSERT(factored != root);
            UNIT_ASSERT(arena[factored].Type == arena[root].Type);
            UNIT_ASSERT(arena[root].Arguments == original);
            UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(factored), factored);
            for (const auto& a : values) for (const auto& b : values) for (const auto& c : values) {
                if (!nullable && std::holds_alternative<std::monostate>(a)) continue;
                const std::array<TScalar, 3> bindings{a, b, c};
                TEvaluationContext context;
                context.Column = [&](TColumnId id) { return TValue{{K::Bool, id != 0 || nullable}, bindings.at(id)}; };
                const auto before = Evaluate(arena, root, context), after = Evaluate(arena, factored, context);
                UNIT_ASSERT(before.Type == after.Type);
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(before), FormatValue(after));
            }
        }
    }

    Y_UNIT_TEST(CommonConjunctIdentityIncludesPayloadsAndSharedDagsStayBounded) {
        TExpressionArena arena;
        const auto a = arena.Column(1, {K::Bool}), b = arena.Column(2, {K::Bool});
        const auto combine = [&](TExprId first, TExprId second) {
            return arena.Binary(O::Or, arena.Binary(O::And, first, a), arena.Binary(O::And, second, b));
        };
        const auto p = arena.Parameter("p", {K::Bool}), q = arena.Parameter("q", {K::Bool});
        const auto parameters = combine(p, q);
        UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(parameters), parameters);
        const auto number = arena.Column(0, {K::Double});
        const auto positive = arena.Binary(O::Equal, number, arena.Literal({K::Double}, 0.0));
        const auto negative = arena.Binary(O::Equal, number, arena.Literal({K::Double}, -0.0));
        const auto zeros = combine(positive, negative);
        UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(zeros), zeros);
        const auto repeated = combine(positive,
            arena.Binary(O::Equal, arena.Column(0, {K::Double}, {123, 4, 5}), arena.Literal({K::Double}, 0.0)));
        UNIT_ASSERT(arena.FactorCommonConjuncts(repeated) != repeated);
        const auto shared = arena.Binary(O::And, a, b);
        auto deep = shared;
        for (std::size_t i = 0; i < 10'000; ++i) deep = arena.Binary(O::Or, deep, deep);
        UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(deep), shared);
    }

    Y_UNIT_TEST(CommonConjunctsDoNotMoveRuntimeChecksPastGuards) {
        for (const bool unwrap : {false, true}) {
            TExpressionArena arena;
            const auto bad = unwrap ? arena.Unary(O::Unwrap, arena.Literal({K::Bool, true}, std::monostate{})) :
                arena.Ensure(arena.Literal({K::Bool}, true), arena.Literal({K::Bool}, false), "deferred check");
            const auto left = arena.Binary(O::And, arena.Column(0, {K::Bool}), bad);
            const auto right = arena.Binary(O::And, arena.Column(1, {K::Bool}), bad);
            const auto root = arena.Binary(O::Or, left, right);
            const auto size = arena.Size();
            UNIT_ASSERT_VALUES_EQUAL(arena.FactorCommonConjuncts(root), root);
            UNIT_ASSERT_VALUES_EQUAL(arena.Size(), size);
            TEvaluationContext context;
            context.Column = [](TColumnId) { return TValue{{K::Bool}, false}; };
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, root, context)), "false");
            context.Column = [](TColumnId) { return TValue{{K::Bool}, true}; };
            UNIT_ASSERT_EXCEPTION(Evaluate(arena, root, context), TFrontendError);
        }
    }

    Y_UNIT_TEST(JsonValuesRetainTheirRuntimeContractAcrossRewrites) {
        for (const auto kind : {K::Json, K::JsonDocument}) {
            TExpressionArena arena;
            UNIT_ASSERT(ParseType(TType{kind, true}.ToString()) == (TType{kind, true}));
            const auto input = arena.Column(0, {kind, true});
            const auto value = arena.JsonValue(input, "$.'a.b'[0].*");
            const auto rewritten = arena.RewriteColumns(value, [&](TExprId, TColumnId, TType type, TSourcePosition) {
                return arena.Column(1, type.Required());
            });
            UNIT_ASSERT_VALUES_EQUAL(arena[rewritten].Name, arena[value].Name);
            UNIT_ASSERT(arena[rewritten].Type == arena[value].Type);
            UNIT_ASSERT_VALUES_EQUAL(arena[arena[rewritten].Arguments[0]].ColumnId, 1);
            TExpressionArena copied;
            const auto imported = copied.Import(arena, rewritten, [&](TColumnId id, TType type, TSourcePosition) {
                return copied.Column(id, type);
            });
            UNIT_ASSERT_VALUES_EQUAL(copied[imported].Name, arena[value].Name);
            TEvaluationContext context;
            context.Column = [&](TColumnId) { return TValue{{kind}, std::string("opaque")}; };
            UNIT_ASSERT_EXCEPTION(Evaluate(copied, imported, context), TFrontendError);
            size_t calls = 0;
            context.JsonValue = [&](const TValue& input, std::string_view path) {
                ++calls;
                UNIT_ASSERT(input.Type.Kind == kind);
                UNIT_ASSERT_VALUES_EQUAL(path, "$.'a.b'[0].*");
                return TValue{{K::Utf8, true}, std::string("result")};
            };
            const TExprId alternatives[]{imported, imported};
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copied, copied.Coalesce(alternatives), context)), "result");
            UNIT_ASSERT_VALUES_EQUAL(calls, 1);
            const auto nullInput = arena.Literal({kind, true}, {});
            const auto nullValue = arena.JsonValue(nullInput, "$.a");
            const TExprId roots[]{value, nullValue};
            const auto folded = arena.FoldConstants(roots);
            UNIT_ASSERT_VALUES_EQUAL(folded[0], value);
            UNIT_ASSERT_VALUES_EQUAL(folded[1], nullValue);
            UNIT_ASSERT_EXCEPTION(arena.Unary(O::StablePickle, input), TUnsupportedFeature);
            if (kind == K::JsonDocument) UNIT_ASSERT_EXCEPTION(arena.Literal({kind}, std::string("{}")), TUnsupportedFeature);
            else UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Literal({kind}, std::string("{}")))), "{}");
            const TType fields[]{{kind, true}};
            UNIT_ASSERT_EXCEPTION(arena.TupleType(fields), TUnsupportedFeature);
            UNIT_ASSERT_EXCEPTION(arena.JsonValue(input, "$.\"a\\nb\""), TUnsupportedFeature);
            UNIT_ASSERT_EXCEPTION(arena.JsonValue(input, "$[4294967296]"), TUnsupportedFeature);
        }
    }

    Y_UNIT_TEST(TupleStatesAreInternedAndHaveValidatedFields) {
        Cerr << "native layout: type=" << sizeof(TType) << ", node=" << sizeof(TExprNode) << ", value=" << sizeof(TValue) << Endl;
        TExpressionArena arena;
        const TType fields[]{{K::Decimal, false, 35, 2}, {K::Uint64}};
        const auto state = arena.TupleType(fields);
        UNIT_ASSERT(state == arena.TupleType(fields));
        UNIT_ASSERT_VALUES_EQUAL(arena.TupleItems(state).size(), 2);
        UNIT_ASSERT(arena.TupleItems(state)[0] == fields[0]);
        UNIT_ASSERT(arena.TupleItems(state.Optional())[1] == fields[1]);
        UNIT_ASSERT(CommonType(state, state.Optional()) == state.Optional());
        UNIT_ASSERT_EXCEPTION(arena.Column(0, {K::Tuple}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Column(0, {K::Tuple, false, 0, 0, 99}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.Column(0, {K::Int64, false, 0, 0, state.Shape}), TFrontendError);
        UNIT_ASSERT_EXCEPTION(arena.TupleItems({K::Double}), TFrontendError);
        const TType malformed[]{{K::Decimal, false, 1, 2}};
        UNIT_ASSERT_EXCEPTION(arena.TupleType(malformed), TFrontendError);
        const TType nested[]{state.Optional()};
        UNIT_ASSERT(arena.TupleItems(arena.TupleType(nested))[0] == state.Optional());
    }

    Y_UNIT_TEST(TupleStatesExecuteFoldRebindAndImportWithoutLosingTypes) {
        TExpressionArena source, target;
        const TType unrelated[]{{K::String}};
        target.TupleType(unrelated); // Source IDs deliberately mean different things here.
        const TType fields[]{{K::Decimal, false, 35, 2}, {K::Uint64}, {K::Float, true}};
        const auto state = source.TupleType(fields);
        const TTuple data = std::make_shared<const TTupleValue>(TTupleValue{{
            {fields[0], TDecimal{12345}}, {fields[1], std::uint64_t{3}}, {fields[2], 1.1}}});
        const auto literal = source.Literal(state, data);
        const auto column = source.Column(7, state);
        const auto lifted = source.Convert(column, state.Optional());
        TEvaluationContext bindings;
        bindings.Column = [&](TColumnId id) -> TValue { UNIT_ASSERT_VALUES_EQUAL(id, 7); return {state, data}; };
        const auto executed = Evaluate(source, lifted, bindings);
        UNIT_ASSERT(executed.Type == state.Optional());
        UNIT_ASSERT_VALUES_EQUAL(std::get<double>(std::get<TTuple>(executed.Data)->Items[2].Data), double(float(1.1)));
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(executed), FormatValue(Evaluate(source, literal)));
        const TExprId alternatives[]{source.Literal(state.Optional(), std::monostate{}), literal,
            source.Parameter("unreached", state)};
        const auto coalesce = source.Coalesce(alternatives);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(source, coalesce)), FormatValue(executed));
        const TExprId roots[]{coalesce, source.Convert(literal, state.Optional())};
        const auto folded = source.FoldConstants(roots);
        UNIT_ASSERT(source[folded[0]].Type == state);
        UNIT_ASSERT(source[folded[1]].Type == state.Optional());
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(source, folded[0])), FormatValue(executed));
        const auto rebound = source.RewriteColumns(lifted, [&](auto, auto, auto, auto) { return literal; });
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(source, rebound)), FormatValue(executed));
        const auto imported = target.Import(source, literal,
            [&](auto, auto, auto) -> TExprId { UNIT_FAIL("literal import has no columns"); return 0; });
        UNIT_ASSERT(target[imported].Type != state);
        UNIT_ASSERT(target[imported].Type == target.ImportType(source, state));
        UNIT_ASSERT(target.TupleItems(target[imported].Type)[0] == fields[0]);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(target, imported)), FormatValue(Evaluate(source, literal)));
        const auto importedColumn = target.Import(source, lifted, [&](auto id, auto type, auto pos) {
            UNIT_ASSERT(type == target.ImportType(source, state));
            return target.Column(id, type, pos);
        });
        UNIT_ASSERT(target[importedColumn].Type == target.ImportType(source, state).Optional());
        const auto bad = std::make_shared<const TTupleValue>(TTupleValue{{{fields[1], std::uint64_t{3}}}});
        UNIT_ASSERT_EXCEPTION(source.Literal(state, bad), TFrontendError);
        UNIT_ASSERT_EXCEPTION(source.Literal(state, TTuple{}), TFrontendError);
        bindings.Column = [&](auto) -> TValue { return {state, bad}; };
        UNIT_ASSERT_EXCEPTION(Evaluate(source, column, bindings), TFrontendError);
        UNIT_ASSERT_EXCEPTION(source.Binary(O::Equal, literal, literal), TFrontendError);
        UNIT_ASSERT_EXCEPTION(source.Unary(O::StablePickle, literal), TFrontendError);
        UNIT_ASSERT_EXCEPTION(source.Cast(literal, {K::String}), TFrontendError);
        const TType textFields[]{{K::String}};
        const auto textType = source.TupleType(textFields);
        const auto text = source.Literal(textType, std::make_shared<const TTupleValue>(TTupleValue{{{textFields[0], std::string(32, 'x')}}}));
        const TExprId textRoots[]{source.Convert(text, textType.Optional())};
        UNIT_ASSERT_VALUES_EQUAL(source.FoldConstants(textRoots, 8).front(), textRoots[0]);
        UNIT_ASSERT(source[source.FoldConstants(textRoots, 32).front()].Op == O::Literal);
    }

    Y_UNIT_TEST(DecimalCoalescePreservesCoefficientsAndOptionality) {
        for (const std::uint8_t scale : {0, 2, 3}) for (const bool fallbackRequired : {false, true}) {
            TExpressionArena arena;
            const TType narrow{K::Decimal, true, 3, scale}, wide{K::Decimal, !fallbackRequired, 35, scale};
            for (const auto& first : {TScalar{std::monostate{}}, TScalar{TDecimal{-999}}, TScalar{TDecimal{0}}, TScalar{TDecimal{999}},
                    TScalar{TDecimal{TDecimal::Inf()}}, TScalar{TDecimal{-TDecimal::Inf()}}, TScalar{TDecimal{TDecimal::NaN()}}}) {
                for (const auto& second : {TScalar{std::monostate{}}, TScalar{TDecimal{123456789}}, TScalar{TDecimal{TDecimal::NaN()}}}) {
                    if (fallbackRequired && std::holds_alternative<std::monostate>(second)) continue;
                    const TExprId args[]{arena.Literal(narrow, first), arena.Literal(wide, second)};
                    const auto result = arena.Coalesce(args);
                    const auto expected = std::holds_alternative<std::monostate>(first) ? second : first;
                    UNIT_ASSERT(arena[result].Type == wide);
                    UNIT_ASSERT(Evaluate(arena, result).Data == expected);
                    const TExprId roots[]{result};
                    const auto folded = arena.FoldConstants(roots).front();
                    UNIT_ASSERT(arena[folded].Type == wide);
                    UNIT_ASSERT(Evaluate(arena, folded).Data == expected);
                }
            }
        }
    }

    Y_UNIT_TEST(IsNaNUsesScalarIeeeComparisonAndPreservesNulls) {
        for (const auto kind : {K::Float, K::Double}) for (const bool nullable : {false, true}) {
            TExpressionArena arena;
            const TType type{kind, nullable};
            for (const double value : {0.0, -0.0, 1.5, -1.5, std::numeric_limits<double>::infinity(),
                    -std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN(), -std::numeric_limits<double>::quiet_NaN()}) {
                const auto literal = arena.Literal(type, value);
                const TExprId args[]{literal};
                const auto result = arena.Call("Math::IsNaN", args);
                UNIT_ASSERT(arena[result].Type == (TType{K::Bool, nullable}));
                UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, result).Data), std::isnan(value));
                const TExprId roots[]{result};
                const auto folded = arena.FoldConstants(roots).front();
                UNIT_ASSERT(arena[folded].Type == arena[result].Type);
                UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, folded).Data), std::isnan(value));
            }
            if (nullable) {
                const TExprId args[]{arena.Literal(type, std::monostate{})};
                UNIT_ASSERT(Evaluate(arena, arena.Call("math::isnan", args)).IsNull());
            }
        }
        TExpressionArena arena;
        for (const auto type : {TType{K::Int64}, TType{K::Uint64}}) {
            const TExprId args[]{arena.Literal(type, type.IsSigned() ? TScalar{std::numeric_limits<std::int64_t>::min()} :
                TScalar{std::numeric_limits<std::uint64_t>::max()})};
            UNIT_ASSERT(!std::get<bool>(Evaluate(arena, arena.Call("math::isnan", args)).Data));
        }
        const TExprId bareNull[]{arena.Null()};
        UNIT_ASSERT_EXCEPTION(arena.Call("math::isnan", bareNull), TUnsupportedFeature);
    }

    Y_UNIT_TEST(UnicodeCasePreservesCodepointsNullsAndTypes) {
        for (const auto& [input, upper, lower] : std::initializer_list<std::tuple<std::string, std::string, std::string>>{
                {"", "", ""}, {"AbC", "ABC", "abc"}, {"École", "ÉCOLE", "école"},
                {"Привет", "ПРИВЕТ", "привет"}, {"σςΣ", "ΣΣΣ", "σςσ"}, {"ıIİi", "IIİI", "ıiii"},
                {"ſS", "SS", "ſs"}, {"ȿⱾ", "ⱾⱾ", "ȿȿ"}, {"e\u0301", "E\u0301", "e\u0301"},
                {"\U00010428\U00010400😀", "\U00010400\U00010400😀", "\U00010428\U00010428😀"},
                {std::string("a\0B", 3), std::string("A\0B", 3), std::string("a\0b", 3)}}) {
            for (const bool nullable : {false, true}) {
                TExpressionArena arena;
                const TType type{K::Utf8, nullable};
                const auto value = arena.Literal(type, input);
                for (const auto& [op, expected] : {std::pair{O::UnicodeUpper, upper}, {O::UnicodeLower, lower}}) {
                    const auto result = arena.Unary(op, value);
                    UNIT_ASSERT(arena[result].Type == type);
                    UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), expected);
                    const TExprId roots[]{result};
                    const auto folded = arena.FoldConstants(roots).front();
                    UNIT_ASSERT(arena[folded].Type == type);
                    UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), expected);
                    if (nullable) UNIT_ASSERT(Evaluate(arena, arena.Unary(op, arena.Literal(type, std::monostate{}))).IsNull());
                }
            }
        }
    }

    Y_UNIT_TEST(IntegerComparisonsPreserveOperandTypesAndValues) {
        struct TInput { TType Type; std::vector<TScalar> Values; };
        std::vector<TInput> inputs;
        for (const auto kind : {K::Int8, K::Uint8, K::Int16, K::Uint16, K::Int32, K::Uint32, K::Int64, K::Uint64}) {
            const TType type{kind};
            const unsigned width = kind == K::Int8 || kind == K::Uint8 ? 8 : kind == K::Int16 || kind == K::Uint16 ? 16 :
                kind == K::Int32 || kind == K::Uint32 ? 32 : 64;
            if (type.IsSigned()) {
                const auto high = static_cast<std::int64_t>((std::uint64_t{1} << (width - 1)) - 1);
                inputs.push_back({type, {std::monostate{}, -high - 1, std::int64_t{-1}, std::int64_t{0}, std::int64_t{1}, high}});
            } else {
                const auto high = width == 64 ? std::numeric_limits<std::uint64_t>::max() : (std::uint64_t{1} << width) - 1;
                inputs.push_back({type, {std::monostate{}, std::uint64_t{0}, std::uint64_t{1}, high / 2, high / 2 + 1, high}});
            }
        }
        auto integer = [](const TScalar& value) -> __int128 {
            if (const auto* signedValue = std::get_if<std::int64_t>(&value)) return *signedValue;
            return std::get<std::uint64_t>(value);
        };
        for (const auto& a : inputs) for (const auto& b : inputs) for (const bool nullableA : {false, true}) for (const bool nullableB : {false, true}) {
            TExpressionArena arena;
            const TType leftType = nullableA ? a.Type.Optional() : a.Type, rightType = nullableB ? b.Type.Optional() : b.Type;
            const auto left = arena.Column(0, leftType), right = arena.Column(1, rightType);
            for (const auto op : {O::Equal, O::NotEqual, O::Less, O::LessEqual, O::Greater, O::GreaterEqual}) {
                const auto root = arena.Binary(op, left, right);
                UNIT_ASSERT(arena[root].Arguments == (std::vector<TExprId>{left, right}));
                UNIT_ASSERT(arena[root].Type == (TType{K::Bool, nullableA || nullableB}));
                for (const auto& x : a.Values) for (const auto& y : b.Values) {
                    if ((!nullableA && std::holds_alternative<std::monostate>(x)) || (!nullableB && std::holds_alternative<std::monostate>(y))) continue;
                    TEvaluationContext context;
                    context.Column = [&](auto id) { return id == 0 ? TValue{leftType, x} : TValue{rightType, y}; };
                    const auto value = Evaluate(arena, root, context);
                    if (std::holds_alternative<std::monostate>(x) || std::holds_alternative<std::monostate>(y)) UNIT_ASSERT(value.IsNull());
                    else {
                        const auto av = integer(x), bv = integer(y);
                        const bool expected = op == O::Equal ? av == bv : op == O::NotEqual ? av != bv : op == O::Less ? av < bv :
                            op == O::LessEqual ? av <= bv : op == O::Greater ? av > bv : av >= bv;
                        UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(value.Data), expected);
                    }
                    const auto constant = arena.RewriteColumns(root, [&](auto, auto id, auto type, auto pos) {
                        return arena.Literal(type, id == 0 ? x : y, pos);
                    });
                    const TExprId roots[]{constant};
                    const auto folded = arena.FoldConstants(roots).front();
                    UNIT_ASSERT(arena[folded].Type == arena[root].Type);
                    UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(value));
                }
            }
        }
    }

    Y_UNIT_TEST(CalendarClampingRangesAndReconstruction) {
        struct TCase { const char* Input; i32 Months; i32 Years; const char* MonthResult; const char* YearResult; };
        for (const auto& test : {
                TCase{"2020-02-29T12:34:56.123456Z", 12, 1, "2021-02-28T12:34:56.123456Z", "2021-02-28T12:34:56.123456Z"},
                TCase{"2020-01-31T23:59:59.999999Z", 1, -1, "2020-02-29T23:59:59.999999Z", "2019-01-31T23:59:59.999999Z"},
                TCase{"1970-01-01T00:00:00Z", -1, -1, nullptr, nullptr},
                TCase{"2105-12-31T23:59:59.999999Z", 1, 1, nullptr, nullptr},
                TCase{"2000-02-29T01:02:03Z", -12, 100, "1999-02-28T01:02:03Z", "2100-02-28T01:02:03Z"},
                TCase{"2000-02-29T01:02:03Z", 49152, 4096, "2000-02-29T01:02:03Z", "2000-02-29T01:02:03Z"}}) {
            TExpressionArena arena;
            auto timestamp = [&](const char* text) {
                const TExprId args[]{arena.Literal({K::String}, std::string{text})};
                return arena.Call("Timestamp", args);
            };
            const auto input = timestamp(test.Input);
            for (const bool years : {false, true}) {
                const TExprId args[]{input, arena.Literal({K::Int32}, std::int64_t{years ? test.Years : test.Months})};
                const auto shifted = arena.Call(years ? "DateTime::ShiftYears" : "DateTime::ShiftMonths", args);
                const TExprId convert[]{shifted};
                const auto result = arena.Call("DateTime::MakeTimestamp", convert);
                UNIT_ASSERT(arena[result].Type == (TType{K::Timestamp, true}));
                const auto expected = years ? test.YearResult : test.MonthResult;
                if (expected) UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), FormatValue(Evaluate(arena, timestamp(expected))));
                else UNIT_ASSERT(Evaluate(arena, result).IsNull());
                const TExprId roots[]{result};
                const auto folded = arena.FoldConstants(roots).front();
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(Evaluate(arena, result)));
                TExpressionArena copy;
                const auto imported = copy.Import(arena, result, [](auto, auto, auto) -> TExprId { UNIT_FAIL("unexpected column"); return 0; });
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copy, imported)), FormatValue(Evaluate(arena, result)));
            }
        }
    }

    Y_UNIT_TEST(RequiredCoalesceRetainsCoercionAndSkipsUnreachableValues) {
        TExpressionArena arena;
        const auto input = arena.Column(0, {K::Int32});
        const auto rejected = arena.Ensure(Integer(arena, 0), arena.Literal({K::Bool}, false), "unreachable");
        const TExprId alternatives[]{input, rejected};
        const auto result = arena.Coalesce(alternatives);
        UNIT_ASSERT(arena[result].Op == O::Convert);
        UNIT_ASSERT(arena[result].Type == TType{K::Int64});
        TEvaluationContext context;
        context.Column = [](auto id) {
            UNIT_ASSERT_VALUES_EQUAL(id, 0);
            return TValue{{K::Int32}, std::int64_t{7}};
        };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result, context)), "7");
        const TExprId invalid[]{input, arena.Literal({K::String}, std::string{"invalid"})};
        UNIT_ASSERT_EXCEPTION(arena.Coalesce(invalid), TFrontendError);
    }

    Y_UNIT_TEST(CoalesceNullabilitySurvivesReconstruction) {
        TExpressionArena arena;
        const auto optional = arena.Column(0, {K::Uint32, true});
        const auto fallback = arena.Column(1, {K::Uint32});
        const TExprId alternatives[]{optional, fallback};
        const auto result = arena.Coalesce(alternatives);
        UNIT_ASSERT(arena[result].Type == TType{K::Uint32});
        TExpressionArena imported;
        const auto copied = imported.Import(arena, result, [&](auto id, auto type, auto pos) { return imported.Column(id + 10, type, pos); });
        UNIT_ASSERT(imported[copied].Type == TType{K::Uint32});
        const auto nullable = arena.RewriteColumns(result, [&](auto, auto id, auto type, auto pos) { return arena.Column(id, type.Optional(), pos); });
        UNIT_ASSERT(arena[nullable].Type == (TType{K::Uint32, true}));
        const auto constants = arena.RewriteColumns(result, [&](auto, auto id, auto type, auto pos) {
            return arena.Literal(type, id == 0 ? TScalar{std::monostate{}} : TScalar{std::uint64_t{7}}, pos);
        });
        const TExprId roots[]{constants};
        const auto folded = arena.FoldConstants(roots).front();
        UNIT_ASSERT(arena[folded].Type == TType{K::Uint32});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), "7");
    }

    Y_UNIT_TEST(StableKeysPreserveNullAndFloatingEquivalence) {
        TExpressionArena arena;
        const auto column = arena.Column(7, {K::Double, true});
        const auto key = arena.Unary(O::StablePickle, column);
        UNIT_ASSERT((arena[key].Type == TType{K::String}));
        auto encode = [&](TScalar value) {
            TEvaluationContext context;
            context.Column = [&](TColumnId id) {
                UNIT_ASSERT_VALUES_EQUAL(id, 7);
                return TValue{{K::Double, true}, value};
            };
            return std::get<std::string>(Evaluate(arena, key, context).Data);
        };
        UNIT_ASSERT_VALUES_EQUAL(encode(0.0), encode(-0.0));
        UNIT_ASSERT_VALUES_EQUAL(encode(std::numeric_limits<double>::quiet_NaN()), encode(-std::numeric_limits<double>::quiet_NaN()));
        UNIT_ASSERT(encode(std::monostate{}) != encode(0.0));
        const auto renamed = arena.RewriteColumns(key, [&](auto, auto, auto, auto pos) {
            return arena.Column(8, {K::Double}, pos);
        });
        UNIT_ASSERT(arena[renamed].Op == O::StablePickle);
        UNIT_ASSERT((arena[renamed].Type == TType{K::String}));
        const TExprId roots[]{key};
        UNIT_ASSERT_VALUES_EQUAL(arena.FoldConstants(roots).front(), key);
    }

    Y_UNIT_TEST(TypeConstructionIsLocalAndExplicit) {
        TExpressionArena arena;
        auto a = arena.Literal({K::Int32}, std::int64_t{4});
        auto b = arena.Literal({K::Uint32}, std::uint64_t{5});
        auto sum = arena.Binary(O::Add, a, b);
        UNIT_ASSERT(arena[sum].Type == TType{K::Int32});
        for (auto arg : arena[sum].Arguments) {
            UNIT_ASSERT(arena[arg].Type == TType{K::Int32});
            UNIT_ASSERT(arg < sum);
        }
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, sum)), "9");
        UNIT_ASSERT((CommonType({K::Int64}, {K::Uint64}) == TType{K::Int64}));
        UNIT_ASSERT((CommonType({K::Decimal, false, 8, 2}, {K::Int64}) == TType{K::Decimal, false, 22, 2}));
        UNIT_ASSERT((CommonType({K::Decimal, false, 8, 2}, {K::Uint64}) == TType{K::Decimal, false, 22, 2}));
    }

    Y_UNIT_TEST(NumericPromotionUsesSqlRankAndWrappingConversion) {
        const std::array rank{K::Uint8, K::Int8, K::Uint16, K::Int16, K::Uint32, K::Int32, K::Uint64, K::Int64, K::Float, K::Double};
        for (std::size_t i = 0; i < rank.size(); ++i) for (std::size_t j = 0; j < rank.size(); ++j)
            for (const bool a : {false, true}) for (const bool b : {false, true})
                UNIT_ASSERT((CommonType({rank[i], a}, {rank[j], b}) == TType{rank[std::max(i, j)], a || b}));
        struct TCase { K From, To; TScalar Input, Expected; };
        for (const auto& test : std::initializer_list<TCase>{
                {K::Uint8, K::Int8, std::uint64_t{255}, std::int64_t{-1}},
                {K::Int8, K::Uint16, std::int64_t{-1}, std::uint64_t{65535}},
                {K::Uint16, K::Int16, std::uint64_t{32768}, std::int64_t{-32768}},
                {K::Int16, K::Uint32, std::int64_t{-32768}, std::uint64_t{4294934528}},
                {K::Uint32, K::Int32, std::uint64_t{4294967295}, std::int64_t{-1}},
                {K::Int32, K::Uint64, std::int64_t{-2147483648}, std::uint64_t{18446744071562067968ULL}},
                {K::Uint64, K::Int64, std::uint64_t{18446744073709551615ULL}, std::int64_t{-1}},
                {K::Uint64, K::Int64, std::uint64_t{9223372036854775808ULL}, std::numeric_limits<std::int64_t>::lowest()}}) {
            for (const bool nullable : {false, true}) {
                TExpressionArena arena;
                const TType from{test.From, nullable}, to{test.To, nullable};
                const auto input = arena.Column(0, from), converted = arena.Convert(input, to);
                TEvaluationContext context; context.Column = [&](auto) { return TValue{from, test.Input}; };
                UNIT_ASSERT(Evaluate(arena, converted, context).Data == test.Expected);
                UNIT_ASSERT(Evaluate(arena, arena.Cast(input, to), context).IsNull());
                const TExprId literal[]{arena.Convert(arena.Literal(from, test.Input), to)};
                UNIT_ASSERT(arena[arena.FoldConstants(literal).front()].Value == test.Expected);
                if (nullable) {
                    context.Column = [&](auto) { return TValue{from, std::monostate{}}; };
                    UNIT_ASSERT(Evaluate(arena, converted, context).IsNull());
                }
            }
        }
        TExpressionArena arena;
        const auto large = arena.Literal({K::Uint64}, std::uint64_t{9223372586610589697ULL});
        const auto sum = arena.Binary(O::Add, large, arena.Literal({K::Float}, 0.0));
        UNIT_ASSERT(arena[sum].Type == TType{K::Float});
        UNIT_ASSERT_VALUES_EQUAL(std::get<double>(Evaluate(arena, sum).Data), double(float(9223372586610589697ULL)));
    }

    Y_UNIT_TEST(ThreeValuedBooleanTables) {
        TExpressionArena arena;
        const std::array<TExprId, 3> inputs{
            arena.Literal({K::Bool}, false), arena.Literal({K::Bool}, true), arena.Cast(arena.Null(), {K::Bool})};
        const std::array<std::array<const char*, 3>, 3> andExpected{{
            {{"false", "false", "false"}}, {{"false", "true", "NULL"}}, {{"false", "NULL", "NULL"}}}};
        const std::array<std::array<const char*, 3>, 3> orExpected{{
            {{"false", "true", "NULL"}}, {{"true", "true", "true"}}, {{"NULL", "true", "NULL"}}}};
        for (std::size_t a = 0; a < 3; ++a) {
            for (std::size_t b = 0; b < 3; ++b) {
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::And, inputs[a], inputs[b]))), andExpected[a][b]);
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Or, inputs[a], inputs[b]))), orExpected[a][b]);
            }
        }
    }

    Y_UNIT_TEST(NeutralBooleanOperandsPreserveTypesAndEvaluation) {
        TExpressionArena arena;
        const auto no = arena.Literal({K::Bool}, false), yes = arena.Literal({K::Bool}, true);
        for (const bool nullable : {false, true}) {
            const auto column = arena.Column(1, {K::Bool, nullable});
            UNIT_ASSERT_VALUES_EQUAL(arena.Binary(O::Or, no, column), column);
            UNIT_ASSERT_VALUES_EQUAL(arena.Binary(O::Or, column, no), column);
            UNIT_ASSERT_VALUES_EQUAL(arena.Binary(O::And, yes, column), column);
            UNIT_ASSERT_VALUES_EQUAL(arena.Binary(O::And, column, yes), column);
        }
        const auto nullableNo = arena.Literal({K::Bool, true}, false);
        const auto required = arena.Column(1, {K::Bool});
        const auto lifted = arena.Binary(O::Or, nullableNo, required);
        UNIT_ASSERT((arena[lifted].Type == TType{K::Bool, true}));
        UNIT_ASSERT(arena[lifted].Op == O::Convert);
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, lifted), TFrontendError);
    }

    Y_UNIT_TEST(LazyBranchesDoNotReadMissingBindings) {
        TExpressionArena arena;
        auto missing = arena.Parameter("missing", {K::Int64});
        auto value = Integer(arena, 42);
        auto condition = arena.Literal({K::Bool}, true);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.If(condition, value, missing))), "42");
        const std::array<TExprId, 2> arguments{value, missing};
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Coalesce(arguments))), "42");
        auto absentBool = arena.Parameter("missing_bool", {K::Bool});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Or, condition, absentBool))), "true");
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, missing), TFrontendError);
    }

    Y_UNIT_TEST(TwoArgumentIfProducesOptionalAndSkipsAbsentBranch) {
        TExpressionArena arena;
        const auto value = Integer(arena, 42);
        const TExprId present[]{arena.Literal({K::Bool}, true), value};
        const auto result = arena.Call("If", present);
        UNIT_ASSERT((arena[result].Type == TType{K::Int64, true}));
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), "42");
        const auto missing = arena.Parameter("missing", {K::Int64});
        for (const auto condition : {arena.Literal({K::Bool}, false), arena.Null()}) {
            const TExprId absent[]{condition, missing};
            UNIT_ASSERT(Evaluate(arena, arena.Call("If", absent)).IsNull());
        }
    }

    Y_UNIT_TEST(CastsAreCheckedAndNullPropagates) {
        TExpressionArena arena;
        auto text = arena.Literal({K::String}, std::string{"123"});
        auto converted = arena.Cast(text, {K::Int16});
        UNIT_ASSERT((arena[converted].Type == TType{K::Int16, true}));
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, converted)), "123");
        auto bad = arena.Literal({K::String}, std::string{"bad"});
        UNIT_ASSERT(Evaluate(arena, arena.Cast(bad, {K::Int64})).IsNull());
        UNIT_ASSERT(Evaluate(arena, arena.Cast(Integer(arena, 256), {K::Uint8})).IsNull());
        for (const auto* invalid : {"1.5", " 1", "1 ", "+", "1e2"}) {
            auto input = arena.Literal({K::String}, std::string{invalid});
            UNIT_ASSERT(Evaluate(arena, arena.Cast(input, {K::Int64})).IsNull());
        }
        auto fractional = arena.Literal({K::Double}, 127.5);
        UNIT_ASSERT(Evaluate(arena, arena.Cast(fractional, {K::Int8})).IsNull());
        auto negativeFractional = arena.Literal({K::Double}, -0.5);
        UNIT_ASSERT(Evaluate(arena, arena.Cast(negativeFractional, {K::Uint64})).IsNull());
        auto null = arena.Cast(arena.Null(), {K::Int64});
        UNIT_ASSERT(Evaluate(arena, arena.Binary(O::Equal, null, Integer(arena, 1))).IsNull());
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Unary(O::IsNull, null))), "true");
    }

    Y_UNIT_TEST(IntegerOverflowHasDefinedWrappingSemantics) {
        TExpressionArena arena;
        auto max = Integer(arena, std::numeric_limits<std::int64_t>::max());
        auto min = Integer(arena, std::numeric_limits<std::int64_t>::min());
        auto one = Integer(arena, 1);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Add, max, one))), "-9223372036854775808");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Subtract, min, one))), "9223372036854775807");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Unary(O::Negate, min))), "-9223372036854775808");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Multiply, max, max))), "1");
        UNIT_ASSERT(Evaluate(arena, arena.Binary(O::Divide, min, Integer(arena, -1))).IsNull());
        UNIT_ASSERT(Evaluate(arena, arena.Binary(O::Modulo, min, Integer(arena, -1))).IsNull());
        UNIT_ASSERT(Evaluate(arena, arena.Binary(O::Divide, one, Integer(arena, 0))).IsNull());
        auto narrow = arena.Literal({K::Int8}, std::int64_t{127});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Add, narrow, arena.Literal({K::Int8}, std::int64_t{1})))), "-128");
    }

    Y_UNIT_TEST(TextCastsFoldAndRuntimeCastsKeepOptionalResults) {
        TExpressionArena arena;
        auto text = arena.Literal({K::String}, std::string{"123"});
        auto converted = arena.Cast(text, {K::Int32});
        UNIT_ASSERT(arena[converted].Op == O::Literal);
        UNIT_ASSERT((arena[converted].Type == TType{K::Int32, true}));
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, converted)), "123");
        auto invalid = arena.Cast(arena.Literal({K::String}, std::string{"invalid"}), {K::Date});
        UNIT_ASSERT(arena[invalid].Op == O::Literal);
        UNIT_ASSERT(Evaluate(arena, invalid).IsNull());
        auto date = arena.Cast(arena.Literal({K::String}, std::string{"1970-01-02"}), {K::Date});
        UNIT_ASSERT(arena[date].Op == O::Literal);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, date)), "1");

        auto dynamic = arena.Parameter("text", {K::String});
        TEvaluationContext context;
        const auto runtimeInteger = arena.Cast(dynamic, {K::Int32}, {12, 3, 5});
        UNIT_ASSERT(arena[runtimeInteger].Op == O::Cast);
        UNIT_ASSERT(arena[runtimeInteger].Type == (TType{K::Int32, true}));
        UNIT_ASSERT_VALUES_EQUAL(arena[runtimeInteger].Position.Offset, 12);
        context.Parameter = [](std::string_view) { return TValue{{K::String}, std::string{"123"}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, runtimeInteger, context)), "123");
        context.Parameter = [](std::string_view) { return TValue{{K::String}, std::string{"1.5"}}; };
        UNIT_ASSERT(Evaluate(arena, runtimeInteger, context).IsNull());
        for (const auto type : {TType{K::Datetime}, TType{K::Timestamp}})
            UNIT_ASSERT_EXCEPTION(arena.Cast(dynamic, type), TUnsupportedFeature);
        context.Parameter = [](std::string_view) { return TValue{{K::String}, std::string{"\xD0\xAF"}}; };
        auto utf8 = arena.Cast(dynamic, {K::Utf8});
        UNIT_ASSERT(arena[utf8].Op == O::Cast);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, utf8, context)), std::string{"\xD0\xAF"});
        const auto runtimeDate = arena.Cast(dynamic, {K::Date});
        context.Parameter = [](std::string_view) { return TValue{{K::String}, std::string{"1998-3-18"}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, runtimeDate, context)), "10303");
        context.Parameter = [](std::string_view) { return TValue{{K::String}, std::string{"1998-2-30"}}; };
        UNIT_ASSERT(Evaluate(arena, runtimeDate, context).IsNull());
    }

    Y_UNIT_TEST(NestedOptionalCastsRetainInnerNullsThroughCopiesAndFolding) {
        TExpressionArena arena;
        const auto type = ParseType("String??");
        UNIT_ASSERT_VALUES_EQUAL(type.OptionalDepth, 2);
        UNIT_ASSERT_VALUES_EQUAL(type.ToString(), "String??");
        const auto value = arena.Column(1, {K::Int32, true});
        const auto cast = arena.Cast(value, type);
        UNIT_ASSERT(arena[cast].Type == type);
        for (bool present : {false, true}) {
            TEvaluationContext context;
            context.Column = [&](TColumnId) { return TValue{{K::Int32, true}, present ? TScalar{std::int64_t{42}} : TScalar{}}; };
            const auto result = Evaluate(arena, cast, context);
            UNIT_ASSERT(!result.IsNull());
            const auto& inner = *std::get<TOptionalValue>(result.Data);
            UNIT_ASSERT_VALUES_EQUAL(inner.IsNull(), !present);
            if (present) UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(inner.Data), "42");
            const auto unwrapped = arena.Unary(O::Unwrap, cast);
            UNIT_ASSERT(Evaluate(arena, unwrapped, context).IsNull() == !present);
            const auto constant = arena.RewriteColumns(cast, [&](TExprId, TColumnId, TType type, TSourcePosition pos) {
                return arena.Literal(type, present ? TScalar{std::int64_t{42}} : TScalar{}, pos);
            });
            const TExprId roots[]{constant};
            const auto folded = arena.FoldConstants(roots).front();
            UNIT_ASSERT(arena[folded].Op == O::Literal);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), present ? "Just(42)" : "Just(NULL)");
            TExpressionArena copy;
            const auto imported = copy.Import(arena, folded, {});
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copy, imported)), FormatValue(result));
        }
        const auto outerNull = arena.Literal(type, {});
        UNIT_ASSERT(Evaluate(arena, outerNull).IsNull());
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, arena.Unary(O::Unwrap, outerNull)), TFrontendError);
        UNIT_ASSERT_VALUES_EQUAL(sizeof(TType), 8);
    }

    Y_UNIT_TEST(DecimalIntegerDivisionPreservesScaleAndRuntimeRounding) {
        const TType type{K::Decimal, false, 3, 2};
        for (const auto& [numerator, divisor, expected] : std::initializer_list<std::tuple<i64, i64, const char*>>{
                {125, 2, "0.62"}, {127, 2, "0.64"}, {-125, 2, "-0.62"}, {-127, 2, "-0.64"},
                {2, 3, "0.01"}, {-2, 3, "-0.01"}, {2, -3, "0.00"}, {-2, -3, "0.00"},
                {125, -2, "-0.62"}, {127, -2, "-0.64"}, {-125, -2, "0.62"}, {-127, -2, "0.64"},
                {0, 0, "nan"}, {1, 0, "inf"}, {-1, 0, "-inf"}}) {
            TExpressionArena arena;
            const auto result = arena.Binary(O::Divide, arena.Literal(type, TDecimal{numerator}),
                arena.Literal({K::Int64}, std::int64_t{divisor}));
            UNIT_ASSERT(arena[result].Type == type);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), expected);
            const TExprId roots[]{result};
            const auto folded = arena.FoldConstants(roots).front();
            UNIT_ASSERT(arena[folded].Type == type);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), expected);
        }
    }

    Y_UNIT_TEST(DecimalScaleAndOverflowAreExplicit) {
        TExpressionArena arena;
        auto a = arena.Literal({K::Decimal, false, 8, 2}, TDecimal{123});
        auto b = arena.Literal({K::Decimal, false, 8, 2}, TDecimal{200});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Multiply, a, b))), "2.46");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Divide, a, b))), "0.62");
        auto string = arena.Literal({K::String}, std::string{"-12.345"});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(string, {K::Decimal, false, 8, 2}))), "-12.34");
        UNIT_ASSERT_EXCEPTION(arena.Literal({K::Decimal, false, 2, 1}, TDecimal{TDecimal::NaN() + 1}), TFrontendError);
        UNIT_ASSERT_EXCEPTION((CommonType({K::Decimal, false, 8, 2}, {K::Double})), TFrontendError);
        auto rounding = arena.Literal({K::String}, std::string{"12.355"});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(rounding, {K::Decimal, false, 8, 2}))), "12.36");
        auto roundedInteger = arena.Literal({K::Decimal, false, 8, 2}, TDecimal{150});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(roundedInteger, {K::Int64}))), "2");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(roundedInteger, {K::String}))), "1.5");
        auto carry = arena.Literal({K::Decimal, false, 2, 1}, TDecimal{99});
        UNIT_ASSERT(Evaluate(arena, arena.Cast(carry, {K::Int64})).IsNull());
        auto fractional = arena.Literal({K::Decimal, false, 2, 2}, TDecimal{99});
        UNIT_ASSERT_EXCEPTION(arena.Cast(fractional, {K::Int64}), TFrontendError);
        __int128 oneCoefficient = 1;
        for (unsigned i = 0; i < 30; ++i) oneCoefficient *= 10;
        auto one = arena.Literal({K::Decimal, false, 35, 30}, TDecimal{oneCoefficient});
        UNIT_ASSERT(std::get<TDecimal>(Evaluate(arena, arena.Binary(O::Multiply, one, one)).Data).Coefficient == oneCoefficient);
        UNIT_ASSERT(std::get<TDecimal>(Evaluate(arena, arena.Binary(O::Divide, one, one)).Data).Coefficient == oneCoefficient);
        auto huge = arena.Literal({K::Decimal, false, 2, 0}, TDecimal{99});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Add, huge, huge))), "inf");
        UNIT_ASSERT_EXCEPTION(arena.Cast(arena.Literal({K::Double}, 1.2), {K::Decimal, false, 8, 2}), TFrontendError);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(arena.Literal({K::String}, std::string{"1e2"}), {K::Decimal, false, 8, 2}))), "100.00");
    }

    Y_UNIT_TEST(ScalarFoldingRestoresSharedRuntimeIdentity) {
        TExpressionArena arena;
        const auto firstInput = arena.Column(7, {K::JsonDocument, true});
        const auto secondInput = arena.Column(7, {K::JsonDocument, true});
        const auto first = arena.JsonValue(firstInput, "$.a", {1, 1, 2});
        const auto second = arena.JsonValue(secondInput, "$.a", {12, 1, 13});
        const auto differentPath = arena.JsonValue(secondInput, "$.b");
        const auto differentColumn = arena.JsonValue(arena.Column(8, {K::JsonDocument, true}), "$.a");
        const auto root = arena.Binary(O::Concat, first, second);
        const TExprId roots[]{root, differentPath, differentColumn};
        const auto folded = arena.FoldConstants(roots);
        const auto& joined = arena[folded[0]];
        UNIT_ASSERT_VALUES_EQUAL(joined.Arguments[0], joined.Arguments[1]);
        UNIT_ASSERT(arena[joined.Arguments[0]].Op == O::JsonValue);
        UNIT_ASSERT(joined.Arguments[0] != folded[1] && joined.Arguments[0] != folded[2]);
        UNIT_ASSERT_VALUES_EQUAL(arena[root].Arguments[0], first);
        UNIT_ASSERT_VALUES_EQUAL(arena[root].Arguments[1], second);
        UNIT_ASSERT_VALUES_EQUAL(arena[second].Position.Column, 13);
        TEvaluationContext context;
        context.Column = [](TColumnId) { return TValue{{K::JsonDocument, true}, std::string("document")}; };
        unsigned calls = 0;
        context.JsonValue = [&](const TValue&, std::string_view path) {
            ++calls;
            UNIT_ASSERT_VALUES_EQUAL(path, "$.a");
            return TValue{{K::Utf8, true}, std::string("value")};
        };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded[0], context)), "valuevalue");
        UNIT_ASSERT_VALUES_EQUAL(calls, 1);
        UNIT_ASSERT(arena.FoldConstants(folded) == folded);
    }

    Y_UNIT_TEST(ScalarSharingPreservesLiteralBitsAndRuntimeGuards) {
        TExpressionArena arena;
        const auto plusZero = arena.Literal({K::Double}, 0.0);
        const auto minusZero = arena.Literal({K::Double}, -0.0);
        const auto required = arena.Column(7, {K::Int32});
        const auto optional = arena.Column(7, {K::Int32, true});
        const auto first = arena.Ensure(required, arena.Parameter("p", {K::Bool}), "first", {1, 1, 2});
        const auto second = arena.Ensure(arena.Column(7, {K::Int32}), arena.Parameter("p", {K::Bool}), "first", {12, 1, 13});
        const auto random1 = arena.Call("RandomNumber", {}), random2 = arena.Call("RandomNumber", {});
        const TExprId roots[]{plusZero, minusZero, required, optional, first, second, random1, random2};
        const auto folded = arena.FoldConstants(roots);
        UNIT_ASSERT(folded[0] != folded[1] && folded[2] != folded[3]);
        UNIT_ASSERT(folded[4] != folded[5] && folded[6] != folded[7]);
        UNIT_ASSERT_VALUES_EQUAL(arena[folded[4]].Position.Column, 2);
        UNIT_ASSERT_VALUES_EQUAL(arena[folded[5]].Position.Column, 13);
        const auto guarded = arena.If(arena.Parameter("take", {K::Bool}), first, required);
        const TExprId guardedRoots[]{guarded};
        const auto shared = arena.FoldConstants(guardedRoots).front();
        TEvaluationContext context;
        context.Column = [](TColumnId) { return TValue{{K::Int32}, std::int64_t{7}}; };
        context.Parameter = [](std::string_view) { return TValue{{K::Bool}, false}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, shared, context)), "7");
        context.Parameter = [](std::string_view name) { return TValue{{K::Bool}, name == "take"}; };
        UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, shared, context), TFrontendError, "first");
    }

    Y_UNIT_TEST(ConstantFoldingSharesWorkAndPreservesRoots) {
        TExpressionArena arena;
        const auto one = arena.Literal({K::Int32}, std::int64_t{1});
        auto root = one;
        for (unsigned i = 0; i < 1000; ++i) root = arena.Binary(O::Add, root, one);
        const auto before = arena.Size();
        const std::vector<TExprId> roots(100, root);
        const auto folded = arena.FoldConstants(roots);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), before + 1000);
        for (auto result : folded) {
            UNIT_ASSERT_VALUES_EQUAL(result, folded.front());
            UNIT_ASSERT(arena[result].Op == O::Literal);
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), "1001");
        }
        UNIT_ASSERT(arena[root].Op == O::Add);
        const auto after = arena.Size();
        UNIT_ASSERT(arena.FoldConstants(folded) == folded);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), after);
        const auto column = arena.Column(7, {K::Int32});
        const TExprId mixed[]{arena.Binary(O::Add, column, root)};
        const auto result = arena.FoldConstants(mixed).front();
        UNIT_ASSERT_VALUES_EQUAL(arena[result].Arguments[0], column);
        UNIT_ASSERT(arena[arena[result].Arguments[1]].Op == O::Literal);
        TEvaluationContext context;
        context.Column = [](TColumnId) { return TValue{{K::Int32}, std::int64_t{3}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result, context)), "1004");
    }

    Y_UNIT_TEST(ConstantFoldingPreservesRuntimeAssertionsAndTypes) {
        TExpressionArena arena;
        const auto yes = arena.Literal({K::Bool}, true), no = arena.Literal({K::Bool}, false);
        const auto seven = arena.Literal({K::Int32}, std::int64_t{7});
        const auto failed = arena.Ensure(seven, no, "cardinality violation");
        const auto selected = arena.If(no, failed, seven);
        const auto conditional = arena.If(arena.Parameter("p", {K::Bool}), failed, seven);
        const auto optional = arena.If(yes, seven, arena.Null());
        const TExprId roots[]{failed, selected, conditional, optional, arena.Ensure(seven, yes, "unused")};
        const auto folded = arena.FoldConstants(roots);
        UNIT_ASSERT(arena[folded[0]].Op == O::Ensure);
        UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, folded[0]), TFrontendError, "cardinality violation");
        UNIT_ASSERT_VALUES_EQUAL(folded[1], seven);
        UNIT_ASSERT_VALUES_EQUAL(folded[4], seven);
        UNIT_ASSERT(arena[folded[3]].Type == arena[optional].Type);
        UNIT_ASSERT(arena[folded[3]].Type.OptionalDepth);
        for (const bool condition : {false, true}) {
            TEvaluationContext context;
            context.Parameter = [&](auto) { return TValue{{K::Bool}, condition}; };
            if (condition) UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, folded[2], context), TFrontendError, "cardinality violation");
            else UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded[2], context)), "7");
        }
        UNIT_ASSERT_EXCEPTION(arena.Ensure(seven, arena.Convert(yes, {K::Bool, true}), "bad predicate"), TFrontendError);
    }

    Y_UNIT_TEST(UnwrapMessagesAreLazyAndSurviveRewrites) {
        TExpressionArena arena;
        const auto value = arena.Column(1, {K::Int32, true});
        const auto message = arena.Column(2, {K::Utf8});
        const TExprId args[]{value, message};
        const auto root = arena.Call("Unwrap", args);
        const auto copied = arena.RewriteColumns(root, [&](TExprId, TColumnId id, TType type, TSourcePosition pos) {
            return arena.Column(id + 10, type, pos);
        });
        for (const auto expr : {root, copied}) for (const bool empty : {false, true}) {
            unsigned messages = 0;
            TEvaluationContext context;
            context.Column = [&](TColumnId id) -> TValue {
                if (id == 1 || id == 11) return {{K::Int32, true}, empty ? TScalar{std::monostate{}} : TScalar{std::int64_t{7}}};
                ++messages;
                return {{K::Utf8}, std::string("missing value")};
            };
            if (empty) UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, expr, context), TFrontendError, "missing value");
            else UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, expr, context)), "7");
            UNIT_ASSERT_VALUES_EQUAL(messages, empty ? 1 : 0);
        }
        const auto required = arena.Literal({K::Int32}, std::int64_t{9});
        const TExprId ignored[]{required, required};
        UNIT_ASSERT_VALUES_EQUAL(arena.Call("unwrap", ignored), required);
        const TExprId untyped[]{arena.Null()};
        UNIT_ASSERT_EXCEPTION(arena.Call("unwrap", untyped), TUnsupportedFeature);
        const TExprId invalid[]{value, required};
        UNIT_ASSERT_EXCEPTION(arena.Call("unwrap", invalid), TFrontendError);
    }

    Y_UNIT_TEST(ConstantFoldingBoundsStringMaterialization) {
        TExpressionArena arena;
        const auto text = arena.Literal({K::String}, std::string(8, 'x'));
        const auto pair = arena.Binary(O::Concat, text, text);
        const TExprId roots[]{arena.Binary(O::Concat, pair, pair)};
        const auto folded = arena.FoldConstants(roots, 16).front();
        UNIT_ASSERT(arena[folded].Op == O::Concat);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), std::string(32, 'x'));
        const auto number = arena.Literal({K::Int32}, std::int64_t{12345});
        const TExprId cast[]{arena.Cast(number, {K::String})};
        const auto retained = arena.FoldConstants(cast, 0).front();
        UNIT_ASSERT_VALUES_EQUAL(retained, cast[0]);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, retained)), "12345");
    }

    Y_UNIT_TEST(SharedSubexpressionsAreEvaluatedOnce) {
        TExpressionArena arena;
        auto column = arena.Column(7, {K::Int64});
        auto expression = arena.Binary(O::Add, column, column);
        unsigned reads = 0;
        TEvaluationContext context;
        context.Column = [&](TColumnId id) {
            UNIT_ASSERT_VALUES_EQUAL(id, 7);
            ++reads;
            return TValue{{K::Int64}, std::int64_t{3}};
        };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, expression, context)), "6");
        UNIT_ASSERT_VALUES_EQUAL(reads, 1);
    }

    Y_UNIT_TEST(TemporalLiteralsAndArithmetic) {
        TExpressionArena arena;
        auto temporal = [&](std::string text, K kind) {
            return arena.Cast(arena.Literal({K::String}, std::move(text)), {kind});
        };
        auto epoch = temporal("1970-01-01", K::Date);
        auto nextDay = temporal("1970-01-02", K::Date);
        auto day = temporal("P1D", K::Interval);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, epoch)), "0");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Add, epoch, day))), "1");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Binary(O::Subtract, nextDay, epoch))), "86400000000");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(nextDay, {K::Timestamp}))), "86400000000");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(nextDay, {K::Datetime}))), "86400");
        auto lateNextDay = temporal("1970-01-02T12:34:56.123Z", K::Timestamp);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(lateNextDay, {K::Date}))), "1");
        UNIT_ASSERT(Evaluate(arena, arena.Binary(O::Subtract, epoch, day)).IsNull());
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, temporal("1970-01-01T00:00:01.25Z", K::Timestamp))), "1250000");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, temporal("1970-01-01T01:00:00Z", K::Datetime))), "3600");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, temporal("-PT1.25S", K::Interval))), "-1250000");
        UNIT_ASSERT(!Evaluate(arena, temporal("2000-02-29", K::Date)).IsNull());
        UNIT_ASSERT(Evaluate(arena, temporal("2001-02-29", K::Date)).IsNull());
        UNIT_ASSERT(Evaluate(arena, temporal("1969-12-31", K::Date)).IsNull());
        UNIT_ASSERT(Evaluate(arena, temporal("P1.0D", K::Interval)).IsNull());
        UNIT_ASSERT(Evaluate(arena, temporal("PT1M1H", K::Interval)).IsNull());
    }

    Y_UNIT_TEST(TemporalConstructorsRequireTimezoneAndPreserveMicroseconds) {
        TExpressionArena arena;
        auto constructor = [&](std::string_view name, std::string text) {
            const TExprId argument[]{arena.Literal({K::String}, std::move(text))};
            return arena.Call(name, argument);
        };
        auto date = constructor("Date", "1970-1-2");
        UNIT_ASSERT(!arena[date].Type.OptionalDepth);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, date)), "1");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena,
            constructor("Datetime", "1970-01-01T01:30:00+01:30"))), "0");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena,
            constructor("Datetime", "1970-1-1T0:0:0-1:30"))), "5400");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena,
            constructor("Timestamp", "1970-01-01T01:00:01.23456789+01:00"))), "1234567");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena,
            constructor("Timestamp", "1970-01-01T00:00:00.0000009Z"))), "0");
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(arena,
            constructor("Timestamp", "2105-12-31T23:59:59.999999Z")).Data),
            std::uint64_t{49'673} * 86'400'000'000ULL - 1);

        for (const auto* value : {"1970-01-01T00:00:00", "1970-01-01T00:00:00z", "1970-01-01T00:00:00+00",
            "1970-01-01T00:00:00+00:01", "2105-12-31T23:59:59-00:01", "2106-01-01T00:00:00+01:00"}) {
            UNIT_ASSERT_EXCEPTION(constructor("Datetime", value), TFrontendError);
            const auto text = arena.Literal({K::String}, std::string(value));
            UNIT_ASSERT(Evaluate(arena, arena.Cast(text, {K::Datetime})).IsNull());
        }
        UNIT_ASSERT_EXCEPTION(constructor("Timestamp", "1970-01-01T00:00:00.Z"), TFrontendError);
        UNIT_ASSERT_EXCEPTION(constructor("Datetime", "1970-01-01T00:00:00.1Z"), TFrontendError);
        UNIT_ASSERT_EXCEPTION(constructor("Date", "2106-01-01"), TFrontendError);
    }

    Y_UNIT_TEST(FloatingPointTextIsLocaleIndependent) {
        TExpressionArena arena;
        auto text = arena.Literal({K::String}, std::string{"1.25e2"});
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(text, {K::Double}))), "125");
        auto junk = arena.Literal({K::String}, std::string{"1.25suffix"});
        UNIT_ASSERT(Evaluate(arena, arena.Cast(junk, {K::Double})).IsNull());
    }

    Y_UNIT_TEST(Utf8CastsValidateAndFloatsRoundAtConstruction) {
        TExpressionArena arena;
        auto valid = arena.Literal({K::String}, std::string{"\xD0\xAF"});
        UNIT_ASSERT(!Evaluate(arena, arena.Cast(valid, {K::Utf8})).IsNull());
        auto invalid = arena.Literal({K::String}, std::string{"\xC0\xAF"});
        UNIT_ASSERT(Evaluate(arena, arena.Cast(invalid, {K::Utf8})).IsNull());
        UNIT_ASSERT_EXCEPTION(arena.Literal({K::Utf8}, std::string{"\xED\xA0\x80"}), TFrontendError);
        auto single = arena.Literal({K::Float}, 0.1);
        UNIT_ASSERT(std::get<double>(arena[single].Value) == static_cast<double>(0.1F));
        auto utf8 = arena.Literal({K::Utf8}, std::string{"\xD0\xAF"});
        const std::array<TExprId, 3> args{arena.Cast(utf8, {K::String}), Integer(arena, 1), Integer(arena, 1)};
        auto byteSlice = arena.Call("Substring", args);
        UNIT_ASSERT(arena[byteSlice].Type.Kind == K::String);
        UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(Evaluate(arena, byteSlice).Data), std::string{"\xAF"});
    }

    Y_UNIT_TEST(FloatingConversionsPreserveNonFiniteValues) {
        TExpressionArena arena;
        auto infinity = arena.Literal({K::Float}, std::numeric_limits<double>::infinity());
        auto wide = arena.Convert(infinity, {K::Double});
        UNIT_ASSERT(std::isinf(std::get<double>(Evaluate(arena, wide).Data)));
        auto nan = arena.Literal({K::Float}, std::numeric_limits<double>::quiet_NaN());
        UNIT_ASSERT(std::isnan(std::get<double>(Evaluate(arena, arena.Convert(nan, {K::Double})).Data)));
        UNIT_ASSERT(Evaluate(arena, arena.Cast(nan, {K::Bool})).IsNull());
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(infinity, {K::Bool}))), "true");
        auto denormal = arena.Literal({K::Float}, static_cast<double>(std::numeric_limits<float>::denorm_min()));
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, arena.Cast(denormal, {K::Bool}))), "false");
        auto text = arena.Literal({K::String}, std::string{"-Inf"});
        UNIT_ASSERT(std::isinf(std::get<double>(Evaluate(arena, arena.Cast(text, {K::Double})).Data)));
    }

    Y_UNIT_TEST(FloatingDivisionPreservesIeeeValuesAndNullability) {
        TExpressionArena arena;
        for (const auto kind : {K::Float, K::Double}) {
            const auto one = arena.Literal({kind}, 1.0);
            const auto zero = arena.Literal({kind}, 0.0);
            const auto infinity = arena.Binary(O::Divide, one, zero);
            UNIT_ASSERT(arena[infinity].Type == (TType{kind}));
            UNIT_ASSERT(std::isinf(std::get<double>(Evaluate(arena, infinity).Data)));
            UNIT_ASSERT(std::isnan(std::get<double>(Evaluate(arena, arena.Binary(O::Divide, zero, zero)).Data)));
            UNIT_ASSERT(std::isnan(std::get<double>(Evaluate(arena, arena.Binary(O::Modulo, one, zero)).Data)));
            const auto remainder = arena.Binary(O::Modulo, arena.Literal({kind}, -5.5), arena.Literal({kind}, 2.0));
            UNIT_ASSERT(arena[remainder].Type == TType{kind});
            UNIT_ASSERT_VALUES_EQUAL(std::get<double>(Evaluate(arena, remainder).Data), -1.5);
            const auto absent = arena.Cast(arena.Null(), {kind});
            const auto nullable = arena.Binary(O::Divide, one, absent);
            UNIT_ASSERT(arena[nullable].Type == (TType{kind, true}));
            UNIT_ASSERT(Evaluate(arena, nullable).IsNull());
            const auto converted = arena.Cast(Integer(arena, 7), {kind});
            UNIT_ASSERT(arena[converted].Type == (TType{kind}));
        }
    }

    Y_UNIT_TEST(UnsignedTemporalCastsPreserveTotality) {
        TExpressionArena arena;
        for (const auto source : {K::Uint8, K::Uint16, K::Uint32, K::Uint64}) {
            for (const auto target : {K::Date, K::Datetime, K::Timestamp}) {
                const bool total = source == K::Uint8 ||
                    (source == K::Uint16 && target != K::Date) ||
                    (source == K::Uint32 && target == K::Timestamp);
                for (const bool nullable : {false, true}) {
                    const auto result = arena.Cast(arena.Parameter("x", {source, nullable}), {target});
                    UNIT_ASSERT(arena[result].Type == (TType{target, nullable || !total}));
                }
            }
        }
    }

    Y_UNIT_TEST(SubstringDefaultsAndTypeAreResolvedAtConstruction) {
        TExpressionArena arena;
        const auto text = arena.Literal({K::String}, std::string{"abcdef"});
        const auto null = arena.Null();
        for (const auto& [args, expected] : std::vector<std::pair<std::vector<TExprId>, std::string>>{
            {{text, null}, "abcdef"}, {{text, Integer(arena, 2), null}, "cdef"},
            {{text, null, Integer(arena, 2)}, "ab"}, {{text, Integer(arena, 99)}, ""}}) {
            const auto result = arena.Call("Substring", args);
            UNIT_ASSERT(arena[result].Type == (TType{K::String}));
            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result)), expected);
        }
        const auto offset = arena.Parameter("start", {K::Uint32, true});
        const auto count = arena.Parameter("count", {K::Uint32, true});
        const TExprId args[]{text, offset, count};
        const auto result = arena.Call("Substring", args);
        UNIT_ASSERT(arena[result].Type == (TType{K::String}));
        TEvaluationContext context;
        context.Parameter = [](std::string_view) { return TValue{{K::Uint32, true}, {}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, result, context)), "abcdef");
        const TExprId invalid[]{text, Integer(arena, -1)};
        UNIT_ASSERT_EXCEPTION(arena.Call("Substring", invalid), TFrontendError);
        const TExprId wide[]{text, arena.Parameter("wide", {K::Uint64})};
        UNIT_ASSERT_EXCEPTION(arena.Call("Substring", wide), TUnsupportedFeature);
    }

    Y_UNIT_TEST(SubstitutionRetypesAndPreservesSharing) {
        TExpressionArena original;
        auto a = original.Column(10, {K::Int32});
        auto expression = original.Binary(O::Add, a, a);
        TExpressionArena target;
        unsigned bindings = 0;
        auto copied = target.Import(original, expression, [&](TColumnId id, TType, TSourcePosition pos) {
            UNIT_ASSERT_VALUES_EQUAL(id, 10);
            ++bindings;
            return target.Column(11, {K::Int64}, pos);
        });
        UNIT_ASSERT_VALUES_EQUAL(bindings, 1);
        UNIT_ASSERT(target[copied].Type == TType{K::Int64});
        UNIT_ASSERT_VALUES_EQUAL(target[copied].Arguments[0], target[copied].Arguments[1]);
    }

    Y_UNIT_TEST(ColumnRewriteRetainsUnchangedSubgraphsAndOriginalRoots) {
        TExpressionArena arena;
        auto left = arena.Column(1, {K::Int32});
        auto right = arena.Column(2, {K::Int32});
        auto one = arena.Literal({K::Int32}, std::int64_t{1});
        auto unchanged = arena.Binary(O::Add, right, one);
        auto branch = arena.Binary(O::Add, left, unchanged);
        auto root = arena.Binary(O::Add, branch, branch);
        const auto before = arena.Size();
        unsigned visits = 0;
        auto rewritten = arena.RewriteColumns(root, [&](TExprId original, TColumnId column, TType type, TSourcePosition position) {
            ++visits;
            return column == 1 ? arena.Column(3, type, position) : original;
        });
        UNIT_ASSERT_VALUES_EQUAL(visits, 2);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), before + 3); // Changed column, branch, root.
        const auto newBranch = arena[rewritten].Arguments[0];
        UNIT_ASSERT_VALUES_EQUAL(newBranch, arena[rewritten].Arguments[1]);
        UNIT_ASSERT_VALUES_EQUAL(arena[newBranch].Arguments[1], unchanged);
        UNIT_ASSERT_VALUES_EQUAL(arena[branch].Arguments[0], left);
        UNIT_ASSERT_VALUES_EQUAL(arena[left].ColumnId, 1);
        TEvaluationContext context;
        context.Column = [](TColumnId id) { return TValue{{K::Int32}, std::int64_t{id == 3 ? 100 : id == 1 ? 10 : 20}}; };
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, root, context)), "62");
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, rewritten, context)), "242");
        const auto after = arena.Size();
        auto identical = arena.RewriteColumns(rewritten, [](TExprId id, TColumnId, TType, TSourcePosition) { return id; });
        UNIT_ASSERT_VALUES_EQUAL(identical, rewritten);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), after);
    }

    Y_UNIT_TEST(ColumnRewriteIsSimultaneousAndRetypesChangedPaths) {
        TExpressionArena arena;
        auto a = arena.Column(1, {K::Int32});
        auto b = arena.Column(2, {K::Int32});
        auto root = arena.Binary(O::Subtract, a, b);
        auto swapped = arena.RewriteColumns(root, [&](TExprId, TColumnId column, TType type, TSourcePosition position) {
            return arena.Column(column == 1 ? 2 : 1, type, position);
        });
        UNIT_ASSERT_VALUES_EQUAL(arena[arena[swapped].Arguments[0]].ColumnId, 2);
        UNIT_ASSERT_VALUES_EQUAL(arena[arena[swapped].Arguments[1]].ColumnId, 1);
        auto widened = arena.RewriteColumns(root, [&](TExprId id, TColumnId column, TType, TSourcePosition position) {
            return column == 1 ? arena.Column(column, {K::Int64}, position) : id;
        });
        UNIT_ASSERT(arena[widened].Type == TType{K::Int64});
        const auto convertedRight = arena[widened].Arguments[1];
        UNIT_ASSERT(arena[convertedRight].Op == O::Convert);
        UNIT_ASSERT_VALUES_EQUAL(arena[convertedRight].Arguments[0], b);
        UNIT_ASSERT(arena[root].Type == TType{K::Int32});
        UNIT_ASSERT_EXCEPTION(arena.RewriteColumns(root, [&](TExprId id, TColumnId column, TType, TSourcePosition position) {
            return column == 1 ? arena.Column(column, {K::Bool}, position) : id;
        }), TFrontendError);
        UNIT_ASSERT(arena[root].Type == TType{K::Int32});
    }

    Y_UNIT_TEST(DeepExpressionsDoNotRecurse) {
        TExpressionArena arena;
        auto one = Integer(arena, 1);
        auto expression = one;
        for (unsigned i = 0; i < 100000; ++i) {
            expression = arena.Binary(O::Add, expression, one);
        }
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), 100001);
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, expression)), "100001");
        TExpressionArena copied;
        auto result = copied.Import(arena, expression, {});
        UNIT_ASSERT_VALUES_EQUAL(copied.Size(), arena.Size());
        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(copied, result)), "100001");
        UNIT_ASSERT_VALUES_EQUAL(arena.RewriteColumns(expression, {}), expression);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), 100001);
    }

    Y_UNIT_TEST(ColumnRewritePreservesLeafAndReplacementContracts) {
        TExpressionArena arena;
        const auto column = arena.Column(7, {K::Int32}, {8, 2, 3});
        const auto literal = Integer(arena, 1);
        const auto parameter = arena.Parameter("p", {K::Int32});
        for (const auto root : {literal, parameter})
            UNIT_ASSERT_VALUES_EQUAL(arena.RewriteColumns(root, {}), root);
        unsigned visits = 0;
        const auto rebound = arena.RewriteColumns(column, [&](auto original, auto id, auto type, auto position) {
            ++visits;
            UNIT_ASSERT_VALUES_EQUAL(original, column);
            UNIT_ASSERT_VALUES_EQUAL(id, 7);
            UNIT_ASSERT(type == TType{K::Int32});
            UNIT_ASSERT_VALUES_EQUAL(position.Offset, 8);
            return arena.Column(id, type.Optional(), position);
        });
        UNIT_ASSERT_VALUES_EQUAL(visits, 1);
        UNIT_ASSERT(arena[column].Type == TType{K::Int32});
        UNIT_ASSERT(arena[rebound].Type == (TType{K::Int32, true}));
        UNIT_ASSERT_EXCEPTION(arena.RewriteColumns(column, [&](auto, auto, auto, auto) {
            return static_cast<TExprId>(arena.Size());
        }), std::out_of_range);
        UNIT_ASSERT_EXCEPTION(arena.RewriteColumns(arena.Size(), {}), std::out_of_range);
    }

    Y_UNIT_TEST(ErrorsPreserveSourceAndRejectInvalidTypes) {
        TExpressionArena arena;
        auto input = Integer(arena, 1);
        try {
            arena.Unary(O::Not, input, {10, 2, 4});
            UNIT_FAIL("Expected a type error");
        } catch (const TFrontendError& error) {
            UNIT_ASSERT_VALUES_EQUAL(error.Position.Offset, 10);
            UNIT_ASSERT_VALUES_EQUAL(error.Position.Line, 2);
            UNIT_ASSERT_VALUES_EQUAL(error.Position.Column, 4);
        }
        UNIT_ASSERT_EXCEPTION(ParseType("List<Int64>"), TFrontendError);
        UNIT_ASSERT_EXCEPTION(ParseType("Decimal(36,2)"), TFrontendError);
        UNIT_ASSERT((ParseType("Decimal(12,2)?") == TType{K::Decimal, true, 12, 2}));
    }
}
