#include "kqp_native_scalar_test_helpers.h"

#include <limits>

namespace NKikimr::NKqp {
using NTests::CheckScalar;
Y_UNIT_TEST_SUITE(KqpNativeScalarBoundaries) {
    Y_UNIT_TEST(DocumentLiteralBytesExecuteWithoutSemanticPasses) {
        using namespace NFrontend;
        for (const auto kind : {ETypeKind::Json, ETypeKind::Yson}) {
            TExpressionArena arena;
            auto value = arena.Literal({kind}, std::string("{}"));
            for (unsigned depth = 0; depth < 3; ++depth) {
                CheckScalar(arena, value, "document literal bytes");
                value = arena.Unary(EOpcode::OptionalWrap, value);
            }
            CheckScalar(arena, arena.Literal({kind, true}, {}), "absent document literal");
        }
    }

    Y_UNIT_TEST(RegexMatchersExecuteWithoutSemanticPasses) {
        using namespace NFrontend;
        const std::pair<const char*, bool> cases[]{
            {"SELECT Re2::Match('a.*')('abc')", true},
            {"SELECT Re2::Match('b')('abc')", false},
            {"SELECT Re2::Grep('b', NULL)('abc')", true},
            {"SELECT Re2::Grep('z')('abc')", false},
            {"SELECT Re2::Match('.*')(NULL)", false},
            {"SELECT Re2::Grep('.*', NULL)(NULL)", false},
            {"SELECT Re2::Match('')('')", true},
            {"SELECT Re2::Match('.')('é')", false},
            {"SELECT Re2::Match('é.')('éя')", true},
            {"$p = 'a.*'; $m = Re2::Match($p); SELECT $m('abc')", true},
        };
        for (const auto& [sql, expected] : cases) {
            const auto compiled = CompileNative(sql, [](auto, auto) -> TCatalogTable {
                throw TFrontendError({}, "unexpected table");
            });
            const auto& program = compiled.Program->Get();
            TEvaluationContext evaluation;
            evaluation.Regex = [expected](const auto&, auto, auto) { return expected; };
            CheckScalar(program.Expressions, NTests::Projection(program), sql, evaluation);
        }
    }

    Y_UNIT_TEST(InResultNullSemanticsExecuteWithoutSemanticPasses) {
        using namespace NFrontend;
        using K = ETypeKind;
        const std::array<TScalar, 3> matches{TScalar{}, TScalar{false}, TScalar{true}};
        for (const bool missing : {false, true}) for (const auto& value : matches) for (const bool tuple : {false, true}) {
            TExpressionArena arena;
            auto lookup = arena.Literal({K::Int32, true}, missing ? TScalar{} : TScalar{std::int64_t{7}});
            if (tuple) lookup = arena.Collection(std::array{arena.Literal({K::String}, std::string{"key"}), lookup}, false);
            const auto match = arena.Literal({K::Bool, true}, value);
            for (const bool ansi : {false, true}) {
                const auto root = arena.InResult(lookup, match, ansi);
                CheckScalar(arena, root, "IN nullable scalar/tuple result");
                CheckScalar(arena, arena.FoldConstants(std::array{root}).front(), "folded IN result");
            }
            if (missing) {
                const auto checked = arena.Ensure(arena.Literal({K::Bool}, true), arena.Literal({K::Bool}, false), "unreached match");
                CheckScalar(arena, arena.InResult(lookup, checked, false), "NULL lookup skips checked match");
            }
        }
        for (const auto* sql : {
                "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; SELECT Just(1) IN (NULL, 1, Ensure(2, FALSE, 'unreached item'))",
                "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; SELECT CAST(NULL AS Int32) IN (Ensure(2, FALSE, 'unreached item'), 3)",
                "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; SELECT Just(1) NOT IN (2, NULL)",
                "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; SELECT Ensure(NULL, FALSE, 'discarded Null check') IN (1, 2)",
                "PRAGMA AnsiInForEmptyOrNullableItemsCollections; SELECT Just(1) NOT IN (2, NULL)"})
            CheckScalar(sql);
    }

    Y_UNIT_TEST(InResultLookupErrorsSurviveTerminalEmission) {
        using namespace NFrontend;
        using K = ETypeKind;
        TExpressionArena arena;
        const auto missing = arena.Literal({K::Int32, true}, {});
        const auto checked = arena.Ensure(arena.Literal({K::Int32}, std::int64_t{7}), arena.Literal({K::Bool}, false), "checked tuple lookup");
        const auto tuple = arena.Collection(std::array{missing, checked}, false);
        const auto root = arena.InResult(tuple, arena.Literal({K::Bool}, true), false);
        for (const auto expression : {root, arena.FoldConstants(std::array{root}).front()}) {
            UNIT_ASSERT_EXCEPTION_CONTAINS(Evaluate(arena, expression), TFrontendError, "checked tuple lookup");
            NYql::TExprContext ctx;
            const auto lambda = LowerNativeLambda(arena, expression, ctx, {});
            UNIT_ASSERT(AreAllNodesTypeAnnotated(*lambda));
            UNIT_ASSERT(!NeedsRboCompatibilityLowering(lambda));
            NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
            NYql::NCommon::TMkqlCommonCallableCompiler compiler;
            NYql::NCommon::TMkqlBuildContext build(compiler, *setup.PgmBuilder, ctx);
            const auto runtime = NYql::NCommon::MkqlBuildLambda(*lambda, build, {setup.PgmBuilder->NewEmptyStruct()});
            auto graph = setup.BuildGraph(runtime);
            UNIT_ASSERT_EXCEPTION_CONTAINS(graph->GetValue(), yexception, "checked tuple lookup");
        }
    }


    Y_UNIT_TEST(StructuralNullChecksExecuteWithoutSemanticPasses) {
        using namespace NFrontend;
        for (const bool missing : {false, true}) {
            TExpressionArena arena;
            const auto item = arena.Literal({ETypeKind::Int32, true}, missing ? TScalar{} : TScalar{std::int64_t{17}});
            const auto list = arena.Collection(std::array{item, item}, true);
            const auto tuple = arena.Collection(std::array{list, item}, false);
            const auto present = arena.Unary(EOpcode::OptionalWrap, tuple);
            const auto absent = arena.Literal(arena[tuple].Type.Optional(), {});
            for (const auto input : {item, list, tuple, present, absent}) {
                const auto root = arena.Unary(EOpcode::HasNull, input);
                CheckScalar(arena, root, "structural NULL check");
                CheckScalar(arena, arena.FoldConstants(std::array{root}).front(), "folded structural NULL check");
            }
        }
        for (const auto* sql : {
            "SELECT ((1, NULL), 2) IN (((1, 'a'), 2), ((2, NULL), 3))",
            "SELECT (1, 'a') IN ((1, 'a'), (2, NULL))",
            "SELECT (1, 'a') NOT IN ((2, NULL), (3, 'b'))",
            "PRAGMA AnsiInForEmptyOrNullableItemsCollections; SELECT (1, NULL) IN ((1, 'a'), (2, NULL))"})
            CheckScalar(sql);
    }

    Y_UNIT_TEST(NestedCollectionTypesAndValuesSurviveArenaAndEmissionBoundaries) {
        using namespace NFrontend;
        TExpressionArena source;
        const auto integer = source.Literal({ETypeKind::Int32}, std::int64_t{17});
        const auto list = source.Collection(std::array{integer, integer}, true);
        const auto tuple = source.Collection(std::array{integer, list}, false);
        const auto root = source.Collection(std::array{tuple, list}, false);
        const auto type = source[root].Type;
        UNIT_ASSERT(type == source.ParseTypeName("Tuple<Tuple<Int32,List<Int32>>,List<Int32>>"));
        CheckScalar(source, root, "nested tuple/list construction");
        const auto folded = source.FoldConstants(std::array{root}).front();
        CheckScalar(source, folded, "nested tuple/list literal");
        TExpressionArena target;
        target.ParseTypeName("Tuple<String,Double>"); // Force different shape IDs.
        const auto imported = target.Import(source, folded, [](auto, auto, auto) -> TExprId {
            ythrow yexception() << "literal import must not bind columns";
        });
        CheckScalar(target, imported, "nested literal shape remapping");
        NYql::TExprContext ctx;
        const auto* emitted = ExportNativeType(type, ctx, &source);
        UNIT_ASSERT(target[imported].Type == ImportNativeType(*emitted, &target));
        auto shared = source.ParseTypeName("Tuple<Int32>");
        for (unsigned i = 0; i < 24; ++i) shared = source.TupleType(std::array{shared, shared});
        const auto* sharedBackend = ExportNativeType(shared, ctx, &source);
        UNIT_ASSERT(sharedBackend);
        UNIT_ASSERT(ImportNativeType(*sharedBackend, &target) == target.ImportType(source, shared));
        auto nested = source.TupleType({});
        for (unsigned i = 1; i < 64; ++i) nested = source.ListType(nested);
        UNIT_ASSERT_EXCEPTION(source.ListType(nested), TResourceLimitError);
    }

    Y_UNIT_TEST(ListIndexesUseCheckedScalarConversion) {
        using namespace NFrontend;
        using K = ETypeKind;
        TExpressionArena arena;
        const TExprId items[]{arena.Literal({K::Int32}, std::int64_t{10}), arena.Literal({K::Int32}, std::int64_t{20})};
        const auto list = arena.Collection(items, true);
        const auto index = arena.Parameter("index", {K::Double, true});
        const auto root = arena.ListElement(list, index);
        for (const std::optional<double> value : {std::optional<double>{}, std::optional<double>{-1}, {-0.5}, {-0.0},
                {0.5}, {1.0}, {1.9}, {2.0}, {1e30}, {std::numeric_limits<double>::infinity()},
                {-std::numeric_limits<double>::infinity()}, {std::numeric_limits<double>::quiet_NaN()}}) {
            TEvaluationContext evaluation;
            evaluation.Parameter = [&](auto) { return TValue{{K::Double, true}, value ? TScalar{*value} : TScalar{}}; };
            const auto expected = value && (*value == 0 || *value == 1);
            const auto evaluated = Evaluate(arena, root, evaluation);
            UNIT_ASSERT_VALUES_EQUAL(evaluated.IsNull(), !expected);
            if (expected) UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(evaluated.Data), *value == 0 ? 10 : 20);
            CheckScalar(arena, root, "list index checked conversion", evaluation,
                [&](NYql::NCommon::TMkqlBuildContext& build, NMiniKQL::TProgramBuilder& pb) {
                    auto item = value ? pb.NewOptional(pb.NewDataLiteral<double>(*value))
                        : pb.NewEmptyOptional(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<double>::Id)));
                    build.Parameters = pb.NewStruct({{"$index", item}});
                });
        }
        for (const auto* text : {"0", "0.5", "0.999", "inf", "nan"}) {
            const auto source = arena.Literal({K::String}, std::string{text});
            const auto decimal = arena.Cast(source, {K::Decimal, false, 35, 34});
            const auto access = arena.ListElement(list, decimal);
            UNIT_ASSERT_VALUES_EQUAL(Evaluate(arena, access).IsNull(), std::string_view(text) != "0");
            CheckScalar(arena, access, "wide Decimal list index requires an exact integral value");
        }
    }

    Y_UNIT_TEST(NullTuplePayloadExecutesWithoutOptionalNullLifting) {
        using namespace NFrontend;
        using K = ETypeKind;
        TExpressionArena arena;
        const TType fields[]{TType{K::Int32}, arena[arena.Null()].Type};
        const auto tupleType = arena.TupleType(fields).Optional();
        const auto tuple = arena.Parameter("tuple", tupleType);
        const auto root = arena.TupleElement(tuple, 1);
        for (const bool present : {false, true}) {
            TEvaluationContext evaluation;
            evaluation.Parameter = [&](auto) {
                if (!present) return TValue{tupleType, {}};
                auto items = std::make_shared<TTupleValue>();
                items->Items = {{{K::Int32}, std::int64_t{1}}, {{K::Null}, {}}};
                return TValue{tupleType, TTuple(std::move(items))};
            };
            CheckScalar(arena, root, "NULL payload in a present or missing tuple", evaluation,
                [&](NYql::NCommon::TMkqlBuildContext& build, NMiniKQL::TProgramBuilder& pb) {
                    auto value = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(1), pb.NewNull()}));
                    if (!present) value = pb.NewEmptyOptional(value.GetStaticType());
                    build.Parameters = pb.NewStruct({{"$tuple", value}});
                });
        }
    }

    Y_UNIT_TEST(CheckedArithmeticAndBitCastExecuteWithoutSemanticPasses) {
        using namespace NFrontend;
        using K = ETypeKind;
        using O = EOpcode;
        for (const auto kind : {K::Int8, K::Int16, K::Int32, K::Int64, K::Uint8, K::Uint16, K::Uint32, K::Uint64})
        for (const bool optional : {false, true}) {
            const TType type{kind, optional};
            const auto width = kind == K::Int8 || kind == K::Uint8 ? 8 : kind == K::Int16 || kind == K::Uint16 ? 16 :
                kind == K::Int32 || kind == K::Uint32 ? 32 : 64;
            const auto maximum = (static_cast<unsigned __int128>(1) << (width - type.IsSigned())) - 1;
            std::vector<TScalar> values = type.IsSigned()
                ? std::vector<TScalar>{std::int64_t{0}, std::int64_t{1}, std::int64_t{-1},
                    static_cast<std::int64_t>(maximum), static_cast<std::int64_t>(-static_cast<__int128>(maximum) - 1)}
                : std::vector<TScalar>{std::uint64_t{0}, std::uint64_t{1}, static_cast<std::uint64_t>(maximum)};
            if (optional) values.emplace_back();
            for (const auto& a : values) {
                TExpressionArena arena;
                const auto left = arena.Literal(type, a);
                CheckScalar(arena, arena.CheckedNegate(left), "checked negate width/overflow/null");
                for (const auto target : {K::Int8, K::Uint8, K::Int64, K::Uint64})
                    CheckScalar(arena, arena.BitCast(left, {target}), "bitcast width/signedness/null");
                for (const auto& b : values)
                for (const auto op : {O::Add, O::Subtract, O::Multiply, O::Divide, O::Modulo})
                    CheckScalar(arena, arena.CheckedBinary(op, left, arena.Literal(type, b)), "checked arithmetic width/overflow/null");
            }
        }
        for (const auto* sql : {"SELECT BITCAST(TRUE AS Uint8)", "SELECT BITCAST(Date('1970-01-03') AS Int8)",
                "SELECT BITCAST(CAST(-1 AS Interval) AS Uint64)", "SELECT BITCAST(CAST(-1 AS Date32) AS Uint8)",
                "PRAGMA CheckedOps='true'; SELECT 0l + 18446744073709551615ul",
                "PRAGMA CheckedOps='true'; SELECT 18446744073709551615ul * 18446744073709551615ul",
                "PRAGMA CheckedOps='true'; SELECT -9223372036854775808l",
                "PRAGMA ClassicDivision='false'; SELECT 1 / 0",
                "PRAGMA UnicodeLiterals; SELECT '\\xff's"}) CheckScalar(sql);
    }

    Y_UNIT_TEST(BitwiseAndNullSafePredicatesEmitTerminalOperations) {
        using namespace NFrontend;
        for (const auto* sql : {"SELECT ~1u", "SELECT 7u & 3u | 1u", "SELECT 1u << 2u",
                "SELECT -1 >> 1", "SELECT 1u ^ 3ul", "SELECT ~NULL", "SELECT NULL & 1u",
                "SELECT NULL IS DISTINCT FROM 1", "SELECT NULL IS NOT DISTINCT FROM NULL",
                "SELECT Just(1) IS NOT DISTINCT FROM 1", "SELECT 1 IS DISTINCT FROM 2ul",
                "SELECT CAST('nan' AS Double) IS NOT DISTINCT FROM CAST('nan' AS Double)",
                "SELECT CAST('nan' AS Float) IS NOT DISTINCT FROM CAST('nan' AS Double)"}) {
            for (const auto parser : {EParser::Handwritten, EParser::Grammar}) {
                TCompilerSettings settings; settings.Parser = parser;
                const auto compiled = CompileNative(sql, {}, settings);
                const auto& program = compiled.Program->Get();
                CheckScalar(program.Expressions, NTests::Projection(program), sql);
            }
        }
        for (const auto kind : {ETypeKind::Int8, ETypeKind::Int16, ETypeKind::Int32, ETypeKind::Int64,
                ETypeKind::Uint8, ETypeKind::Uint16, ETypeKind::Uint32, ETypeKind::Uint64})
        for (const bool optional : {false, true}) {
            TExpressionArena arena;
            const TType type{kind, optional};
            for (const auto input : {0, 1, 127}) {
                const auto value = arena.Literal(type, type.IsSigned() ? TScalar{std::int64_t{-input}} : TScalar{std::uint64_t(input)});
                for (const auto count : {0u, 1u, 7u, 8u, 16u, 31u, 32u, 63u, 64u, 255u})
                for (const auto op : {EOpcode::ShiftLeft, EOpcode::ShiftRight})
                    CheckScalar(arena, arena.Binary(op, value, arena.Literal({ETypeKind::Uint8}, std::uint64_t{count})), "shift width/signedness");
                if (!type.IsSigned()) {
                    CheckScalar(arena, arena.Unary(EOpcode::BitNot, value), "bitwise complement");
                    for (const auto op : {EOpcode::BitAnd, EOpcode::BitOr, EOpcode::BitXor})
                        CheckScalar(arena, arena.Binary(op, value, arena.Literal({ETypeKind::Uint64}, std::uint64_t{255})), "bitwise width promotion");
                }
            }
            if (optional) CheckScalar(arena, arena.Binary(EOpcode::ShiftRight, arena.Literal(type, {}),
                arena.Literal({ETypeKind::Uint8}, std::uint64_t{1})), "absent shift input");
        }
        for (const auto kind : {ETypeKind::Float, ETypeKind::Double, ETypeKind::Decimal})
        for (const bool optional : {false, true}) {
            TExpressionArena arena;
            const TType type = kind == ETypeKind::Decimal ? TType{kind, optional, 12, 2} : TType{kind, optional};
            auto values = kind == ETypeKind::Decimal
                ? std::vector<TScalar>{TDecimal{0}, TDecimal{200}, TDecimal{TDecimal::Inf()}, TDecimal{TDecimal::NaN()}}
                : std::vector<TScalar>{0.0, -0.0, 2.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()};
            if (optional) values.emplace_back();
            for (const auto& a : values) for (const auto& b : values) {
                const auto root = arena.Binary(EOpcode::IsNotDistinct, arena.Literal(type, a), arena.Literal(type, b));
                CheckScalar(arena, root, "null-safe equality unordered/absent input");
                CheckScalar(arena, arena.Unary(EOpcode::Not, root), "null-safe inequality");
            }
        }
    }

    Y_UNIT_TEST(BetweenModesExecuteWithoutSemanticLowering) {
        for (const auto parser : {NFrontend::EParser::Handwritten, NFrontend::EParser::Grammar})
        for (const auto* operation : {" BETWEEN ", " NOT BETWEEN ", " BETWEEN SYMMETRIC ", " NOT BETWEEN SYMMETRIC "})
        for (const auto* value : {"0.0", "2.0", "4.0", "NULL", "CAST('nan' AS Double)"})
        for (const auto* bounds : {"1.0 AND 3.0", "3.0 AND 1.0", "NULL AND 3.0", "1.0 AND NULL"}) {
            const auto sql = std::string("SELECT ") + value + operation + bounds;
            NFrontend::TCompilerSettings settings; settings.Parser = parser;
            auto compiled = NFrontend::CompileNative(sql, {}, settings);
            const auto& program = compiled.Program->Get();
            CheckScalar(program.Expressions, NTests::Projection(program), sql);
        }
        CheckScalar("SELECT 2 IN COMPACT (1, NULL)");
    }

    Y_UNIT_TEST(ScalarMinMaxAndListMetadataEmitTerminalOperations) {
        for (const auto* sql : {
            "SELECT ListLength([1, 2])", "SELECT ListLength([])", "SELECT ListLength(NULL)",
            "SELECT ListHasItems([1])", "SELECT ListHasItems([])",
            "SELECT ListLength(ListFromRange(Just(1), 6, 2))",
            "SELECT ListHasItems(ListFromRange(CAST(NULL AS Int32?), 6))",
            "SELECT Starts_With('abc', 'a')", "SELECT Ends_With('abc', 'c')",
            "SELECT Starts_With(CAST(NULL AS String?), 'a')",
            "SELECT MaxOf(1, 2l)", "SELECT MinOf(1, 2l)",
            "SELECT Greatest(1, 2l, Just(3))", "SELECT Least(1, NULL, 3)",
            "SELECT MaxOf('abc', 'ab')", "SELECT MinOf(TRUE, FALSE)",
            "SELECT MinOf(Just(Date('2020-01-01')), Date('2021-01-01'))",
            "SELECT MaxOf(Decimal('1.25', 9, 2), Decimal('2.50', 9, 2))",
        }) CheckScalar(sql);
        using namespace NFrontend;
        for (const auto kind : {ETypeKind::Float, ETypeKind::Double, ETypeKind::Decimal})
        for (const bool optional : {false, true})
        for (const auto op : {EOpcode::Minimum, EOpcode::Maximum}) {
            const TType type = kind == ETypeKind::Decimal ? TType{kind, optional, 12, 2} : TType{kind, optional};
            const std::vector<TScalar> values = kind == ETypeKind::Decimal
                ? std::vector<TScalar>{TDecimal{-300}, TDecimal{200}, TDecimal{TDecimal::Inf()}, TDecimal{TDecimal::NaN()}}
                : std::vector<TScalar>{-3.0, 2.0, -0.0, 0.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()};
            for (const auto& left : values) for (const auto& right : values) {
                TExpressionArena arena;
                const auto root = arena.Binary(op, arena.Literal(type, left), arena.Literal(type, right));
                CheckScalar(arena, root, "scalar min/max NULL/NaN/zero contract");
                const auto folded = arena.FoldConstants(std::span{&root, 1}).front();
                UNIT_ASSERT(arena[folded].Op == EOpcode::Literal);
                CheckScalar(arena, folded, "folded scalar min/max contract");
                if (optional) CheckScalar(arena, arena.Binary(op, arena.Literal(type, {}), arena.Literal(type, left)),
                    "scalar min/max absent input");
            }
        }
    }

}
} // namespace NKikimr::NKqp
