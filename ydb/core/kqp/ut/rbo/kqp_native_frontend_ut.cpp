#include "native_boundaries/kqp_native_scalar_test_helpers.h"
#include <ydb/core/kqp/opt/rbo/kqp_native_frontend.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_lookup_join.h>
#include <ydb/core/kqp/opt/rbo/frontend_lowering.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_selectivity.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_compatibility.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_source_builder.h>
#include <ydb/core/kqp/opt/rbo/physical_conversion/kqp_rbo_physical_lookup_join_builder.h>
#include <ydb/core/kqp/ut/rbo/kqp_rbo_test_helpers.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_type_ops.h>
#include <yql/essentials/providers/common/mkql/yql_provider_mkql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/types/binary_json/write.h>
#include <yql/essentials/minikql/jsonpath/jsonpath.h>

#include <algorithm>
#include <cmath>
#include <bit>
#include <limits>
#include <type_traits>
#include <tuple>

namespace NKikimr::NKqp {
namespace {
TNativeTable NoTables(const NFrontend::TTableAddress&, NFrontend::TSourcePosition pos) {
    throw NFrontend::TFrontendError(pos, "unexpected table lookup");
}

void SetNativeReadType(NTests::TIdTestContext& f, TOpRead& read, const TUnorderedIUs& nullable = {},
    NFrontend::ETypeKind kind = NFrontend::ETypeKind::Int32) {
    if (!f.Props.NativeExpressions) f.Props.NativeExpressions = std::make_shared<NFrontend::TExpressionArena>();
    TNativeSchema schema{{}, std::nullopt, f.Props.NativeExpressions};
    NFrontend::TCatalogTable catalog{std::string(read.TableRef.Path), {}};
    THashSet<TString> names;
    for (auto id : read.GetColumns()) {
        NFrontend::TType type{kind, nullable.Contains(id)};
        schema.Row.emplace(id, type);
        const auto name = f.Props.InfoUnitRegistry.Get(id).GetColumnName();
        if (names.insert(name).second) catalog.Columns.push_back({std::string(name), type});
    }
    read.NativeSchema = std::move(schema);
    read.TableRef.NativeCatalog.emplace(std::move(catalog));
}

IGraphTransformer::TStatus ComputeTypesWithoutBackend(TOpRoot& root, TRBOContext& context) {
    const auto nodes = context.ExprCtx.NodeAllocationCounter;
    const auto types = context.ExprCtx.TypeNodes.size();
    const auto result = root.ComputeTypes(context);
    UNIT_ASSERT_VALUES_EQUAL(context.ExprCtx.NodeAllocationCounter, nodes);
    UNIT_ASSERT_VALUES_EQUAL(context.ExprCtx.TypeNodes.size(), types);
    for (const auto& item : root) {
        UNIT_ASSERT(item.Current->NativeSchema);
        for (const auto& expression : item.Current->GetExpressions()) {
            UNIT_ASSERT(expression.get().IsNative());

        }
    }
    return result;
}

using NTests::Projection;
using NTests::CheckScalar;
} // namespace

Y_UNIT_TEST_SUITE(KqpNativeFrontend) {
    Y_UNIT_TEST(AggregateFiltersUseOrdinaryNativeOperators) {
        using namespace NFrontend;
        for (const auto* sql : {
            "SELECT SUM(v) FILTER(WHERE id > 1), COUNT(*) FILTER(WHERE id > 2) FROM t",
            "SELECT SUM(v) FILTER(WHERE id > 1) OVER(), SUM(v) FILTER(WHERE id > 2) OVER(), SUM(v) OVER() FROM t"}) {
            NTests::TIdTestContext f;
            const auto nodes = f.ExprCtx.NodeAllocationCounter;
            const auto types = f.ExprCtx.TypeNodes.size();
            auto query = CompileNativeToRbo(sql, [](const TTableAddress&, TSourcePosition) {
                return TNativeTable{TCatalogTable{"t", {{"id", {ETypeKind::Int32}}, {"v", {ETypeKind::Int32, true}}}},
                    TRboTable{"/Root/t", "1:2", "", "3"}};
            }, f.ExprCtx);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
            auto& root = *query.Roots.front();
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
            size_t conditionals = 0;
            for (const auto& node : root.PlanProps.NativeExpressions->Nodes()) conditionals += node.Op == EOpcode::If;
            UNIT_ASSERT_VALUES_EQUAL(conditionals, 2);
        }
    }

    Y_UNIT_TEST(CatalogSnapshotSurvivesTheResolverAndIsSharedByAliasedReads) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const TCatalogTable* catalog = nullptr;
        size_t resolutions = 0;
        auto query = [&] {
            const TCatalogSnapshot snapshot(TCatalogTable{"t", {{"z", {ETypeKind::String}}, {"a", {ETypeKind::Int32}}}});
            catalog = &snapshot.Get();
            return CompileNativeToRbo("SELECT l.z, r.z FROM t AS l JOIN t AS r ON l.a = r.a",
                [&](const TTableAddress& address, TSourcePosition) {
                    const std::string_view name = address.Name;
                    UNIT_ASSERT_VALUES_EQUAL(name, "t");
                    ++resolutions;
                    return TNativeTable{snapshot, TRboTable{"/Root/t", "1:2", "", "3"}};
                }, f.ExprCtx);
        }();
        UNIT_ASSERT_VALUES_EQUAL(resolutions, 1);
        auto& root = *query.Roots.front();
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
        TUnorderedIUs bindings;
        size_t reads = 0;
        for (const auto& item : root) if (item.Current->Kind == EOperator::Source) {
            const auto& read = CastOperator<TOpRead>(*item.Current);
            UNIT_ASSERT(read.TableRef.NativeCatalog);
            UNIT_ASSERT(&read.TableRef.NativeCatalog->Get() == catalog);
            UNIT_ASSERT(read.TableRef.GetColumnType("a") == (TType{ETypeKind::Int32}));
            for (const auto id : read.GetColumns()) UNIT_ASSERT(bindings.Add(id));
            ++reads;
        }
        UNIT_ASSERT_VALUES_EQUAL(reads, 2);
        UNIT_ASSERT_VALUES_EQUAL(bindings.Size(), 4);
    }

    Y_UNIT_TEST(ListOperationsEmitExecutableTerminalNodes) {
        for (const auto* sql : {
            "SELECT ListConcat(CAST(ListFromRange(1, 10) AS List<String>), '..')",
            "SELECT ListConcat(CAST(ListFromRange(5, -1, -2) AS List<String>), ',')",
            "SELECT ListConcat(CAST(ListFromRange(Just(1), 4) AS List<String>), ',')",
            "SELECT ListConcat(CAST(ListFromRange(CAST(NULL AS Int32?), 4) AS List<String>))",
            "SELECT ListConcat(CAST(ListFromRange(1, 1) AS List<String>))",
            "SELECT ListConcat(AsList('a', NULL, 'b', NULL), ',')",
            "SELECT ListConcat(AsList(NULL, 'a', NULL), ',')",
            "SELECT ListConcat(AsList('a', 'b'), CAST(NULL AS String?))",
            "SELECT ListConcat(AsList('a'u, NULL, '\\xc3\\xa9'u), ','u)",
            "SELECT CAST(AsList('1', 'bad') AS List<Int32>) IS NULL",
            "SELECT CAST(AsList('1', 'bad') AS List<Int32>?) IS NULL",
            "SELECT CAST(AsList(127, 128) AS List<Int8>) IS NULL",
            "SELECT ListConcat(CAST(CAST(AsList('1', 'bad', '2') AS List<Int32?>) AS List<String?>), ',')",
            "SELECT ListConcat(CAST(AsList() AS List<String>))",
        }) CheckScalar(sql);
    }

    Y_UNIT_TEST(LambdaClosuresProduceTheSameNativeOperatorsAsInlineScalars) {
        using namespace NFrontend;
        std::optional<TString> plan;
        for (const auto* sql : {
            "$n = 1l; $f = ($x) -> { $n = $n + $x; RETURN $n; }; "
                "$make = ($x) -> { $local = $x + 1l; RETURN ($y) -> ($local + $y); }; "
                "SELECT id, $make(id)(2l) AS total FROM t WHERE $f(id) > 2l ORDER BY id",
            "SELECT id, (id + 1l) + 2l AS total FROM t WHERE (1l + id) > 2l ORDER BY id",
        }) {
            NTests::TIdTestContext f;
            const auto nodes = f.ExprCtx.NodeAllocationCounter;
            const auto types = f.ExprCtx.TypeNodes.size();
            const auto query = CompileNativeToRbo(sql, [](auto, auto) {
                return TNativeTable{NFrontend::TCatalogTable{"t", {{"id", {ETypeKind::Int64}}}},
                    TRboTable{"/Root/t", "1:2", "", "3"}, NYql::EStorageType::RowStorage};
            }, f.ExprCtx);
            UNIT_ASSERT_VALUES_EQUAL(query.Roots.size(), 1);
            auto& root = *query.Roots.front();
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
            const auto actual = root.PlanToString(f.ExprCtx);
            if (plan) UNIT_ASSERT_VALUES_EQUAL(*plan, actual);
            else plan = actual;
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        }
    }

    Y_UNIT_TEST(LikeRuntimeOpcodeIsTerminalAndUnicodeAware) {
        using namespace NFrontend;
        struct TCase { std::optional<std::string> Input; std::string Pattern; std::string Escape; bool Expected; bool CaseSensitive = true; };
        for (const auto& test : std::vector<TCase>{
            {"a_b", "a#_b", "#", true}, {"acb", "a#_b", "#", false},
            {"a%b", "a#%b", "#", true}, {"a#b", "a##b", "#", true},
            {"é", "_", "", true}, {"éж", "_", "", false},
            {"a\nb", "a_b", "", true}, {std::string{"a\0b", 3}, "a_b", "", true},
            {std::string{"\xff", 1}, "_", "", false}, {std::nullopt, "%", "", false},
            {"", "", "", true}, {"a.b", "a.b", "", true},
            {"Éж", "éЖ", "", true, false}, {"Σ", "ς", "", true, false},
            {"K", "k", "", true, false}, {"A_B", "a#_b", "#", true, false},
            {"a_b", "A_B", "", true, false}, {"a_b", "A_B", "", false},
            {std::nullopt, "a%", "", false, false}, {std::string{"\xff", 1}, "_", "", false, false}}) {
            TExpressionArena arena;
            const auto input = arena.Parameter("input", {ETypeKind::String, true});
            const auto pattern = arena.Parameter("pattern", {ETypeKind::String});
            const auto root = arena.Like(input, pattern, test.Escape, {}, test.CaseSensitive);
            TEvaluationContext evaluation;
            evaluation.Parameter = [&](std::string_view name) -> TValue {
                if (name == "pattern") return {{ETypeKind::String}, test.Pattern};
                return {{ETypeKind::String, true}, test.Input ? TScalar{*test.Input} : TScalar{}};
            };
            evaluation.Like = [&](const TValue&, std::string_view, std::string_view, bool caseSensitive) {
                UNIT_ASSERT_VALUES_EQUAL(caseSensitive, test.CaseSensitive);
                return test.Expected;
            };
            CheckScalar(arena, root, "native LIKE runtime", evaluation, [&](NYql::NCommon::TMkqlBuildContext& build, NMiniKQL::TProgramBuilder& pb) {
                auto value = test.Input ? pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(
                    NUdf::TStringRef(test.Input->data(), test.Input->size()))) :
                    pb.NewEmptyOptional(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<char*>::Id)));
                build.Parameters = pb.NewStruct({{"$input", value}, {"$pattern",
                    pb.NewDataLiteral<NUdf::EDataSlot::String>(NUdf::TStringRef(test.Pattern.data(), test.Pattern.size()))}});
            });
        }
    }


    Y_UNIT_TEST_TWIN(LikeStorageApplyUsesTheNativeRuntimeExpression, CaseSensitive) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto input = arena.Column(1, {ETypeKind::String, true});
        const auto pattern = arena.Literal({ETypeKind::String}, std::string{"_id%000_"});
        const auto predicate = arena.Like(input, pattern, {}, {}, CaseSensitive);
        UNIT_ASSERT(!CanLowerNativeOlapPredicate(arena, predicate));
        UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate, true));
        TExprContext ctx;
        const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
        UNIT_ASSERT_VALUES_EQUAL(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApplyColumnArg"); }).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(FindNodes(lowered, [](const auto& node) {
            return node->IsCallable("Udf") && node->Head().IsAtom("Re2.Match");
        }).size(), 1);
    }

    Y_UNIT_TEST(TemporalListDictionaryMembershipMatchesRuntime) {
        using namespace NFrontend;
        for (bool ansi : {false, true}) for (bool empty : {false, true}) {
            TExpressionArena arena;
            const TType item{ETypeKind::Timestamp64, true};
            const auto listType = arena.ListType(item);
            auto rows = std::make_shared<TTupleValue>();
            if (!empty) rows->Items = {{item, std::int64_t{-1000001}}, {item, {}}};
            const auto list = arena.Literal(listType, TTuple(rows));
            for (const TScalar& key : {TScalar{std::int64_t{-1000001}}, TScalar{std::int64_t{0}}, TScalar{}}) {
                const auto found = arena.InCollection(arena.Literal(item, key), list, ansi);
                CheckScalar(arena, found, "Timestamp64 dictionary membership");
            }
        }
    }

    Y_UNIT_TEST(DecimalListDictionaryMembershipMatchesRuntime) {
        using namespace NFrontend;
        for (bool ansi : {false, true}) for (bool empty : {false, true}) {
            TExpressionArena arena;
            const TType item{ETypeKind::Decimal, true, 1, 0};
            const auto listType = arena.ListType(item);
            auto rows = std::make_shared<TTupleValue>();
            if (!empty) rows->Items = {{item, TDecimal{1}}, {item, TDecimal{TDecimal::NaN()}}, {item, {}}};
            const auto list = arena.Literal(listType, TTuple(rows));
            for (const TScalar& key : {TScalar{TDecimal{1}}, TScalar{TDecimal{2}}, TScalar{TDecimal{TDecimal::NaN()}}, TScalar{}})
                CheckScalar(arena, arena.InCollection(arena.Literal(item, key), list, ansi), "Decimal dictionary membership");
        }
    }

    Y_UNIT_TEST(DecimalComparisonsPreserveOperandScalesAtRuntime) {
        using namespace NFrontend;
        using O = EOpcode;
        __int128 limit = 1;
        for (unsigned i = 0; i < 35; ++i) limit *= 10;
        for (const unsigned scale : {0, 2, 9, 35}) {
            TExpressionArena arena;
            const TType leftType{ETypeKind::Decimal, false, 35, 2}, rightType{ETypeKind::Decimal, false, 35, static_cast<std::uint8_t>(scale)};
            for (const __int128 a : {__int128{0}, __int128{1}, __int128{-1}, limit - 1, 1 - limit, TDecimal::Inf(), -TDecimal::Inf(), TDecimal::NaN()}) {
                for (const __int128 b : {__int128{0}, __int128{1}, __int128{-1}, limit - 1, 1 - limit, TDecimal::Inf(), -TDecimal::Inf(), TDecimal::NaN()}) {
                    const auto left = arena.Literal(leftType, TDecimal{a}), right = arena.Literal(rightType, TDecimal{b});
                    for (const auto op : {O::Equal, O::NotEqual, O::Less, O::LessEqual, O::Greater, O::GreaterEqual})
                        CheckScalar(arena, arena.Binary(op, left, right), "Decimal cross-scale comparison");
                }
            }
        }
    }

    Y_UNIT_TEST(UuidContractMatchesDirectRuntimeWithoutPeepholes) {
        using namespace NFrontend;
        NMiniKQL::TScopedAlloc allocator(__LOCATION__);
        for (const std::string text : {"01234567-89ab-cdef-0123-456789abcdef", "FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF", "", "0123456789abcdef0123456789abcdef", "01234567-89ab-cdef-0123-456789abcdeg"}) {
            TExpressionArena arena;
            const auto uuid = arena.Cast(arena.Literal({ETypeKind::String}, text), {ETypeKind::Uuid});
            const auto native = Evaluate(arena, uuid);
            const NUdf::TUnboxedValue runtime = NMiniKQL::ValueFromString(NUdf::EDataSlot::Uuid, NUdf::TStringRef(text.data(), text.size()));
            UNIT_ASSERT_VALUES_EQUAL_C(native.IsNull(), !runtime, text);
            if (runtime) UNIT_ASSERT_VALUES_EQUAL(TString(runtime.AsStringRef()), TString(std::get<std::string>(native.Data)));
            CheckScalar(arena, uuid, "native UUID literal and parse");
            CheckScalar(arena, arena.Cast(uuid, {ETypeKind::Utf8}), "native UUID canonical string");
            CheckScalar(arena, arena.Unary(EOpcode::StablePickle, uuid), "native UUID stable key");
        }
        TExpressionArena arena;
        const auto dependency = arena.Literal({ETypeKind::Uint64}, std::uint64_t{0});
        const auto random = arena.Call("RandomUuid", std::span(&dependency, 1));
        TEvaluationContext context;
        context.RandomUuid = [] { return std::string(16, '\0'); };
        // Random values may be regenerated by separate runtime consumers.
        // Compare deterministic properties, not the reference service's value.
        const auto equal = arena.Binary(EOpcode::Equal, random, random);
        UNIT_ASSERT(!arena[equal].Repeatable);
        const auto folded = arena.FoldConstants(std::span(&equal, 1));
        UNIT_ASSERT(arena[folded[0]].Op == EOpcode::Equal);
        CheckScalar(arena, arena.Unary(EOpcode::Length, arena.Cast(random, {ETypeKind::String})), "UUID text length", context);
    }

    Y_UNIT_TEST(RandomNumberLowersDirectlyWithNullableDependencies) {
        using namespace NFrontend;
        for (const bool nullable : {false, true}) {
            TExpressionArena arena;
            const auto dependency = nullable ? arena.Cast(arena.Null(), {ETypeKind::Uint64}) :
                arena.Literal({ETypeKind::Uint64}, std::uint64_t{0});
            const auto random = arena.Call("RandomNumber", std::span(&dependency, 1));
            const auto maximum = arena.Literal({ETypeKind::Uint64}, std::numeric_limits<std::uint64_t>::max());
            const auto inRange = arena.Binary(EOpcode::LessEqual, random, maximum);
            UNIT_ASSERT(!arena[inRange].Repeatable);
            TEvaluationContext context;
            context.RandomNumber = [] { return std::uint64_t{42}; };
            CheckScalar(arena, inRange, "random Uint64 range, without scalar peepholes", context);
        }
    }

    Y_UNIT_TEST(ExtendedTemporalSemanticsMatchRuntime) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (const auto [kind, minimum, maximum] : {
                std::tuple<K, std::int64_t, std::int64_t>{K::Date32, NUdf::MIN_DATE32, NUdf::MAX_DATE32},
                std::tuple<K, std::int64_t, std::int64_t>{K::Datetime64, NUdf::MIN_DATETIME64, NUdf::MAX_DATETIME64},
                std::tuple<K, std::int64_t, std::int64_t>{K::Timestamp64, NUdf::MIN_TIMESTAMP64, NUdf::MAX_TIMESTAMP64},
                std::tuple<K, std::int64_t, std::int64_t>{K::Interval64, -NUdf::MAX_INTERVAL64, NUdf::MAX_INTERVAL64}}) {
            TExpressionArena arena;
            for (const auto value : {minimum, std::int64_t{-1}, std::int64_t{0}, std::int64_t{1}, maximum}) {
                const auto literal = arena.Literal({kind}, value);
                CheckScalar(arena, literal, "extended temporal literal");
                CheckScalar(arena, arena.Unary(EOpcode::StablePickle, literal), "extended temporal stable key");
                for (auto target : {K::Int64, K::Uint64, K::Double, K::String})
                    CheckScalar(arena, arena.Cast(literal, {target}), "extended temporal numeric cast");
                if (kind != K::Interval64) {
                    for (auto target : {K::Date, K::Datetime, K::Timestamp, K::Date32, K::Datetime64, K::Timestamp64})
                        CheckScalar(arena, arena.Cast(literal, {target}), "extended temporal date cast");
                    for (const std::int64_t delta : {-1000001, -1, 0, 1, 1000001}) {
                        const auto interval = arena.Literal({K::Interval64}, delta);
                        CheckScalar(arena, arena.Binary(EOpcode::Add, literal, interval), "extended temporal add");
                        CheckScalar(arena, arena.Binary(EOpcode::Subtract, literal, interval), "extended temporal subtract");
                    }
                }
            }
            for (const auto value : {minimum - 1, maximum + 1})
                CheckScalar(arena, arena.Cast(arena.Literal({K::Int64}, value), {kind}), "extended temporal range rejection");
        }
        for (const auto source : {K::Date, K::Datetime, K::Timestamp, K::Interval, K::Date32, K::Datetime64, K::Timestamp64, K::Interval64}) {
            for (const auto target : {K::Int8, K::Uint8, K::Int16, K::Uint16, K::Int32, K::Uint32, K::Int64, K::Uint64,
                    K::Float, K::Double}) {
                const auto options = *NUdf::GetCastResult(*NUdf::FindDataSlot(TString(TType{source}.ToString())),
                    *NUdf::FindDataSlot(TString(TType{target}.ToString())));
                TExpressionArena arena;
                const auto converted = arena.Cast(arena.Parameter("p", {source}), {target});
                UNIT_ASSERT_VALUES_EQUAL_C(bool(arena[converted].Type.OptionalDepth), bool(options & NUdf::MayFail),
                    TType{source}.ToString() << " -> " << TType{target}.ToString());
            }
        }
        for (const auto kind : {K::Date32, K::Datetime64, K::Timestamp64}) {
            const auto slot = *NUdf::FindDataSlot(TString(TType{kind}.ToString()));
            for (const std::string date : {"--01-02", "-+1-01-01", "-144169-01-01", "-400-02-29", "-1-02-29", "0-01-01", "1-01-01",
                    "1900-02-29", "1969-12-31", "2000-02-29", "+120000-06-30", "148107-12-31", "148108-01-01"}) {
                for (const std::string time : {"", "T23:59:59Z", "T23:59:59.999999Z", "T23:59:59+01:30"}) {
                    const auto text = date + time;
                    const auto runtime = NMiniKQL::ValueFromString(slot, NUdf::TStringRef(text.data(), text.size()));
                    TExpressionArena arena;
                    const auto parsed = arena.Cast(arena.Literal({K::String}, text), {kind});
                    const auto native = Evaluate(arena, parsed);
                    UNIT_ASSERT_VALUES_EQUAL_C(!runtime, native.IsNull(), text);
                    if (runtime) UNIT_ASSERT_VALUES_EQUAL_C(kind == K::Date32 ? runtime.Get<i32>() : runtime.Get<i64>(),
                        std::get<std::int64_t>(native.Data), text);
                }
            }
        }
    }

    Y_UNIT_TEST(TemporalComparisonsRetainOperandsAndMatchRuntime) {
        using namespace NFrontend;
        using K = ETypeKind;
        const TType types[]{{K::Date}, {K::Datetime}, {K::Timestamp}, {K::Date32}, {K::Datetime64}, {K::Timestamp64}};
        for (auto leftType : types) for (auto rightType : types) {
            for (bool nullable : {false, true}) {
                leftType.OptionalDepth = nullable;
                rightType.OptionalDepth = nullable;
                TExpressionArena arena;
                const auto payload = [](TType type, std::int64_t value) -> TScalar {
                    return type.IsExtendedTemporal() ? TScalar{value} : TScalar{std::uint64_t(value)};
                };
                for (const auto [a, b] : {std::pair{0LL, 1LL}, {1LL, 0LL}, {1LL, 1LL}, {1LL, 86400LL}}) {
                    // Date's bounded domain excludes 86400 days; use two days
                    // while retaining the finer types' one-day boundary.
                    const auto rightValue = rightType.Kind == K::Date && b == 86400 ? 2 : b;
                    const auto left = arena.Literal(leftType, payload(leftType, a));
                    const auto right = arena.Literal(rightType, payload(rightType, rightValue));
                    for (const auto op : {EOpcode::Equal, EOpcode::NotEqual, EOpcode::Less,
                            EOpcode::LessEqual, EOpcode::Greater, EOpcode::GreaterEqual}) {
                        const auto comparison = arena.Binary(op, left, right);
                        UNIT_ASSERT_VALUES_EQUAL(arena[comparison].Arguments[0], left);
                        UNIT_ASSERT_VALUES_EQUAL(arena[comparison].Arguments[1], right);
                        CheckScalar(arena, comparison, leftType.ToString() + " / " + rightType.ToString());
                        const auto folded = arena.FoldConstants(std::span(&comparison, 1)).front();
                        UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(Evaluate(arena, comparison)));
                    }
                }
                if (nullable) {
                    const auto null = arena.Literal(leftType, {});
                    const auto value = arena.Literal(rightType, payload(rightType, 1));
                    CheckScalar(arena, arena.Binary(EOpcode::Equal, null, value), "nullable temporal comparison");
                }
                if (leftType.IsExtendedTemporal()) {
                    CheckScalar(arena, arena.Binary(EOpcode::Less, arena.Literal(leftType, std::int64_t{-1}),
                        arena.Literal(rightType, payload(rightType, 0))), "negative temporal comparison");
                }
                const auto leftColumn = arena.Column(1, leftType), rightColumn = arena.Column(2, rightType);
                const auto predicate = arena.Binary(EOpcode::GreaterEqual, leftColumn, rightColumn);
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate));
                TExprContext ctx;
                const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
                UNIT_ASSERT(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).empty());
            }
        }
        for (auto type : types) {
            TExpressionArena arena;
            const auto left = arena.Column(1, type.Optional());
            const auto right = arena.Column(2, type.Optional());
            const auto interval = arena.Column(3, {K::Interval64, true});
            for (const auto op : {EOpcode::Add, EOpcode::Subtract}) {
                const auto shifted = arena.Binary(op, right, interval);
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, arena.Binary(EOpcode::LessEqual, left, shifted)));
            }
            const auto difference = arena.Binary(EOpcode::Subtract, left, right);
            UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, arena.Binary(EOpcode::LessEqual, interval, difference)));
        }
    }

    Y_UNIT_TEST(IntervalComparisonsRetainOperandsAndMatchRuntime) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (const auto leftKind : {K::Interval, K::Interval64}) for (const auto rightKind : {K::Interval, K::Interval64}) {
            const std::int64_t leftMax = leftKind == K::Interval ? NUdf::MAX_TIMESTAMP - 1 : NUdf::MAX_INTERVAL64;
            const std::int64_t rightMax = rightKind == K::Interval ? NUdf::MAX_TIMESTAMP - 1 : NUdf::MAX_INTERVAL64;
            for (const bool leftNullable : {false, true}) for (const bool rightNullable : {false, true}) {
                TExpressionArena arena;
                const TType leftType{leftKind, leftNullable}, rightType{rightKind, rightNullable};
                for (const auto a : {-leftMax, std::int64_t{-1}, std::int64_t{0}, leftMax}) {
                    for (const auto b : {-rightMax, std::int64_t{-1}, std::int64_t{0}, rightMax}) {
                        const auto left = arena.Literal(leftType, a), right = arena.Literal(rightType, b);
                        for (const auto op : {EOpcode::Equal, EOpcode::NotEqual, EOpcode::Less,
                                EOpcode::LessEqual, EOpcode::Greater, EOpcode::GreaterEqual}) {
                            const auto comparison = arena.Binary(op, left, right);
                            UNIT_ASSERT_VALUES_EQUAL(arena[comparison].Arguments[0], left);
                            UNIT_ASSERT_VALUES_EQUAL(arena[comparison].Arguments[1], right);
                            CheckScalar(arena, comparison, leftType.ToString() + " / " + rightType.ToString());
                            const auto folded = arena.FoldConstants(std::span(&comparison, 1)).front();
                            UNIT_ASSERT_VALUES_EQUAL(FormatValue(Evaluate(arena, folded)), FormatValue(Evaluate(arena, comparison)));
                        }
                    }
                }
                if (leftNullable) CheckScalar(arena, arena.Binary(EOpcode::Equal,
                    arena.Literal(leftType, {}), arena.Literal(rightType, std::int64_t{0})), "NULL interval comparison");
                if (rightNullable) CheckScalar(arena, arena.Binary(EOpcode::Equal,
                    arena.Literal(leftType, std::int64_t{0}), arena.Literal(rightType, {})), "NULL interval comparison");
                const auto predicate = arena.Binary(EOpcode::LessEqual, arena.Column(1, leftType), arena.Column(2, rightType));
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate));
                TExprContext ctx;
                const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
                UNIT_ASSERT(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).empty());
            }
        }
    }

    Y_UNIT_TEST(RuntimeCollectionMembershipEmitsTypedTerminalOperations) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (bool ansi : {false, true}) for (bool empty : {false, true}) for (bool nullable : {false, true}) {
            TExpressionArena arena;
            const TType item{K::Int32, nullable};
            auto values = std::make_shared<TTupleValue>();
            if (!empty) {
                values->Items.push_back({item, std::int64_t{2}});
                if (nullable) values->Items.push_back({item, {}});
            }
            const auto list = arena.Literal(arena.ListType(item), TTuple(values));
            std::vector<TExprId> fixed;
            for (const auto& value : values->Items) fixed.push_back(arena.Literal(value.Type, value.Data));
            const auto collection = arena.Collection(fixed, true);
            for (const auto lookup : {arena.Literal({K::Int64, true}, {}), arena.Literal({K::Int64, true}, std::int64_t{2})})
                CheckScalar(arena, arena.InCollection(lookup, collection, ansi), "fixed collection with optional lookup");
            for (const double value : {2.0, 2.5, 2147483648.0})
                CheckScalar(arena, arena.InCollection(arena.Literal({K::Double}, value), list, ansi), "dynamic list with checked numeric key");
            CheckScalar(arena, arena.InCollection(arena.Literal({K::Int64, true}, {}), list, ansi), "NULL lookup into runtime list");
            CheckScalar(arena, arena.InCollection(arena.Literal({K::Int64, true}, std::int64_t{2}), list, ansi), "optional lookup into runtime list");
        }
    }

    Y_UNIT_TEST(NullableStorageStringKernelsRespectBooleanUseContexts) {
        using namespace NFrontend;
        using O = EOpcode;
        TExpressionArena arena;
        const auto text = arena.Column(1, {ETypeKind::Utf8, true});
        const auto needle = arena.Literal({ETypeKind::Utf8}, std::string("a"));
        const auto flag = arena.Column(2, {ETypeKind::Bool, true});
        for (const auto operation : {O::StartsWith, O::EndsWith, O::Contains}) {
            const auto string = arena.Binary(operation, text, needle);
            UNIT_ASSERT(arena[string].Type.OptionalDepth);
            for (const auto predicate : {string, arena.Binary(O::And, string, flag), arena.Binary(O::Or, flag, string)}) {
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate));
                TExprContext ctx;
                const auto lowered = LowerNativeOlapPredicate(arena, predicate, ctx, {});
                UNIT_ASSERT(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapApply"); }).empty());
            }
            const TExprId alternatives[]{string, arena.Literal({ETypeKind::Bool}, true)};
            const auto negated = arena.Unary(O::Not, string);
            const auto value = arena.Coalesce(alternatives);
            // String kernels preserve optional results. NOT can use a truth
            // boundary adapter when only selected rows are observable.
            for (const auto predicate : {negated, value, arena.Binary(O::Or, string, negated),
                    arena.Binary(O::Or, negated, string), arena.Binary(O::And, string, value),
                    arena.Unary(O::Not, arena.Binary(O::Or, string, flag))}) {
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate));
                UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, predicate, true));
            }
            // A nullable NOT used as a value still requires scalar apply.
            const TExprId scalarAlternatives[]{negated, arena.Literal({ETypeKind::Bool}, true)};
            const auto scalarUse = arena.Coalesce(scalarAlternatives);
            UNIT_ASSERT(!CanLowerNativeOlapPredicate(arena, scalarUse));
            UNIT_ASSERT(CanLowerNativeOlapPredicate(arena, scalarUse, true));
        }
    }

    Y_UNIT_TEST(GeneratedWriteExpressionsStayNativeUntilEmission) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        const auto types = f.ExprCtx.TypeNodes.size();
        auto query = CompileNativeToRbo("INSERT INTO t (payload) VALUES (7) RETURNING id, g", [](auto, auto) {
            TWriteSchema writes{{"id"}, {{"id", EColumnDefault::Sequence}}, false};
            writes.GeneratedColumns = {{"g", "id * 10 + COALESCE(payload, 0)"}};
            return TNativeTable{NFrontend::TCatalogTable{"t", {{"id", {ETypeKind::Int32}}, {"payload", {ETypeKind::Int32, true}},
                {"g", {ETypeKind::Int32, true}}}, std::move(writes)}, TRboTable{"/Root/t", "1:2", "", "3"}, NYql::EStorageType::RowStorage};
        }, f.ExprCtx);
        auto& root = *query.Roots.front();
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        const auto& effect = CastOperator<TOpTableEffect>(*root.GetInput());
        UNIT_ASSERT_VALUES_EQUAL(effect.Options.GeneratedColumns.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(effect.Options.GeneratedColumns[0].first, "g");
        const auto lambda = LowerRowExpressionLambda(*effect.Options.GeneratedColumns[0].second, f.ExprCtx, f.Pos);
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*lambda));
        UNIT_ASSERT(!NeedsRboCompatibilityLowering(lambda));
        const auto members = FindNodes(lambda, [](const auto& node) { return node->IsCallable("Member"); });
        UNIT_ASSERT_VALUES_EQUAL(members.size(), 2);
        for (const auto& member : members) UNIT_ASSERT(member->Tail().IsAtom("id") || member->Tail().IsAtom("payload"));
    }

    Y_UNIT_TEST(HandoffOwnsOrderedResultsParametersAndExecutionMetadata) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        const auto types = f.ExprCtx.TypeNodes.size();
        auto query = CompileNativeToRbo(R"(
            DECLARE $p AS Int64;
            SELECT value AS z, id AS a FROM t WHERE id = $p;
            SELECT id AS second FROM t;
        )", [](const TTableAddress& address, TSourcePosition) {
            const std::string_view name = address.Name;
            UNIT_ASSERT_VALUES_EQUAL(name, "t");
            return TNativeTable{NFrontend::TCatalogTable{"t", {{"id", {ETypeKind::Int64}}, {"value", {ETypeKind::String, true}}}},
                TRboTable{"/Root/t", "1:2", "", "3"}, NYql::EStorageType::RowStorage};
        }, f.ExprCtx);
        UNIT_ASSERT_VALUES_EQUAL(query.Roots.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(query.Execution.TableOperations.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(query.Execution.TableOperations.front().GetTable(), "/Root/t");
        UNIT_ASSERT_VALUES_EQUAL(query.Execution.TableOperations.front().GetOperation(), static_cast<ui32>(NYql::TYdbOperation::Select));
        UNIT_ASSERT(query.Execution.TableOperations.front().HasPosition());
        UNIT_ASSERT_VALUES_EQUAL(query.Execution.TableOperations.front().GetPosition().GetRow(), 3);
        UNIT_ASSERT(query.Execution.DiscardResults == (TVector<bool>{false, false}));
        UNIT_ASSERT(!query.Execution.HasUncommittedChangesRead);
        UNIT_ASSERT_VALUES_EQUAL(query.Parameters.size(), 1);
        UNIT_ASSERT(query.Parameters.at("p") == TParameterType(TType{ETypeKind::Int64}));
        UNIT_ASSERT_VALUES_EQUAL(query.Roots[0]->GetColumns().Items()[0].second, "z");
        UNIT_ASSERT_VALUES_EQUAL(query.Roots[0]->GetColumns().Items()[1].second, "a");
        UNIT_ASSERT_VALUES_EQUAL(query.Roots[1]->GetColumns().Items()[0].second, "second");
        auto moved = std::move(query);
        for (const auto& root : moved.Roots) {
            for (const auto& item : *root) for (const auto& expression : item.Current->GetExpressions()) {
                UNIT_ASSERT(expression.get().IsNative());
                UNIT_ASSERT(expression.get().PlanProps == &root->PlanProps);
            }
            UNIT_ASSERT(ComputeTypesWithoutBackend(*root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
    }

    Y_UNIT_TEST(HandoffIncludesTablesReadOnlyBySubqueries) {
        NTests::TIdTestContext f;
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        const auto types = f.ExprCtx.TypeNodes.size();
        auto query = CompileNativeToRbo("SELECT (SELECT MAX(id) FROM t), EXISTS(SELECT id FROM u)",
            [](const NFrontend::TTableAddress& address, NFrontend::TSourcePosition) {
                const std::string_view name = address.Name;
                const TString path = TStringBuilder() << "/Root/" << name;
                return TNativeTable{NFrontend::TCatalogTable{std::string(name), {{"id", {NFrontend::ETypeKind::Int64}}}},
                    TRboTable{path, "1:2", "", "3"}, NYql::EStorageType::ColumnStorage};
            }, f.ExprCtx);
        THashSet<TString> tables;
        for (const auto& operation : query.Execution.TableOperations) tables.insert(operation.GetTable());
        UNIT_ASSERT(tables == (THashSet<TString>{"/Root/t", "/Root/u"}));
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
    }

    Y_UNIT_TEST(TableLookupStagesRemainNativeUntilEmission) {
        NTests::TIdTestContext f;
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        const auto types = f.ExprCtx.TypeNodes.size();
        auto roots = CompileNativeToRbo("SELECT 1ul AS key", NoTables, f.ExprCtx).Roots;
        auto& source = *roots.front();
        UNIT_ASSERT(ComputeTypesWithoutBackend(source, f.RboCtx) == IGraphTransformer::TStatus::Ok);
        const auto key = *source.GetInput()->GetOutputIUs().begin();
        const auto fetched = source.PlanProps.InfoUnitRegistry.Add(TInfoUnit("value"));
        TRboTable table("/Root/table", "1:2", "", "3");
        table.NativeCatalog.emplace(NFrontend::TCatalogTable{std::string(table.Path), {
            {"value", {NFrontend::ETypeKind::Int32, true}}}});
        TOpTableLookup::TLookupKeys keys;
        keys.Append(key, "key");
        auto lookup = MakeIntrusive<TOpTableLookup>(source.GetInput(), source.Pos, table, TUnorderedIUs{fetched}, std::move(keys));
        auto root = MakeIntrusive<TOpRoot>(lookup, source.Pos, TOrderedIUs<TString>{{fetched, "value"}});
        root->PlanProps = std::move(source.PlanProps);
        for (const auto& item : *root) item.Current->BindExpressionPlanProps(&root->PlanProps);
        root->ComputeParents();
        UNIT_ASSERT(ComputeTypesWithoutBackend(*root, f.RboCtx) == IGraphTransformer::TStatus::Ok);
        TAssignStagesStage().RunStage(*root, f.RboCtx);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        TIntrusivePtr<TStreamLookupConnection> stream;
        for (const auto& [edge, connections] : root->PlanProps.StageGraph.Connections)
            for (const auto& connection : connections) if (IsConnection<TStreamLookupConnection>(connection))
                stream = static_cast<TStreamLookupConnection*>(connection.Get());
        UNIT_ASSERT(stream && !stream->InputType);
        UNIT_ASSERT(stream->Keys.front().second == lookup->GetInput()->GetNativeIUType(key));
        const TPhysicalNames names(root->PlanProps.InfoUnitRegistry);
        const auto emitted = stream->BuildConnection(f.ExprCtx.NewCallable(f.Pos, "DqStage", {}), f.Pos, f.ExprCtx, names);
        const auto backend = NYql::NNodes::TKqpCnStreamLookup(emitted);
        UNIT_ASSERT_VALUES_EQUAL(backend.Table().Path().Value(), table.Path);
        UNIT_ASSERT_VALUES_EQUAL(backend.Table().PathId().Value(), table.PathId);
        UNIT_ASSERT_VALUES_EQUAL(backend.Table().Version().Value(), table.Version);
        UNIT_ASSERT(f.ExprCtx.NodeAllocationCounter > nodes);
        UNIT_ASSERT(f.ExprCtx.TypeNodes.size() > types);
        UNIT_ASSERT(lookup->NativeSchema);
    }

    Y_UNIT_TEST(RowParametersStayNativeThroughTypingAndCopy) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto allocated = f.ExprCtx.NodeAllocationCounter;
        auto roots = CompileNativeToRbo(
            "DECLARE $p AS List<Struct<value:String?,id:Int32>>; SELECT p.id,p.value FROM AS_TABLE($p) p WHERE p.id > 0",
            NoTables, f.ExprCtx).Roots;
        auto& root = *roots.front();
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, allocated);
        TIntrusivePtr<TOpEmptySource> source;
        for (const auto& item : root) if (item.Current->Kind == EOperator::EmptySource) source = CastOperator<TOpEmptySource>(item.Current);
        UNIT_ASSERT(source && source->NativeInput);
        UNIT_ASSERT_VALUES_EQUAL(source->NativeInput->Name, "$p");
        for (const auto& [id, field] : source->NativeInput->Fields.Items()) {
            UNIT_ASSERT_VALUES_EQUAL(root.PlanProps.InfoUnitRegistry.Get(id).GetAlias(), "p");
            const auto column = MakeColumnAccess(id, *source, source->Pos, &root.PlanProps);
            UNIT_ASSERT(column.IsNative() && column.GetNativeType() == field.Type);
        }
        auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("row parameter invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
        source->ComputeMetadata(context, root.PlanProps);
        UNIT_ASSERT(source->Props.Metadata->LogicalCard == ELogicalCardinality::ZeroOrMore);
        TSubstitutions renames;
        const auto copy = source->Copy(root.PlanProps, renames);
        UNIT_ASSERT(copy);
        const auto& copied = CastOperator<TOpEmptySource>(*copy);
        UNIT_ASSERT(copied.NativeInput);
        TParameterType::TRows fields;
        for (const auto& [id, field] : source->NativeInput->Fields.Items()) {
            UNIT_ASSERT(renames.At(id) != id);
            UNIT_ASSERT(copied.NativeInput->Fields.At(renames.At(id)) == field);
            fields.push_back(field);
        }
        for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions())
            UNIT_ASSERT(expression.get().IsNative());
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, allocated);
        const auto lowered = LowerNativeParameter(copied.NativeInput->Name, TParameterType(std::move(fields)), f.ExprCtx, {});
        UNIT_ASSERT(lowered->IsCallable("Parameter"));
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*lowered));
        UNIT_ASSERT(!NeedsRboCompatibilityLowering(lowered));
        UNIT_ASSERT_VALUES_EQUAL(lowered->Head().Content(), "$p");
        const auto* row = lowered->GetTypeAnn()->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
        UNIT_ASSERT(ImportNativeType(*row->FindItemType("id")) == (TType{ETypeKind::Int32}));
        UNIT_ASSERT(ImportNativeType(*row->FindItemType("value")) == (TType{ETypeKind::String, true}));
    }

    Y_UNIT_TEST(NativeScalarInspectionAndRebindingDoNotGenerateBackendNodes) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto source = f.Id("source"), renamed = f.Id("renamed");
        auto arena = std::make_shared<TExpressionArena>();
        const auto expression = [&](TExprId root) {
            return TExpression::FromNative(arena, root, &f.Props);
        };
        const auto allocated = f.ExprCtx.NodeAllocationCounter;
        const TType integer{ETypeKind::Int32};
        const auto column = arena->Column(source, integer);
        const auto direct = expression(column);
        const auto lifted = expression(arena->Convert(column, integer.Optional()));
        UNIT_ASSERT_VALUES_EQUAL(direct.GetColumnId().value(), source);
        UNIT_ASSERT(!lifted.GetColumnId());
        UNIT_ASSERT_VALUES_EQUAL(lifted.GetColumnId(true).value(), source);
        UNIT_ASSERT(!expression(arena->Convert(column, {ETypeKind::Int64, true})).GetColumnId(true));
        UNIT_ASSERT(!expression(arena->Literal(integer.Optional(), {})).GetColumnId(true));
        UNIT_ASSERT(!expression(arena->Parameter("p", integer)).GetColumnId(true));
        const TType items[]{integer, {ETypeKind::Uint64}};
        const auto state = arena->TupleType(items);
        const auto stateColumn = arena->Column(source, state);
        UNIT_ASSERT_VALUES_EQUAL(expression(arena->Convert(stateColumn, state.Optional())).GetColumnId(true).value(), source);

        const auto rebound = lifted.ApplyRenames(TSubstitutions{{source, renamed}});
        UNIT_ASSERT_VALUES_EQUAL(rebound.GetColumnId(true).value(), renamed);
        UNIT_ASSERT(rebound.GetRawInputIUs() == TUnorderedIUs{renamed});
        const auto nullable = direct.WithNativeColumnTypes([&](TInfoUnitId id) {
            UNIT_ASSERT_VALUES_EQUAL(id, source);
            return integer.Optional();
        });
        UNIT_ASSERT(nullable.GetNativeType() == integer.Optional());
        UNIT_ASSERT(direct.GetNativeType() == integer);
        UNIT_ASSERT_VALUES_EQUAL(lifted.GetColumnId(true).value(), source);
        static_assert(!std::is_constructible_v<TExpression, NYql::TExprNode::TPtr, TExprContext*>);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, allocated);
    }

    Y_UNIT_TEST(BackendScalarAnnotationsDoNotLeakIntoNativeExpressions) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto column = f.Id("value");
        auto arena = std::make_shared<TExpressionArena>();
        const TType type{ETypeKind::Int32};
        const auto input = arena->Column(column, type);
        const auto native = TExpression::FromNative(arena, arena->Convert(input, type.Optional()), &f.Props);
        auto first = LowerExpressionLambda(native, f.ExprCtx);
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*first));
        UNIT_ASSERT(!NeedsRboCompatibilityLowering(first));
        UNIT_ASSERT_VALUES_EQUAL(FromString<TInfoUnitId>(first->Tail().Head().Tail().Content()), column);
        first->SetTypeAnn(nullptr);
        first->Tail().SetTypeAnn(nullptr);
        auto second = LowerExpressionLambda(native, f.ExprCtx);
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*second));
        UNIT_ASSERT(first != second);
        UNIT_ASSERT(first->Head().HeadPtr() != second->Head().HeadPtr());

        UNIT_ASSERT(native.GetNativeType() == type.Optional());

        const auto rebound = native.ApplyRenames(TSubstitutions{{column, f.Id("copy")}});
        const auto lowered = LowerExpressionLambda(rebound, f.ExprCtx);
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*lowered));
        UNIT_ASSERT_VALUES_EQUAL(FromString<TInfoUnitId>(lowered->Tail().Head().Tail().Content()), rebound.GetColumnId(true).value());
        UNIT_ASSERT_VALUES_EQUAL(native.GetColumnId(true).value(), column);
    }

    Y_UNIT_TEST(JsonValuesExecuteWithoutSemanticLowering) {
        using namespace NFrontend;
        using K = ETypeKind;
        struct TCase { TString Json; const char* Path; std::optional<TString> Expected; };
        const TCase cases[]{
            {R"({"a":"alpha"})", "$.a", "alpha"}, {R"({"a.b.c":"dotted"})", "$.\"a.b.c\"", "dotted"},
            {R"({"a.b.c":"single"})", "$.'a.b.c'", "single"}, {R"({"a":12.5})", "$.a", "12.5"},
            {R"({"a":true})", "$.a", "true"}, {R"({"a":""})", "$.a", ""},
            {R"({"a":null})", "$.a", std::nullopt}, {"{}", "$.a", std::nullopt},
            {"{}", "strict $.a", std::nullopt}, {R"({"a":{"x":1}})", "$.a", std::nullopt},
            {R"({"a":[1,2]})", "$.a", std::nullopt}, {R"({"a":[1,2]})", "$.a[*]", std::nullopt},
            {R"({"a":[1,2]})", "$.a[1]", "2"}, {R"([{"a":"one"}])", "lax $.a", "one"},
            {R"([{"a":"one"}])", "strict $.a", std::nullopt}, {"42", "$", "42"},
            {"null", "$", std::nullopt}, {R"({"a":"é"})", "$.a", "é"},
            {R"({"a":"x\u0000y"})", "$.a", TString("x\0y", 3)}, {R"({"a":"one"})", "$.*", "one"}};
        for (const auto kind : {K::Json, K::JsonDocument}) for (const bool nullable : {false, true})
            for (const bool present : {false, true}) {
                if (!nullable && !present) continue;
                for (const auto& test : cases) {
                    TExpressionArena arena;
                    const auto input = arena.Parameter("json", {kind, nullable});
                    const auto root = arena.JsonValue(input, test.Path);
                    TExprContext ctx;
                    const auto lowered = LowerNativeLambda(arena, root, ctx, {});
                    UNIT_ASSERT(AreAllNodesTypeAnnotated(*lowered));
                    UNIT_ASSERT(!NeedsRboCompatibilityLowering(lowered));
                    NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
                    auto registry = setup.FunctionRegistry->Clone(); NMiniKQL::FillStaticModules(*registry);
                    setup.FunctionRegistry = registry;
                    setup.PgmBuilder.Reset(new NMiniKQL::TProgramBuilder(*setup.Env, *registry));
                    auto& pb = *setup.PgmBuilder;
                    TString bytes = test.Json;
                    if (kind == K::JsonDocument) {
                        const auto binary = NBinaryJson::SerializeToBinaryJson(bytes);
                        UNIT_ASSERT(std::holds_alternative<NBinaryJson::TBinaryJson>(binary));
                        const auto& data = std::get<NBinaryJson::TBinaryJson>(binary);
                        bytes = TString(data.Data(), data.Size());
                    }
                    auto value = kind == K::Json ? pb.NewDataLiteral<NUdf::EDataSlot::Json>(NUdf::TStringRef(bytes.data(), bytes.size())) :
                        pb.NewDataLiteral<NUdf::EDataSlot::JsonDocument>(NUdf::TStringRef(bytes.data(), bytes.size()));
                    if (nullable) value = present ? pb.NewOptional(value) : pb.NewEmptyOptional(pb.NewOptionalType(value.GetStaticType()));
                    NYql::NCommon::TMkqlCommonCallableCompiler compiler;
                    NYql::NCommon::TMkqlBuildContext build(compiler, pb, ctx);
                    build.Parameters = pb.NewStruct({{"$json", value}});
                    auto graph = setup.BuildGraph(NYql::NCommon::MkqlBuildLambda(*lowered, build, {pb.NewEmptyStruct()}));
                    const auto actual = graph->GetValue();
                    const auto expected = present ? test.Expected : std::nullopt;
                    UNIT_ASSERT_VALUES_EQUAL_C(bool(actual), bool(expected), test.Path);
                    if (expected) UNIT_ASSERT_VALUES_EQUAL_C(TString(actual.AsStringRef()), *expected, test.Path);
                }
            }
    }

    Y_UNIT_TEST(JsonSelectorValidationAgreesWithTheOriginalParser) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto input = arena.Parameter("json", {ETypeKind::Json});
        const auto check = [&](const std::string& path) {
            NYql::TIssues issues;
            const bool original = bool(NYql::NJsonPath::ParseJsonPath(TString(path), issues, 7));
            bool native = false;
            try { arena.JsonValue(input, path); native = true; } catch (const TFrontendError&) {}
            UNIT_ASSERT_VALUES_EQUAL_C(native, original, path);
        };
        for (const auto* path : {"$", " $.a ", "lax $.a", "strict $.a", "$.'a.b'", "$.\"a.b\"", "$.\"\"", "$.é",
                "$.*[0].x", "$.a[*].b", "$[4294967295]", "$[01]", "$.true", "$.a$b", "$.\"é\"",
                "", "$.", "$[", "$[]", "$..x", "$[1", "$[1]]", "lax$", "strict$", "$.\"unterminated"}) check(path);
        const char* steps[]{".a", ".true", ".a$b", ".\"a.b\"", ".'é'", "[0]", "[*]", ".*"};
        for (const auto* mode : {"", "lax ", "strict "}) for (const auto* space : {"", " ", "\n"})
            for (const auto* first : steps) for (const auto* second : steps)
                check(std::string(mode) + "$" + space + first + space + second + space);
    }

    Y_UNIT_TEST(PlannerPragmasApplyOnceAtRboHandoffWithoutBeingDropped) {
        NTests::TIdTestContext f;
        auto compile = [&](const TString& sql) {
            return CompileNativeToRbo(std::string_view(sql.data(), sql.size()), NoTables, f.ExprCtx, {}, {}, f.Config.Get()).Roots;
        };
        auto checkBool = [&](const char* name, auto& option) {
            for (const bool value : {false, true}) {
                auto roots = compile(TStringBuilder() << "PRAGMA ydb." << name << "='" << (value ? "true" : "false") << "'; SELECT 1");
                UNIT_ASSERT_VALUES_EQUAL_C(option.Get().GetRef(), value, name);
                UNIT_ASSERT_VALUES_EQUAL(roots.size(), 1);
                UNIT_ASSERT(roots.front()->PlanProps.NativeExpressions);
                for (const auto& item : *roots.front()) for (const auto& expression : item.Current->GetExpressions())
                    UNIT_ASSERT(expression.get().IsNative());
            }
        };
        checkBool("OptDisableAutoIndexSelection", f.Config->OptDisableAutoIndexSelection);
        checkBool("EnableAutoIndexSelectionForIndexLookupJoin", f.Config->EnableAutoIndexSelectionForIndexLookupJoin);
        checkBool("OptDisableTopSort", f.Config->OptDisableTopSort);
        checkBool("OptEnablePredicateExtract", f.Config->OptEnablePredicateExtract);
        checkBool("OptShuffleElimination", f.Config->OptShuffleElimination);
        checkBool("OptShuffleEliminationWithMap", f.Config->OptShuffleEliminationWithMap);
        checkBool("OptShuffleEliminationForAggregation", f.Config->OptShuffleEliminationForAggregation);
        checkBool("EnableNewRBOPhysicalStagePeephole", f.Config->EnableNewRBOPhysicalStagePeephole);
        checkBool("WindowFunctionsV2", f.Config->WindowFunctionsV2);
        checkBool("OptEnableOlapPushdown", f.Config->OptEnableOlapPushdown);
        checkBool("OptEnableOlapPushdownAggregate", f.Config->OptEnableOlapPushdownAggregate);
        checkBool("OptEnableOlapPushdownProjections", f.Config->OptEnableOlapPushdownProjections);
        checkBool("OptEnableOlapPushdownRegexp", f.Config->OptEnableOlapPushdownRegexp);
        checkBool("DqHashOperatorsUseBlocks", f.Config->DqHashOperatorsUseBlocks);
        checkBool("UseBlockHashJoin", f.Config->UseBlockHashJoin);
        checkBool("DisableBlockExecution", f.Config->DisableBlockExecution);
        compile("PRAGMA ydb.OptFallbackToLegacyOptimizer='false'; SELECT 1");
        UNIT_ASSERT(!f.Config->OptFallbackToLegacyOptimizer.Get().GetRef());
        f.Config->SetDefaultCostBasedOptimizationLevel(3);
        for (const auto& [pragma, expected] : std::initializer_list<std::pair<const char*, ui32>>{
                {"ydb.CostBasedOptimizationLevel='4'", 4}, {"ydb.CostBasedOptimization='off'", 0},
                {"ydb.CostBasedOptimization='on'", std::numeric_limits<ui32>::max()}, {"ydb.CostBasedOptimization='auto'", 3},
                {"ydb.CostBasedOptimization='off'; PRAGMA ydb.CostBasedOptimizationLevel='4'", 4}}) {
            compile(TStringBuilder() << "PRAGMA " << pragma << "; SELECT 1");
            UNIT_ASSERT_VALUES_EQUAL(f.Config->CostBasedOptimizationLevel.Get().GetRef(), expected);
        }
        for (const char* mode : {"off", "map", "dict", "grace", "graceandself"}) {
            compile(TStringBuilder() << "PRAGMA ydb.HashJoinMode='" << mode << "'; SELECT 1");
            UNIT_ASSERT(f.Config->GetHashJoinMode() == FromString<NYql::NDq::EHashJoinMode>(mode));
        }
        compile("PRAGMA ydb.DqChannelVersion='1'; PRAGMA ydb.OptimizerHints='Rows(t # 123) JoinType(t u Shuffle)'; SELECT 1");
        UNIT_ASSERT_VALUES_EQUAL(f.Config->DqChannelVersion.Get().GetRef(), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.Config->OptimizerHints.Get().GetRef().GetUnappliedString().size(), 2);
        compile("PRAGMA ydb.OptimizerHints='Rows(t # 123)'; SELECT 1; PRAGMA ydb.OptimizerHints; SELECT 2");
        UNIT_ASSERT(f.Config->OptimizerHints.Get().GetRef().GetUnappliedString().empty());

        f.Config->OptDisableAutoIndexSelection = false;
        f.Config->FreezeDefaults();
        compile("PRAGMA ydb.OptDisableAutoIndexSelection; SELECT 1");
        UNIT_ASSERT(f.Config->IsAutoIndexSelectionDisabled());
        f.Config->Restore();
        UNIT_ASSERT(!f.Config->IsAutoIndexSelectionDisabled());
        // Invalid overwritten hints must still fail, before any option is applied.
        UNIT_ASSERT_EXCEPTION(compile("PRAGMA ydb.OptDisableAutoIndexSelection='true'; PRAGMA ydb.OptimizerHints='broken'; "
            "PRAGMA ydb.OptimizerHints='Rows(t # 5)'; SELECT 1"), NFrontend::TFrontendError);
        UNIT_ASSERT(!f.Config->IsAutoIndexSelectionDisabled());
        UNIT_ASSERT_EXCEPTION(compile("PRAGMA ydb.OptDisableAutoIndexSelection='true'; PRAGMA ydb.HashJoinMode='unknown'; SELECT 1"),
            NFrontend::TFrontendError);
        UNIT_ASSERT(!f.Config->IsAutoIndexSelectionDisabled());
        UNIT_ASSERT_EXCEPTION(CompileNativeToRbo("PRAGMA ydb.WindowFunctionsV2='false'; SELECT 1", NoTables, f.ExprCtx).Roots,
            NFrontend::TUnsupportedFeature);
    }

    Y_UNIT_TEST(NativeSelectivityUsesKeysAndTypedStatisticsWithoutLowering) {
        using namespace NFrontend;
        using O = EOpcode;
        using K = ETypeKind;
        TExpressionArena arena;
        TOptimizerStatistics stats;
        stats.Nrows = 1000;
        stats.KeyColumns = MakeIntrusive<TOptimizerStatistics::TKeyColumns>(TVector<TString>{"10"});
        const auto key = arena.Column(10, {K::Int64});
        const auto value = arena.Column(20, {K::Int64, true});
        const auto one = arena.Literal({K::Int64}, std::int64_t{1});
        const auto parameter = arena.Parameter("p", {K::Int64});
        const auto estimate = [&](TExprId root, double expected) {
            const auto size = arena.Size();
            UNIT_ASSERT_DOUBLES_EQUAL(ComputeNativeSelectivity(arena, root, stats), expected, 1e-12);
            UNIT_ASSERT_VALUES_EQUAL(arena.Size(), size);
        };
        const auto keyEq = arena.Binary(O::Equal, key, one), valEq = arena.Binary(O::Equal, value, one);
        estimate(keyEq, 0.001);
        estimate(valEq, 0.1);
        estimate(arena.Binary(O::Equal, parameter, key), 0.001);
        estimate(arena.Binary(O::Less, key, parameter), 0.001);
        estimate(arena.Binary(O::Equal, key, value), 0.3);
        estimate(arena.Binary(O::And, keyEq, valEq), 0.0001);
        estimate(arena.Binary(O::Or, keyEq, valEq), 0.101);
        estimate(arena.Binary(O::NotEqual, key, one), 0.002);
        estimate(arena.Binary(O::Equal, arena.Cast(key, {K::Int32}), one), 0.001);
        estimate(arena.Unary(O::IsNull, key), 0);
        estimate(arena.Unary(O::IsNull, value), 1);
        estimate(arena.Unary(O::IsNotNull, value), 1);
        estimate(arena.Unary(O::IsNull, arena.Column(40, {K::Null})), 1);
        const auto nil = arena.Literal({K::Bool, true}, {});
        const auto yes = arena.Literal({K::Bool}, true), no = arena.Literal({K::Bool}, false);
        estimate(nil, 0);
        estimate(arena.Unary(O::Not, nil), 0);
        estimate(arena.Binary(O::And, nil, yes), 0);
        estimate(arena.Binary(O::Or, nil, yes), 1);
        estimate(arena.Unary(O::Not, arena.Binary(O::Or, nil, no)), 0);
        const TExprId coalesceTrue[]{nil, yes}, coalesceFalse[]{nil, no};
        estimate(arena.Coalesce(coalesceTrue), 1);
        estimate(arena.Coalesce(coalesceFalse), 0);
        estimate(arena.Binary(O::Equal, value, arena.Cast(arena.Null(), {K::Int64})), 0);
        const auto text = arena.Column(30, {K::String});
        const auto needle = arena.Literal({K::String}, std::string("abc"));
        estimate(arena.Binary(O::Contains, text, needle), 0.1);
        estimate(arena.Binary(O::StartsWith, text, needle), 0.5);

        // The common defaults match the old estimator; it is used only as a
        // test oracle after direct lowering, never by the native visitor.
        TExprContext context;
        const auto less = arena.Binary(O::Less, key, one);
        const auto textEq = arena.Binary(O::Equal, text, needle);
        // The legacy frontend removes optional Boolean lifts before statistics.
        // Use equally nullable operands for compound oracle predicates so direct
        // backend lowering does not introduce an unnormalized Just(predicate).
        const auto requiredValue = arena.Column(20, {K::Int64});
        const auto requiredEq = arena.Binary(O::Equal, requiredValue, one);
        const auto optionalKeyEq = arena.Binary(O::Equal, arena.Column(10, {K::Int64, true}), one);
        for (const auto root : {keyEq, valEq, less, arena.Binary(O::Equal, key, value),
                arena.Binary(O::And, keyEq, requiredEq), arena.Binary(O::Or, keyEq, requiredEq),
                arena.Binary(O::And, optionalKeyEq, valEq), arena.Binary(O::Or, optionalKeyEq, valEq),
                arena.Binary(O::Or, less, requiredEq),
                arena.Binary(O::And, arena.Binary(O::Or, keyEq, requiredEq), textEq),
                arena.Binary(O::Or, arena.Binary(O::And, keyEq, requiredEq), textEq),
                arena.Binary(O::Or, arena.Binary(O::Or, less, arena.Binary(O::Less, requiredValue, one)),
                    arena.Binary(O::Less, text, needle))}) {
            const auto lambda = LowerNativeLambda(arena, root, context, {});
            const auto legacy = TPredicateSelectivityComputer(std::make_shared<TOptimizerStatistics>(stats))
                .Compute(NNodes::TCoLambda(lambda).Body());
            UNIT_ASSERT_DOUBLES_EQUAL(ComputeNativeSelectivity(arena, root, stats), legacy, 1e-12);
        }
        // A deep DAG with repeated children must not become an exponential tree
        // or consume the native call stack. Unrelated arena roots are untouched.
        auto deep = valEq;
        for (size_t i = 0; i < 20000; ++i) deep = arena.Binary(O::And, deep, deep);
        estimate(deep, 0.1);
    }

    Y_UNIT_TEST(NativeSelectivityMatchesNormalizedLegacyPolicy) {
        using namespace NFrontend;
        using O = EOpcode;
        using K = ETypeKind;
        TExpressionArena arena;
        const auto a = arena.Column(10, {K::Int64}), b = arena.Column(20, {K::Int64, true});
        const auto text = arena.Column(30, {K::String}), unknown = arena.Column(40, {K::Bool});
        const auto one = arena.Literal({K::Int64}, std::int64_t{1});
        const auto three = arena.Literal({K::Int64}, std::int64_t{3});
        const auto parameter = arena.Parameter("p", {K::Int64});
        const auto needle = arena.Literal({K::String}, std::string("x"));
        const auto decimal = arena.Column(50, {K::Decimal, true, 7, 2});
        const auto date = arena.Cast(arena.Column(60, {K::Utf8, true}), {K::Date});
        TVector<TExprId> predicates{unknown, arena.Unary(O::IsNull, b), arena.Unary(O::IsNotNull, b),
            arena.Binary(O::Contains, text, needle), arena.Binary(O::StartsWith, text, needle),
            arena.Binary(O::Contains, text, arena.Parameter("s", {K::String})),
            arena.Like(text, needle), arena.Like(text, needle, {}, {}, false),
            arena.Regex(text, needle, true), arena.Regex(text, needle, false),
            arena.Binary(O::GreaterEqual, decimal, arena.Literal({K::Decimal, false, 35, 2}, TDecimal{99})),
            arena.Binary(O::LessEqual, decimal, arena.Literal({K::Decimal, false, 35, 2}, TDecimal{149})),
            arena.Binary(O::GreaterEqual, date, arena.Literal({K::Date}, std::uint64_t{10304})),
            arena.Binary(O::LessEqual, date, arena.Literal({K::Date}, std::uint64_t{10344})),
            arena.Binary(O::GreaterEqual, a, arena.Literal({K::Int64}, std::int64_t{10})),
            arena.Binary(O::Equal, arena.Cast(a, {K::Int32}), one),
            arena.Binary(O::Equal, arena.Binary(O::Add, a, b), one)};
        for (auto op : {O::Equal, O::NotEqual, O::Less, O::LessEqual, O::Greater, O::GreaterEqual}) {
            for (auto right : {one, three, parameter, b}) predicates.push_back(arena.Binary(op, a, right));
            predicates.push_back(arena.Binary(op, b, one));
            predicates.push_back(arena.Binary(op, three, a));
        }
        TVector<TExprId> cases = predicates;
        for (auto predicate : predicates) cases.push_back(arena.Unary(O::Not, predicate));
        for (auto left : predicates) for (auto right : predicates) {
            cases.push_back(arena.Binary(O::And, left, right));
            cases.push_back(arena.Binary(O::Or, left, right));
            cases.push_back(arena.Unary(O::Not, arena.Binary(O::And, left, right)));
            cases.push_back(arena.Unary(O::Not, arena.Binary(O::Or, left, right)));
        }
        size_t mismatches = 0;
        TStringStream failures;
        for (unsigned mode = 0; mode < 3; ++mode) {
            TOptimizerStatistics stats;
            stats.Nrows = 1000;
            stats.KeyColumns = MakeIntrusive<TOptimizerStatistics::TKeyColumns>(TVector<TString>{"10"});
            if (mode) {
                stats.ColumnStatistics = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();
                for (const auto name : {"10", "20"}) {
                    auto& source = stats.ColumnStatistics->Data[name];
                    source.Type = "Int64";
                    if (mode == 2) {
                        source.CountMinSketch.reset(TCountMinSketch::Create());
                        auto histogram = std::make_shared<TEqWidthHistogram>(100, EHistogramValueType::Int64);
                        histogram->InitializeBuckets(i64{0}, i64{100});
                        for (i64 i = 0; i < 1000; ++i) {
                            const i64 value = i % 100;
                            source.CountMinSketch->Count(reinterpret_cast<const char*>(&value), sizeof(value));
                            histogram->AddElement(value);
                        }
                        source.EqWidthHistogramEstimator = std::make_shared<TEqWidthHistogramEstimator>(histogram);
                    }
                }
                stats.ColumnStatistics->Data["30"].Type = "String";
                stats.ColumnStatistics->Data["40"].Type = "Bool";
                stats.ColumnStatistics->Data["50"].Type = "Decimal(7,2)";
                stats.ColumnStatistics->Data["60"].Type = "Utf8";
            }
            TExprContext context;
            for (auto root : cases) {
                const auto size = arena.Size();
                const auto allocations = context.NodeAllocationCounter;
                const auto native = ComputeNativeSelectivity(arena, root, stats);
                UNIT_ASSERT_VALUES_EQUAL(arena.Size(), size);
                UNIT_ASSERT_VALUES_EQUAL(context.NodeAllocationCounter, allocations);
                const auto lambda = LowerNativeLambda(arena, root, context, {});
                // The legacy frontend flattens logical operators and pulls
                // optional Boolean lifts out before the statistics visitor.
                // Normalize only those representation details in this oracle.
                auto normalized = lambda->ChildPtr(1);
                THashMap<const NYql::TExprNode*, NYql::TExprNode::TPtr> rewritten;
                TVector<std::pair<NYql::TExprNode::TPtr, bool>> work{{normalized, false}};
                while (!work.empty()) {
                    const auto [node, ready] = work.back(); work.pop_back();
                    if (rewritten.contains(node.Get())) continue;
                    if (!ready) {
                        work.emplace_back(node, true);
                        for (const auto& child : node->Children()) work.emplace_back(child, false);
                        continue;
                    }
                    NYql::TExprNode::TListType children;
                    for (const auto& child : node->Children()) children.push_back(rewritten.at(child.Get()));
                    auto result = context.ExactChangeChildren(*node, std::move(children));
                    if (result->IsCallable("Just") &&
                        RemoveAllOptionals(result->GetTypeAnn())->GetKind() == ETypeAnnotationKind::Data &&
                        RemoveAllOptionals(result->GetTypeAnn())->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool) {
                        result = result->ChildPtr(0);
                    } else if (result->IsCallable({"And", "Or"})) {
                        NYql::TExprNode::TListType terms;
                        for (const auto& child : result->Children()) {
                            if (child->IsCallable(result->Content())) terms.insert(terms.end(), child->Children().begin(), child->Children().end());
                            else terms.push_back(child);
                        }
                        result = context.ExactChangeChildren(*result, std::move(terms));
                    }
                    rewritten.emplace(node.Get(), std::move(result));
                }
                normalized = rewritten.at(normalized.Get());
                const auto legacy = TPredicateSelectivityComputer(std::make_shared<TOptimizerStatistics>(stats))
                    .Compute(NNodes::TExprBase(normalized));
                if (std::abs(native - legacy) > 1e-12) {
                    if (mismatches++ < 20) failures << "statistics mode=" << mode << " root=" << root
                        << " native=" << native << " legacy=" << legacy << " " << normalized->Dump() << Endl;
                }
            }
        }
        UNIT_ASSERT_VALUES_EQUAL_C(mismatches, 0, failures.Str());
    }

    Y_UNIT_TEST(NativeOuterJoinFilterKeepsExistingRewriteScope) {
        using namespace NFrontend;
        using O = EOpcode;
        using K = ETypeKind;
        NTests::TIdTestContext f;
        const auto leftId = f.Id("left"), rightId = f.Id("right");
        auto arena = std::make_shared<TExpressionArena>();
        f.Props.NativeExpressions = arena;
        const auto value = arena->Column(rightId, {K::Int32, true});
        const auto zero = arena->Literal({K::Int32}, std::int64_t{0});
        const auto positive = arena->Binary(O::Greater, value, zero);
        const TExprId alternatives[]{positive, arena->Literal({K::Bool}, false)};
        for (const auto& [predicate, rewrites] : std::initializer_list<std::pair<TExprId, bool>>{
                {positive, true}, {arena->Unary(O::Not, positive), true},
                {arena->Binary(O::Equal, zero, value), true},
                {arena->Unary(O::IsNotNull, value), false},
                {arena->Binary(O::Greater, arena->Binary(O::Add, value, zero), zero), false},
                {arena->Binary(O::Greater, value, arena->Parameter("p", {K::Int32})), false},
                {arena->Binary(O::Equal, value, arena->Literal({K::Int32, true}, std::int64_t{0})), false},
                {arena->Coalesce(alternatives), false}}) {
            auto left = f.Read({leftId}), right = f.Read({rightId});
            SetNativeReadType(f, *left);
            SetNativeReadType(f, *right, {rightId});
            auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Left", TJoinIUs{});
            auto filter = MakeIntrusive<TOpFilter>(join, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            const auto allocations = f.ExprCtx.NodeAllocationCounter;
            const auto result = TPushFilterIntoJoinRule().SimpleMatchAndApply(filter, f.RboCtx, f.Props);
            UNIT_ASSERT_VALUES_EQUAL_C(join->JoinKind, rewrites ? "Inner" : "Left", predicate);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, allocations);
            UNIT_ASSERT_VALUES_EQUAL(result->Kind == EOperator::Join, rewrites);
        }
    }

    Y_UNIT_TEST(NativeOuterJoinFilterRetainsExtendedTemporalConstants) {
        using namespace NFrontend;
        // The original YQL rule's simple-constant allowlist includes all four
        // extended temporal types. Native type inspection must retain them.
        for (const auto kind : {ETypeKind::Date32, ETypeKind::Datetime64, ETypeKind::Timestamp64, ETypeKind::Interval64}) {
            NTests::TIdTestContext f;
            const auto leftId = f.Id("left"), rightId = f.Id("right");
            auto left = f.Read({leftId}), right = f.Read({rightId});
            SetNativeReadType(f, *left, {}, kind);
            SetNativeReadType(f, *right, {rightId}, kind);
            auto arena = f.Props.NativeExpressions;
            const auto predicate = arena->Binary(EOpcode::Greater, arena->Column(rightId, {kind, true}),
                arena->Literal({kind}, std::int64_t{0}));
            auto join = MakeIntrusive<TOpJoin>(left, right, f.Pos, "Left", TJoinIUs{});
            auto filter = MakeIntrusive<TOpFilter>(join, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            const auto nodes = f.ExprCtx.NodeAllocationCounter;
            const auto types = f.ExprCtx.TypeNodes.size();
            const auto result = TPushFilterIntoJoinRule().SimpleMatchAndApply(filter, f.RboCtx, f.Props);
            UNIT_ASSERT_VALUES_EQUAL(join->JoinKind, "Inner");
            UNIT_ASSERT(result->Kind == EOperator::Join);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
            UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.TypeNodes.size(), types);
        }
    }

    Y_UNIT_TEST(NativeSelectivityTraversesSharedBooleanDagWithoutExpansion) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto column = arena.Column(7, {ETypeKind::Int32});
        auto predicate = arena.Binary(EOpcode::Less, column, arena.Literal({ETypeKind::Int32}, std::int64_t{4}));
        for (size_t i = 0; i < 20000; ++i) predicate = arena.Binary(EOpcode::And, predicate, predicate);
        TOptimizerStatistics stats;
        stats.Nrows = 1000;
        stats.KeyColumns = MakeIntrusive<TOptimizerStatistics::TKeyColumns>(TVector<TString>{"7"});
        const auto size = arena.Size();
        UNIT_ASSERT_DOUBLES_EQUAL(ComputeNativeSelectivity(arena, predicate, stats), 0.001, 1e-12);
        UNIT_ASSERT_VALUES_EQUAL(arena.Size(), size);
    }

    Y_UNIT_TEST(NativeSelectivityProbesExactStorageWidths) {
        using namespace NFrontend;
        using K = ETypeKind;
        const auto check = [&]<typename T>(TType type, TScalar value, T storageValue) {
            TExpressionArena arena;
            TOptimizerStatistics stats;
            stats.Nrows = 10;
            stats.ColumnStatistics = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();
            auto& columnStats = stats.ColumnStatistics->Data["7"];
            columnStats.Type = type.ToString();
            columnStats.CountMinSketch.reset(TCountMinSketch::Create());
            for (size_t i = 0; i < 3; ++i) {
                if constexpr (std::is_same_v<T, std::string_view>)
                    columnStats.CountMinSketch->Count(storageValue.data(), storageValue.size());
                else
                    columnStats.CountMinSketch->Count(reinterpret_cast<const char*>(&storageValue), sizeof(storageValue));
            }
            const auto column = arena.Column(7, type.Optional()), literal = arena.Literal(type, value);
            const auto equal = arena.Binary(EOpcode::Equal, column, literal);
            UNIT_ASSERT_DOUBLES_EQUAL_C(ComputeNativeSelectivity(arena, equal, stats), 0.3, 1e-12, type.ToString());
            const auto notEqual = arena.Binary(EOpcode::NotEqual, literal, column);
            UNIT_ASSERT_DOUBLES_EQUAL_C(ComputeNativeSelectivity(arena, notEqual, stats), 0.8, 1e-12, type.ToString());
            // Stale or different source types must not reinterpret storage bytes.
            columnStats.Type = "unsupported";
            UNIT_ASSERT_DOUBLES_EQUAL(ComputeNativeSelectivity(arena, equal, stats), 0.1, 1e-12);
        };
        check(TType{K::Bool}, true, ui8{1});
        check(TType{K::Int8}, std::int64_t{-7}, i8{-7});
        check(TType{K::Int16}, std::int64_t{-300}, i16{-300});
        check(TType{K::Int32}, std::int64_t{-70000}, i32{-70000});
        check(TType{K::Int64}, std::int64_t{9007199254740993LL}, i64{9007199254740993LL});
        check(TType{K::Uint8}, std::uint64_t{7}, ui8{7});
        check(TType{K::Uint16}, std::uint64_t{300}, ui16{300});
        check(TType{K::Uint32}, std::uint64_t{70000}, ui32{70000});
        check(TType{K::Uint64}, std::uint64_t{18446744073709551615ULL}, ui64{18446744073709551615ULL});
        check(TType{K::Float}, 1.5, 1.5f);
        check(TType{K::Double}, 1.5, 1.5);
        check(TType{K::Decimal, false, 7, 2}, TDecimal{150}, 1.5);
        check(TType{K::Date}, std::uint64_t{100}, ui16{100});
        check(TType{K::Datetime}, std::uint64_t{70000}, ui32{70000});
        check(TType{K::Timestamp}, std::uint64_t{1234567890123456ULL}, ui64{1234567890123456ULL});
        check(TType{K::Interval}, std::int64_t{-1234567890123456LL}, i64{-1234567890123456LL});
        check(TType{K::String}, std::string("a\0b", 3), std::string_view("a\0b", 3));
        check(TType{K::Utf8}, std::string("héllo"), std::string_view("héllo"));
    }

    Y_UNIT_TEST(NativeSelectivityMergesTypedHistogramRanges) {
        using namespace NFrontend;
        using O = EOpcode;
        TExpressionArena arena;
        TOptimizerStatistics stats;
        stats.Nrows = 100;
        stats.ColumnStatistics = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>();
        auto& source = stats.ColumnStatistics->Data["7"];
        source.Type = "Int64";
        auto histogram = std::make_shared<TEqWidthHistogram>(100, EHistogramValueType::Int64);
        histogram->InitializeBuckets(i64{0}, i64{100});
        for (i64 i = 0; i < 100; ++i) histogram->AddElement(i);
        source.EqWidthHistogramEstimator = std::make_shared<TEqWidthHistogramEstimator>(histogram);
        const auto column = arena.Column(7, {ETypeKind::Int64});
        const auto literal = [&](i64 value) { return arena.Literal({ETypeKind::Int64}, std::int64_t{value}); };
        const auto cmp = [&](O op, i64 value) { return arena.Binary(op, column, literal(value)); };
        const auto range = [&](i64 low, i64 high) { return arena.Binary(O::And, cmp(O::GreaterEqual, low), cmp(O::Less, high)); };
        const auto check = [&](TExprId root, double expected) {
            UNIT_ASSERT_DOUBLES_EQUAL(ComputeNativeSelectivity(arena, root, stats), expected, 1e-12);
        };
        check(cmp(O::Less, 30), 0.3);
        check(cmp(O::LessEqual, 30), 0.31);
        check(cmp(O::GreaterEqual, 70), 0.3);
        check(arena.Binary(O::Greater, literal(30), column), 0.3);
        check(arena.Unary(O::Not, cmp(O::GreaterEqual, 70)), 0.7);
        check(range(20, 40), 0.2);
        check(arena.Binary(O::And, cmp(O::Greater, 20), cmp(O::Less, 40)), 0.19);
        check(range(40, 20), 0);
        check(range(20, 20), 0);
        check(arena.Binary(O::And, range(20, 40), range(30, 50)), 0.1);
        check(arena.Binary(O::Or, range(20, 40), range(30, 50)), 0.4);
        check(arena.Binary(O::Or, range(10, 20), range(30, 40)), 0.2);
        check(arena.Binary(O::Or, range(40, 20), range(70, 80)), 0.1);
        check(arena.Binary(O::Or, range(10, 20), range(20, 30)), 0.2);
        check(arena.Binary(O::Or, cmp(O::Less, 20), cmp(O::Greater, 20)), 1.0);
        // No lossy signed/unsigned narrowing to obtain a sketch/histogram key.
        check(arena.Binary(O::Equal, column, arena.Literal({ETypeKind::Uint64}, std::uint64_t{18446744073709551615ULL})), 0.1);
        source.Type = "Int32";
        check(cmp(O::Less, 30), 0.4);
    }

    Y_UNIT_TEST(NativeStatisticsRequestsMatchConsumedPredicates) {
        using namespace NFrontend;
        using O = EOpcode;
        using K = ETypeKind;
        TExpressionArena arena;
        const auto a = arena.Column(1, {K::Int64, true}), b = arena.Column(2, {K::Int64, true});
        const auto one = arena.Literal({K::Int64}, std::int64_t{1});
        const auto keyEquality = arena.Binary(O::Equal, a, b);
        const auto equality = arena.Binary(O::Equal, a, one), inequality = arena.Binary(O::Less, b, one);
        auto usage = CollectNativePredicateStatistics(arena, arena.Binary(O::And, equality, inequality));
        UNIT_ASSERT(usage.CountMinColumns == TUnorderedIUs{1});
        UNIT_ASSERT(usage.HistogramColumns == TUnorderedIUs{2});
        UNIT_ASSERT(usage.JoinKeys.empty());
        usage = CollectNativePredicateStatistics(arena, arena.Binary(O::NotEqual, a, one));
        UNIT_ASSERT(usage.CountMinColumns.Empty());
        UNIT_ASSERT(usage.HistogramColumns == TUnorderedIUs{1});

        // Intersecting two inequalities into a point needs the equality sketch
        // as well as the individual range estimates, using the same visitor.
        const auto point = arena.Binary(O::And, arena.Binary(O::GreaterEqual, a, one), arena.Binary(O::LessEqual, a, one));
        usage = CollectNativePredicateStatistics(arena, point);
        UNIT_ASSERT(usage.CountMinColumns == TUnorderedIUs{1});
        UNIT_ASSERT(usage.HistogramColumns == TUnorderedIUs{1});
        for (const auto predicate : {
            arena.Binary(O::Equal, a, arena.Parameter("p", {K::Int64})),
            arena.Binary(O::Less, arena.Column(3, {K::String}), arena.Literal({K::String}, std::string("x")))}) {
            usage = CollectNativePredicateStatistics(arena, predicate);
            UNIT_ASSERT(usage.CountMinColumns.Empty());
            UNIT_ASSERT(usage.HistogramColumns.Empty());
            UNIT_ASSERT(usage.JoinKeys.empty());
        }

        // The established estimator recognizes attributes through scalar casts.
        usage = CollectNativePredicateStatistics(arena, arena.Binary(O::Equal, arena.Cast(a, {K::Int32}), one));
        UNIT_ASSERT(usage.CountMinColumns == TUnorderedIUs{1});
        UNIT_ASSERT(usage.HistogramColumns.Empty());

        const TExprId nullAsFalse[]{keyEquality, arena.Literal({K::Bool}, false)};
        for (const auto predicate : {keyEquality, arena.Binary(O::And, equality, keyEquality), arena.Coalesce(nullAsFalse)}) {
            usage = CollectNativePredicateStatistics(arena, predicate);
            UNIT_ASSERT_VALUES_EQUAL(usage.JoinKeys.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(usage.JoinKeys.front().first, 1);
            UNIT_ASSERT_VALUES_EQUAL(usage.JoinKeys.front().second, 2);
        }
        const TExprId nullAsTrue[]{keyEquality, arena.Literal({K::Bool}, true)};
        for (const auto predicate : {arena.Binary(O::Or, equality, keyEquality), arena.Unary(O::Not, keyEquality), arena.Coalesce(nullAsTrue)})
            UNIT_ASSERT(CollectNativePredicateStatistics(arena, predicate).JoinKeys.empty());
    }

    Y_UNIT_TEST(NativeFilterAndPushedReadUseTheSameStatistics) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto id = f.Id("key");
        auto arena = std::make_shared<TExpressionArena>();
        const auto root = arena->Binary(EOpcode::Equal, arena->Column(id, {ETypeKind::Int64}),
            arena->Literal({ETypeKind::Int64}, std::int64_t{42}));
        const auto expression = TExpression::FromNative(arena, root, &f.Props);
        auto read = f.Read({id});
        read->Props.Metadata.emplace();
        read->Props.Metadata->ColumnsCount = 1;
        read->Props.Metadata->SourceStatsColumns.Add(id);
        read->Props.Metadata->KeyColumns.Append(id);
        const auto relation = f.Props.ColumnLineage.AddRelation("t", "/Root/table");
        f.Props.ColumnLineage.Add(id, {.SourceAlias = "t", .TableName = "/Root/table", .ColumnName = "key", .Relation = relation});
        auto metadata = MakeIntrusive<TKikimrTableMetadata>();
        metadata->DoesExist = true;
        metadata->RecordsCount = 100;
        metadata->DataSize = 800;
        f.Tables->GetOrAddTable("ut", "/Root", "/Root/table").Metadata = metadata;
        using TStatsMap = NYql::TOptimizerStatistics::TColumnStatMap;
        auto& global = f.TypeCtx.ColumnStatisticsByTableName["/Root/table"];
        global = MakeIntrusive<TStatsMap>();
        auto& source = global->Data["key"];
        source.Type = "Int64";
        source.CountMinSketch.reset(TCountMinSketch::Create());
        const i64 value = 42;
        for (int i = 0; i < 3; ++i) source.CountMinSketch->Count(reinterpret_cast<const char*>(&value), sizeof(value));
        read->ComputeStatistics(f.RboCtx, f.Props);
        auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, expression);
        filter->ComputeMetadata(f.RboCtx, f.Props);
        filter->ComputeStatistics(f.RboCtx, f.Props);
        UNIT_ASSERT_DOUBLES_EQUAL(filter->Props.Statistics->ERows, 3, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(filter->Props.Statistics->EBytes, 24, 1e-12);
        read->OriginalPredicate = expression;
        read->ComputeStatistics(f.RboCtx, f.Props);
        UNIT_ASSERT_DOUBLES_EQUAL(read->Props.Statistics->ERows, filter->Props.Statistics->ERows, 1e-12);
        UNIT_ASSERT_DOUBLES_EQUAL(read->Props.Statistics->Selectivity, filter->Props.Statistics->Selectivity, 1e-12);


    }

    Y_UNIT_TEST(TupleStatesLowerDirectlyAndRoundTripThroughBackendTypes) {
        using namespace NFrontend;
        TExpressionArena arena, imported;
        const TType fields[]{{ETypeKind::Decimal, false, 35, 2}, {ETypeKind::Uint64}, {ETypeKind::Float, true}};
        const auto state = arena.TupleType(fields);
        const TTuple value = std::make_shared<const TTupleValue>(TTupleValue{{
            {fields[0], TDecimal{12345}}, {fields[1], std::uint64_t{3}}, {fields[2], 1.1}}});
        const auto literal = arena.Literal(state, value);
        CheckScalar(arena, literal, "native tuple state literal");
        CheckScalar(arena, arena.Convert(literal, state.Optional()), "optional tuple state");
        CheckScalar(arena, arena.Literal(state.Optional(), std::monostate{}), "absent tuple state");
        TExprContext ctx;
        for (const auto type : {state, state.Optional()}) {
            const auto* backend = ExportNativeType(type, ctx, &arena);
            const auto native = ImportNativeType(*backend, &imported);
            UNIT_ASSERT(native == imported.ImportType(arena, type));
            UNIT_ASSERT(IsSameAnnotation(*backend, *ExportNativeType(native, ctx, &imported)));
        }
        const auto* nestedTuple = ctx.MakeType<TTupleExprType>(TVector<const TTypeAnnotationNode*>{ExportNativeType(state.Optional(), ctx, &arena)});
        const auto nativeNestedTuple = ImportNativeType(*nestedTuple, &imported);
        UNIT_ASSERT_VALUES_EQUAL(imported.TupleItems(nativeNestedTuple).size(), 1);
        UNIT_ASSERT(imported.TupleItems(nativeNestedTuple).front() == imported.ImportType(arena, state.Optional()));
        UNIT_ASSERT(IsSameAnnotation(*nestedTuple, *ExportNativeType(nativeNestedTuple, ctx, &imported)));
        const auto* nestedOptional = ctx.MakeType<TOptionalExprType>(ctx.MakeType<TOptionalExprType>(ctx.MakeType<TDataExprType>(EDataSlot::Uint64)));
        const auto nativeOptional = ImportNativeType(*nestedOptional, &imported);
        UNIT_ASSERT_VALUES_EQUAL(nativeOptional.OptionalDepth, 2);
        UNIT_ASSERT(IsSameAnnotation(*nestedOptional, *ExportNativeType(nativeOptional, ctx, &imported)));
    }

    Y_UNIT_TEST(UnicodeCaseLowersDirectlyToRuntime) {
        using namespace NFrontend;
        for (const auto& text : {std::string{}, std::string("AbC"), std::string("Привет École σςΣ ıIİi ſS ȿⱾ e\u0301"),
                std::string("\U00010428\U00010400😀"), std::string("a\0B", 3)}) {
            for (const bool nullable : {false, true}) {
                TExpressionArena arena;
                const TType type{ETypeKind::Utf8, nullable};
                const auto value = arena.Literal(type, text);
                for (const auto op : {EOpcode::UnicodeUpper, EOpcode::UnicodeLower}) {
                    CheckScalar(arena, arena.Unary(op, value), "Unicode case");
                    if (nullable) CheckScalar(arena, arena.Unary(op, arena.Literal(type, std::monostate{})), "Unicode NULL");
                }
            }
        }
        CheckScalar("SELECT Unicode::ToUpper('hello')");
        CheckScalar("SELECT Unicode::ToLower(NULL)");
    }

    Y_UNIT_TEST(GroupingSetsExpansionKeepsNativeExpressions) {
        for (const auto* clause : {"ROLLUP(k,v)", "CUBE(k,v)", "k, ROLLUP(v)", "GROUPING SETS ((k,v))", "GROUPING SETS ((k,v),())"}) {
            for (const auto* measures : {"SUM(k), SUM(v), AVG(k), COUNT(*)", "COUNT(DISTINCT k), SUM(v), SUM(DISTINCT k)", "1"}) {
                NTests::TIdTestContext f;
                const TString sql = TStringBuilder() << "SELECT k, v, GROUPING(k,v), " << measures
                    << " FROM (SELECT 1 AS k, CAST(2 AS Int32?) AS v) AS q GROUP BY " << clause;
                auto roots = CompileNativeToRbo(std::string_view(sql.data(), sql.size()), NoTables, f.ExprCtx).Roots;
                auto& root = *roots.front();
                for (const auto& item : root) UNIT_ASSERT(item.Current->Kind != EOperator::GroupingSets);
                auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                    UNIT_FAIL("native grouping sets invoked YQL annotation");
                    return IGraphTransformer::TStatus::Error;
                });
                TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
                TGlobalPruningStage("native grouping sets pruning").RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                const auto* expected = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                TVector<std::unique_ptr<IRule>> rules;
                rules.emplace_back(std::make_unique<TExpandGroupingSetsRule>());
                rules.emplace_back(std::make_unique<TExpandDistinctAggregationRule>());
                TRuleBasedStage("native grouping sets expansion", std::move(rules)).RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                TGlobalPruningStage("native grouping sets padding pruning").RunStage(root, context);
                TVector<std::unique_ptr<IRule>> copies;
                copies.emplace_back(std::make_unique<TExpandReplicateRule>());
                TRuleBasedStage("native grouping sets copies", std::move(copies)).RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                const auto* actual = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                for (const auto& [id, label] : root.GetColumns().Items()) {
                    const auto name = f.ExprCtx.GetIndexAsString(id);
                    UNIT_ASSERT_C(IsSameAnnotation(*expected->FindItemType(name), *actual->FindItemType(name)), sql);
                }
                NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
                for (const auto& item : root) {
                    UNIT_ASSERT(item.Current->Kind != EOperator::GroupingSets);
                    for (const auto& expression : item.Current->GetExpressions()) {
                        UNIT_ASSERT(expression.get().IsNative());

                    }
                }
            }
        }
    }

    Y_UNIT_TEST(SampleDeviationStatesStayTypedAcrossAggregationPhases) {
        for (const auto* kind : {"Int32", "Int64", "Uint64", "Float", "Double"}) {
            for (const bool nullable : {false, true}) {
                for (const auto* grouping : {"", " GROUP BY k", " GROUP BY ROLLUP(k)", " GROUP BY GROUPING SETS((k),(),())"}) {
                    NTests::TIdTestContext f;
                    const TString sql = TStringBuilder() << "SELECT " << (*grouping ? "k, " : "")
                        << "STDDEV_SAMPLE(v) AS s, AVG(v) AS a, AVG(Decimal('1.25',8,2)) AS d "
                        << "FROM (SELECT 1 AS k, CAST(" << kind << "('2') AS " << kind << (nullable ? "?" : "") << ") AS v) AS q" << grouping;
                    auto roots = CompileNativeToRbo(std::string_view(sql.data(), sql.size()), NoTables, f.ExprCtx).Roots;
                    auto& root = *roots.front();
                    auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                        UNIT_FAIL("native aggregate phases invoked YQL annotation");
                        return IGraphTransformer::TStatus::Error;
                    });
                    TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
                    TGlobalPruningStage("native aggregate pruning").RunStage(root, context);
                    UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                    const auto* expected = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    TVector<std::pair<IOperator*, size_t>> edges;
                    for (const auto& item : root) if (item.Current->Kind == EOperator::Aggregate)
                        edges.emplace_back(item.Parent ? item.Parent : &root, item.Parent ? item.ChildIndex : 0);
                    UNIT_ASSERT(!edges.empty());
                    for (const auto& [parent, index] : edges) {
                        UNIT_ASSERT(TPropagateAggregateThroughStageRule().MatchAndApply(parent->MutableChild(index), context, root.PlanProps));
                    }
                    root.RecomputeOutputIUsSubtree();
                    root.ComputeParents();
                    UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                    // Forward actual partial states through native scalar maps,
                    // so their tuple type must survive ID substitution and binding.
                    size_t partials = 0;
                    for (const auto& [parent, index] : edges) {
                        auto final = CastOperator<TOpAggregate>(parent->MutableChild(index));
                        UNIT_ASSERT(final->GetAggregationPhase() == EOpPhase::Final);
                        auto partial = CastOperator<TOpAggregate>(final->GetInput().Get());
                        UNIT_ASSERT(partial->GetAggregationPhase() == EOpPhase::Intermediate);
                        TSubstitutions copies;
                        TMapIUs fields;
                        for (const auto& [output, traits] : partial->GetAggregationTraits().Items()) {
                            if (traits.AggFunction != "variance_1_1") continue;
                            ++partials;
                            const auto* type = ExportIUType(*partial, output, f.ExprCtx);
                            UNIT_ASSERT_VALUES_EQUAL(type->IsOptionalOrNull(), nullable);
                            const auto& required = RemoveOptionality(*type);
                            UNIT_ASSERT(required.GetKind() == ETypeAnnotationKind::Tuple);
                            const auto& items = required.Cast<TTupleExprType>()->GetItems();
                            UNIT_ASSERT_VALUES_EQUAL(items.size(), 3);
                            for (const auto* item : items) UNIT_ASSERT(item->GetKind() == ETypeAnnotationKind::Data &&
                                item->Cast<TDataExprType>()->GetSlot() == EDataSlot::Double);
                            const auto copy = root.PlanProps.InfoUnitRegistry.AddCopy(output);
                            fields.Add(copy, MakeColumnAccess(output, *partial, partial->Pos, &root.PlanProps));
                            copies.Add(output, copy);
                        }
                        auto map = MakeIntrusive<TOpMap>(final->GetInput(), final->Pos, std::move(fields));
                        final->SetChild(0, std::move(map));
                        final->RenameUsedIUs(copies);
                    }
                    UNIT_ASSERT_VALUES_EQUAL(partials, edges.size());
                    root.RecomputeOutputIUsSubtree();
                    root.ComputeParents();
                    UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                    TGlobalPruningStage("native state forwarding pruning").RunStage(root, context);
                    TVector<std::unique_ptr<IRule>> copies;
                    copies.emplace_back(std::make_unique<TExpandReplicateRule>());
                    TRuleBasedStage("native state forwarding copies", std::move(copies)).RunStage(root, context);
                    UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                    const auto* actual = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                    for (const auto& [id, label] : root.GetColumns().Items()) {
                        const auto name = f.ExprCtx.GetIndexAsString(id);
                        UNIT_ASSERT_C(IsSameAnnotation(*expected->FindItemType(name), *actual->FindItemType(name)), sql);
                    }
                    NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
                    for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions()) {
                        UNIT_ASSERT(expression.get().IsNative());

                    }
                }
            }
        }
    }

    Y_UNIT_TEST(MixedDistinctExpansionKeepsNativeExpressions) {
        for (const auto* projection : {
                "COUNT(DISTINCT v), COUNT(*), SUM(v), SUM(DISTINCT v), MIN(v), MAX(DISTINCT v)",
                "COUNT(DISTINCT v), SUM(d), SUM(DISTINCT d), MIN(d), MAX(DISTINCT d)",
                "COUNT(DISTINCT v), SOME(CAST(NULL AS Int32)), SOME(7)",
                "COUNT(DISTINCT v), AVG(v), AVG(DISTINCT v), AVG(k)",
                "COUNT(DISTINCT v), AVG(d), AVG(DISTINCT d)",
                "AVG(DISTINCT v), AVG(DISTINCT d), AVG(DISTINCT k)",
                "COUNT(DISTINCT v), SUM(Interval('PT1S')), SUM(CAST(NULL AS Interval))"}) {
            for (const bool grouped : {false, true}) {
                NTests::TIdTestContext f;
                const TString sql = TStringBuilder() << "SELECT " << projection
                    << " FROM (SELECT 1 AS k, CAST(2 AS Int32?) AS v, Decimal('1.25',8,2) AS d) AS q"
                    << (grouped ? " GROUP BY k" : "");
                auto roots = CompileNativeToRbo(std::string_view(sql.data(), sql.size()), NoTables, f.ExprCtx).Roots;
                auto& root = *roots.front();
                auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                    UNIT_FAIL("native DISTINCT expansion invoked YQL annotation");
                    return IGraphTransformer::TStatus::Error;
                });
                TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
                TGlobalPruningStage("native distinct pruning").RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                TUnorderedIUs originalResults;
                for (const auto& item : root) if (item.Current->Kind == EOperator::Aggregate)
                    originalResults.UnionWith(CastOperator<TOpAggregate>(*item.Current).GetAggregationTraits().Keys());
                const auto* expected = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                TVector<std::unique_ptr<IRule>> rules;
                rules.emplace_back(std::make_unique<TExpandDistinctAggregationRule>());
                TRuleBasedStage("native distinct expansion", std::move(rules)).RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                bool finalFound = false;
                for (const auto& item : root) if (item.Current->Kind == EOperator::Aggregate) {
                    const auto& aggregate = CastOperator<TOpAggregate>(*item.Current);
                    if (aggregate.GetInput()->Kind != EOperator::UnionAll) continue;
                    // Match the original rule boundary: the final aggregate
                    // defines the public result IDs without an extra map.
                    UNIT_ASSERT(aggregate.GetAggregationPhase() == EOpPhase::Final);
                    UNIT_ASSERT(aggregate.GetAggregationTraits().Keys() == originalResults);
                    finalFound = true;
                }
                UNIT_ASSERT(finalFound);
                TGlobalPruningStage("native distinct padding pruning").RunStage(root, context);
                TVector<std::unique_ptr<IRule>> copies;
                copies.emplace_back(std::make_unique<TExpandReplicateRule>());
                TRuleBasedStage("native distinct copies", std::move(copies)).RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                const auto* actual = ExportOperatorType(*root.GetInput(), f.ExprCtx)->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
                for (const auto& [id, label] : root.GetColumns().Items()) {
                    const auto name = f.ExprCtx.GetIndexAsString(id);
                    UNIT_ASSERT(IsSameAnnotation(*expected->FindItemType(name), *actual->FindItemType(name)));
                }
                NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
                for (const auto& item : root) {
                    if (item.Current->Kind == EOperator::Aggregate) {
                        const auto& aggregate = CastOperator<TOpAggregate>(*item.Current);
                        for (const auto& [output, traits] : aggregate.GetAggregationTraits().Items()) {
                            UNIT_ASSERT(!traits.Distinct);
                            if (traits.AggFunction != "avg") continue;
                            UNIT_ASSERT(traits.ResultType);
                            const auto* type = ExportIUType(aggregate, output, f.ExprCtx);
                            const auto& required = RemoveOptionality(*type);
                            if (aggregate.GetAggregationPhase() == EOpPhase::Intermediate) {
                                UNIT_ASSERT(required.GetKind() == ETypeAnnotationKind::Tuple);
                                const auto& fields = required.Cast<TTupleExprType>()->GetItems();
                                UNIT_ASSERT_VALUES_EQUAL(fields.size(), 2);
                                UNIT_ASSERT(fields[1]->Cast<TDataExprType>()->GetSlot() == EDataSlot::Uint64);
                                const auto* input = ExportIUType(*aggregate.GetInput(), traits.Input, f.ExprCtx);
                                UNIT_ASSERT_VALUES_EQUAL(type->IsOptionalOrNull(), input->IsOptionalOrNull());
                                if (traits.ResultType->Kind == NFrontend::ETypeKind::Decimal) {
                                    const auto* decimal = fields[0]->Cast<TDataExprParamsType>();
                                    UNIT_ASSERT_VALUES_EQUAL(decimal->GetParamOne(), "35");
                                    UNIT_ASSERT_VALUES_EQUAL(decimal->GetParamTwo(), "2");
                                } else UNIT_ASSERT(fields[0]->Cast<TDataExprType>()->GetSlot() == EDataSlot::Double);
                            } else {
                                UNIT_ASSERT(required.GetKind() == ETypeAnnotationKind::Data);
                                UNIT_ASSERT(ImportNativeType(required) == traits.ResultType->Required());
                            }
                        }
                    }
                    for (const auto& expression : item.Current->GetExpressions()) {
                        UNIT_ASSERT(expression.get().IsNative());

                    }
                }
            }
        }
    }

    Y_UNIT_TEST(KeyRangesStayNativeThroughPushdownPruningAndCopy) {
        using namespace NFrontend;
        using K = ETypeKind;
        using O = EOpcode;
        for (const auto storage : {NYql::EStorageType::RowStorage, NYql::EStorageType::ColumnStorage}) {
            NTests::TIdTestContext f;
            const auto key = f.Id("key"), suffix = f.Id("suffix"), payload = f.Id("payload"), unused = f.Id("unused");
            auto metadata = MakeIntrusive<TKikimrTableMetadata>();
            metadata->DoesExist = true;
            metadata->Name = "/Root/table";
            metadata->Kind = storage == NYql::EStorageType::RowStorage ? EKikimrTableKind::Datashard : EKikimrTableKind::Olap;
            metadata->KeyColumnNames = {"key", "suffix"};
            f.Tables->GetOrAddTable("ut", "/Root", metadata->Name).Metadata = metadata;
            auto arena = std::make_shared<TExpressionArena>();
            const auto a = arena->Column(key, {K::Int32}), b = arena->Column(suffix, {K::Int32, true});
            const auto equal = arena->Binary(O::Equal, a, arena->Literal({K::Uint64}, std::uint64_t{2}));
            const auto upper = arena->Binary(O::Greater, arena->Literal({K::Int64}, std::int64_t{5}), b);
            const auto residual = arena->Binary(O::Greater, arena->Column(payload, {K::Int32}), arena->Literal({K::Int32}, std::int64_t{0}));
            const auto rootId = arena->Binary(O::And, arena->Binary(O::And, equal, upper), residual);
            auto read = f.Read({key, suffix, payload, unused});
            read->StorageType = storage;
            SetNativeReadType(f, *read, {suffix});
            const auto expression = TExpression::FromNative(arena, rootId, &f.Props);
            auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, expression);
            auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
                UNIT_FAIL("native key ranges invoked YQL annotation");
                return IGraphTransformer::TStatus::Error;
            });
            TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
            TPushRangesRule rule(storage);
            const auto pushed = rule.SimpleMatchAndApply(filter, context, f.Props);
            UNIT_ASSERT(pushed->Kind == EOperator::Filter);
            const auto& remaining = CastOperator<TOpFilter>(*pushed);
            UNIT_ASSERT_VALUES_EQUAL(remaining.GetFilterExpression().GetNativeRoot(), residual);
            auto& source = CastOperator<TOpRead>(*remaining.GetInput());
            UNIT_ASSERT(source.RangeInfo && source.RangeInfo->NativeRanges);

            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->UsedPrefixLen, 2);
            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->PointPrefixLen, 1);
            UNIT_ASSERT_VALUES_EQUAL(*source.RangeInfo->ExpectedMaxRanges, 1);
            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->NativeRanges->size(), 1);
            const auto& bounds = source.RangeInfo->NativeRanges->front();
            UNIT_ASSERT_VALUES_EQUAL(bounds.Lower.Values.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(bounds.Upper.Values.size(), 2);
            UNIT_ASSERT(!bounds.Lower.Inclusive && !bounds.Upper.Inclusive);
            for (const auto* bound : {&bounds.Lower, &bounds.Upper}) {
                UNIT_ASSERT(bound->Values[0].GetNativeType() == TType{K::Int32});
                UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(*arena, bound->Values[0].GetNativeRoot()).Data), 2);
            }
            UNIT_ASSERT(Evaluate(*arena, bounds.Lower.Values[1].GetNativeRoot()).IsNull());
            UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(*arena, bounds.Upper.Values[1].GetNativeRoot()).Data), 5);
            UNIT_ASSERT_VALUES_EQUAL(source.OriginalPredicate->GetNativeRoot(), rootId);
            UNIT_ASSERT(rule.SimpleMatchAndApply(pushed, context, f.Props) == pushed);
            for (const auto& term : source.GetExpressions()) {
                UNIT_ASSERT(term.get().IsNative());
                UNIT_ASSERT(term.get().GetRawInputIUs().Empty());
            }
            TSubstitutions renames;
            const auto copied = source.Copy(f.Props, renames);
            UNIT_ASSERT(copied);
            const auto& copy = CastOperator<TOpRead>(*copied);
            UNIT_ASSERT(copy.RangeInfo->NativeRanges);
            UNIT_ASSERT_VALUES_EQUAL(copy.RangeInfo->NativeRanges->front().Upper.Values[1].GetNativeRoot(), bounds.Upper.Values[1].GetNativeRoot());
            UNIT_ASSERT(copy.OriginalPredicate->GetRawInputIUs().Contains(*renames.Find(key)));
            UNIT_ASSERT(!copy.OriginalPredicate->GetRawInputIUs().Contains(key));
            auto root = f.Root(pushed, {{payload, "payload"}});
            TGlobalPruningStage("prune").RunStage(*root, context);
            UNIT_ASSERT(source.GetColumns() == (TUnorderedIUs{key, suffix, payload}));
            NTests::AssertIdInvariants(*root, root->PlanProps);
            const auto explain = source.ToJson(0, root->PlanProps.InfoUnitRegistry);
            UNIT_ASSERT_VALUES_EQUAL(explain["ReadRangesExpectedSize"].GetStringSafe(), "1");
            UNIT_ASSERT(!explain["ReadRangeLower"]["Inclusive"].GetBooleanSafe());
        }
    }

    Y_UNIT_TEST(NativeIndexRangesKeepPredicatesAndBoundsNative) {
        using namespace NFrontend;
        using K = ETypeKind;
        using O = EOpcode;
        for (const bool covering : {false, true}) for (const bool reverse : {false, true})
        for (const bool disabled : {false, true}) for (const bool payloadPredicate : {false, true}) {
            NTests::TIdTestContext f;
            f.Config->OptDisableAutoIndexSelection = disabled;
            f.Config->EnableLookupJoinPointPrefixes = true;
            const auto pk = f.Id("id"), group = f.Id("g"), extra = f.Id("x"), payload = f.Id("payload");
            auto metadata = MakeIntrusive<TKikimrTableMetadata>();
            metadata->Name = "/Root/table";
            metadata->DoesExist = true;
            metadata->Kind = EKikimrTableKind::Datashard;
            metadata->KeyColumnNames = {"id"};
            for (const auto* name : {"id", "g", "x", "payload"}) {
                auto& column = metadata->Columns[name]; column.Type = "Int32"; column.NotNull = TStringBuf(name) != "g";
                column.TypeInfo = NScheme::TTypeInfo(NScheme::NTypeIds::Int32);
            }
            f.Tables->GetOrAddTable("ut", "/Root", metadata->Name).Metadata = metadata;
            for (const char* name : reverse ? TVector<const char*>{"B", "A", "0_async", "0_unready", "0_json", "0_json_compact"}
                                           : TVector<const char*>{"0_json_compact", "0_json", "0_unready", "0_async", "A", "B"}) {
                auto index = MakeIntrusive<TKikimrTableMetadata>();
                index->Name = TStringBuilder() << metadata->Name << '/' << name << "/indexImplTable";
                index->DoesExist = true;
                index->Kind = EKikimrTableKind::Datashard;
                index->KeyColumnNames = {"g", "id"};
                for (const char* column : {"g", "id", "x"}) index->Columns.emplace(column, metadata->Columns.at(column));
                if (covering) index->Columns.emplace("payload", metadata->Columns.at("payload"));
                const auto indexType = TStringBuf(name) == "0_async" ? TIndexDescription::EType::GlobalAsync
                    : TStringBuf(name) == "0_json" ? TIndexDescription::EType::GlobalJson
                    : TStringBuf(name) == "0_json_compact" ? TIndexDescription::EType::GlobalJsonCompact
                    : TIndexDescription::EType::GlobalSync;
                metadata->Indexes.emplace_back(name, TVector<TString>{"g"}, TVector<TString>{},
                    indexType,
                    TStringBuf(name) == "0_unready" ? TIndexDescription::EIndexState::NotReady : TIndexDescription::EIndexState::Ready,
                    1, 1, 1, std::monostate{});
                metadata->ImplTables.push_back(index);
                f.Tables->GetOrAddTable("ut", "/Root", index->Name).Metadata = index;
            }
            auto arena = std::make_shared<TExpressionArena>();
            const auto equal = arena->Binary(O::Equal, arena->Column(group, {K::Int32, true}),
                arena->Literal({K::Int32}, std::int64_t{2}));
            const auto residual = arena->Binary(O::Greater, arena->Column(payloadPredicate ? payload : extra, {K::Int32}),
                arena->Literal({K::Int32}, std::int64_t{0}));
            const auto predicate = arena->Binary(O::And, equal, residual);
            auto read = f.Read({pk, group, extra, payload});
            SetNativeReadType(f, *read, {group});
            auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
                UNIT_FAIL("native index selection invoked YQL annotation");
                return IGraphTransformer::TStatus::Error;
            });
            TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
            const auto pushed = TPushRangesRule(NYql::EStorageType::RowStorage).SimpleMatchAndApply(filter, context, f.Props);
            if (disabled || (payloadPredicate && !covering)) {
                UNIT_ASSERT(pushed == filter);
                continue;
            }
            UNIT_ASSERT(pushed->Kind == (covering ? EOperator::Filter : EOperator::TableLookup));
            const auto indexFilter = covering ? pushed : CastOperator<TOpTableLookup>(*pushed).GetInput();
            UNIT_ASSERT(indexFilter->Kind == EOperator::Filter);
            const auto& remaining = CastOperator<TOpFilter>(*indexFilter).GetFilterExpression();
            auto& source = CastOperator<TOpRead>(*indexFilter->GetChildren().front());
            UNIT_ASSERT_VALUES_EQUAL(source.TableRef.Path, "/Root/table/A/indexImplTable");
            UNIT_ASSERT(source.RangeInfo && source.RangeInfo->NativeRanges);
            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->MainTable, "/Root/table");
            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->PointPrefixLen, 1);
            UNIT_ASSERT_VALUES_EQUAL(source.RangeInfo->UsedPrefixLen, 1);
            UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(Evaluate(*arena,
                source.RangeInfo->NativeRanges->front().Lower.Values.front().GetNativeRoot()).Data), 2);
            UNIT_ASSERT(remaining.IsNative());
            UNIT_ASSERT(source.OriginalPredicate && source.OriginalPredicate->IsNative());
            UNIT_ASSERT(remaining.GetRawInputIUs().IsSubsetOf(source.GetColumns()));
            UNIT_ASSERT(source.OriginalPredicate->GetRawInputIUs().IsSubsetOf(source.GetColumns()));
            if (!covering) {
                auto& lookup = CastOperator<TOpTableLookup>(*pushed);
                UNIT_ASSERT(lookup.GetColumns() == read->GetColumns());
                UNIT_ASSERT(!lookup.GetColumns().HasAny(source.GetColumns()));
                UNIT_ASSERT(lookup.SourceRead && lookup.SourceRead->Predicate.IsNative());
                UNIT_ASSERT_VALUES_EQUAL(lookup.SourceRead->Predicate.GetNativeRoot(), predicate);
                UNIT_ASSERT_VALUES_EQUAL(lookup.LookupKeys.Items().size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(lookup.LookupKeys.Items().front().second, "id");
                UNIT_ASSERT(source.GetColumns().Contains(lookup.LookupKeys.Items().front().first));
            }
            auto root = f.Root(pushed, {{payload, "payload"}});
            NTests::AssertIdInvariants(*root, root->PlanProps);
            for (const auto& item : *root) for (const auto& expression : item.Current->GetExpressions())
                UNIT_ASSERT(expression.get().IsNative());
            if (covering) {
                TSubstitutions renames;
                const auto copy = source.Copy(root->PlanProps, renames);
                UNIT_ASSERT(copy && CastOperator<TOpRead>(*copy).RangeInfo->NativeRanges);
                UNIT_ASSERT(!CastOperator<TOpRead>(*copy).OriginalPredicate->GetRawInputIUs().Contains(group));
            } else {
                auto& lookup = CastOperator<TOpTableLookup>(*pushed);
                const auto restored = MatchLookupJoinRightSide(pushed);
                UNIT_ASSERT(restored && !restored->Filter && restored->Read->RangeInfo);
                UNIT_ASSERT(!restored->Read->RangeInfo->NativeRanges);
                // The enabled policy saves prefixes for both eligible indexes,
                // independently of which index wins range selection.
                TVector<TString> prefixTables;
                for (const auto& prefix : restored->Read->RangeInfo->PointPrefixes) {
                    UNIT_ASSERT(prefix.NativePoints);
                    prefixTables.push_back(prefix.Table);
                }
                std::sort(prefixTables.begin(), prefixTables.end());
                UNIT_ASSERT_VALUES_EQUAL(prefixTables.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(prefixTables[0], "/Root/table/A/indexImplTable");
                UNIT_ASSERT_VALUES_EQUAL(prefixTables[1], "/Root/table/B/indexImplTable");
                UNIT_ASSERT(restored->Read->GetColumns() == lookup.GetColumns());
                UNIT_ASSERT(restored->Read->OriginalPredicate->IsNative());
                UNIT_ASSERT_VALUES_EQUAL(restored->Read->OriginalPredicate->GetNativeRoot(), predicate);
                UNIT_ASSERT_VALUES_EQUAL(restored->Read->TableRef.Path, "/Root/table");
                TSubstitutions renames;
                const auto renamed = root->PlanProps.InfoUnitRegistry.Add(TInfoUnit("renamed"));
                renames.Add(group, renamed);
                lookup.RewriteExpressions([&](const TExpression& expression) { return expression.ApplyRenames(renames); });
                UNIT_ASSERT(lookup.SourceRead->Predicate.GetRawInputIUs().Contains(renamed));
                UNIT_ASSERT(!lookup.SourceRead->Predicate.GetRawInputIUs().Contains(group));
                // Saved predicate is not a lookup-input dependency; the point
                // values are exposed to rewriting and have no row dependencies.
                UNIT_ASSERT(!lookup.GetExpressions().empty());
                for (const auto& value : lookup.GetExpressions()) {
                    UNIT_ASSERT(value.get().IsNative());
                    UNIT_ASSERT(value.get().GetRawInputIUs().Empty());
                }
            }
        }
    }

    Y_UNIT_TEST(NativeRangeBranchesPreserveResidualsAndBoundExpansion) {
        using namespace NFrontend;
        using K = ETypeKind;
        using O = EOpcode;
        for (const auto storage : {NYql::EStorageType::RowStorage, NYql::EStorageType::ColumnStorage}) {
            NTests::TIdTestContext f;
            const auto key = f.Id("key"), suffix = f.Id("suffix"), payload = f.Id("payload");
            auto metadata = MakeIntrusive<TKikimrTableMetadata>();
            metadata->DoesExist = true;
            metadata->Name = "/Root/table";
            metadata->Kind = storage == NYql::EStorageType::RowStorage ? EKikimrTableKind::Datashard : EKikimrTableKind::Olap;
            metadata->KeyColumnNames = {"key", "suffix"};
            f.Tables->GetOrAddTable("ut", "/Root", metadata->Name).Metadata = metadata;
            auto arena = std::make_shared<TExpressionArena>();
            const auto a = arena->Column(key, {K::Int32, true}), b = arena->Column(suffix, {K::Int32, true});
            const auto v = arena->Column(payload, {K::Int32});
            const auto compare = [&](O op, TExprId column, i64 value) {
                return arena->Binary(op, column, arena->Literal({K::Int32}, std::int64_t{value}));
            };
            const auto binary = [&](O op, TExprId left, TExprId right) { return arena->Binary(op, left, right); };
            const auto first = compare(O::Equal, a, 1), second = compare(O::Equal, a, 2);
            const auto suffixes = binary(O::Or, compare(O::Equal, b, 0), compare(O::Equal, b, 5));
            const auto keys = binary(O::Or, first, second);
            const auto nullPoint = arena->Unary(O::IsNull, a);
            const auto residual = binary(O::Or, binary(O::And, first, compare(O::Equal, v, 0)),
                binary(O::And, second, compare(O::Equal, v, 1)));
            auto large = suffixes;
            for (size_t i = 0; i < 20; ++i) large = binary(O::And, large, suffixes);
            struct TCase {
                TExprId Predicate;
                size_t Ranges;
                bool Residual;
                ui64 Limit = 10000;
                size_t Points = 0;
                ui64 PointLimit = 1000;
            };
            for (const auto test : TVector<TCase>{
                    {binary(O::Or, compare(O::Less, a, 0), compare(O::GreaterEqual, a, 5)), 2, false},
                    {binary(O::Or, keys, first), 3, false, 10000, 2, 2},
                    {binary(O::And, first, suffixes), 2, false},
                    {binary(O::And, keys, suffixes), 4, false},
                    {residual, 2, true, 10000, 2},
                    {binary(O::Or, first, compare(O::Equal, v, 1)), 0, true},
                    {nullPoint, 1, false, 10000, 1},
                    {binary(O::Or, first, nullPoint), 2, false, 10000, 2},
                    {binary(O::Or, binary(O::Or, first, nullPoint), nullPoint), 3, false, 10000, 2, 2},
                    {arena->Unary(O::IsNull, v), 0, true},
                    {binary(O::Equal, a, arena->Literal({K::Int32, true}, {})), 0, true},
                    {binary(O::And, first, suffixes), 1, true, 1, 1},
                    {binary(O::And, first, large), 1, true, 0, 1},
                    {keys, 2, false, 10000, 0, 1},
                    {first, 1, false, 10000, 0, 0},
                    {binary(O::Or, first, arena->Ensure(second, arena->Literal({K::Bool}, false), "checked")), 0, true}}) {
                f.Config->SetExtractPredicateRangesLimit(test.Limit);
                f.Config->SetIdxLookupJoinPointsLimit(test.PointLimit);
                auto read = f.Read({key, suffix, payload});
                read->StorageType = storage;
                SetNativeReadType(f, *read, {key, suffix});
                auto filter = MakeIntrusive<TOpFilter>(read, f.Pos,
                    TExpression::FromNative(arena, test.Predicate, &f.Props));
                auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
                    UNIT_FAIL("native interval unions invoked YQL annotation");
                    return IGraphTransformer::TStatus::Error;
                });
                TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
                TPushRangesRule rule(storage);
                auto result = rule.SimpleMatchAndApply(filter, context, f.Props);
                UNIT_ASSERT_VALUES_EQUAL(result->Kind == EOperator::Filter, test.Residual);
                if (!test.Ranges) { UNIT_ASSERT(result == filter); continue; }
                auto source = test.Residual ? CastOperator<TOpFilter>(*result).GetInput() : result;
                auto& extracted = CastOperator<TOpRead>(*source);
                UNIT_ASSERT(extracted.RangeInfo && extracted.RangeInfo->NativeRanges);
                UNIT_ASSERT_VALUES_EQUAL(extracted.RangeInfo->NativeRanges->size(), test.Ranges);
                const auto pointCount = storage == NYql::EStorageType::RowStorage ? test.Points : 0;
                UNIT_ASSERT_VALUES_EQUAL(extracted.RangeInfo->PointPrefixes.size(), pointCount ? 1 : 0);
                if (pointCount) {
                    const auto& prefix = extracted.RangeInfo->PointPrefixes.front();
                    UNIT_ASSERT(prefix.HasPoints() && prefix.NativePoints && prefix.NativePoints.has_value());
                    UNIT_ASSERT_VALUES_EQUAL(prefix.Table, metadata->Name);
                    UNIT_ASSERT(prefix.Columns == TVector<TString>{"key"});
                    UNIT_ASSERT(prefix.NativePoints->Types == (TVector<TType>{{K::Int32, true}}));
                    UNIT_ASSERT_VALUES_EQUAL(prefix.NativePoints->Rows.size(), pointCount);
                    UNIT_ASSERT_VALUES_EQUAL(*prefix.ExpectedMaxPoints, pointCount);
                }

                UNIT_ASSERT_VALUES_EQUAL(extracted.OriginalPredicate->GetNativeRoot(), test.Predicate);
                if (test.Predicate == residual)
                    UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpFilter>(*result).GetFilterExpression().GetNativeRoot(), residual);
                for (const auto& expression : extracted.GetExpressions()) {
                    UNIT_ASSERT(expression.get().IsNative());
                    UNIT_ASSERT(expression.get().GetRawInputIUs().Empty());
                }
                TSubstitutions renames;
                const auto copied = extracted.Copy(f.Props, renames);
                UNIT_ASSERT_VALUES_EQUAL(CastOperator<TOpRead>(*copied).RangeInfo->NativeRanges->size(), test.Ranges);
                const auto& copiedPrefixes = CastOperator<TOpRead>(*copied).RangeInfo->PointPrefixes;
                UNIT_ASSERT_VALUES_EQUAL(copiedPrefixes.size(), pointCount ? 1 : 0);
                if (pointCount) {
                    UNIT_ASSERT_VALUES_EQUAL(copiedPrefixes.front().NativePoints->Rows.size(), pointCount);
                    // The same rewrite contract must reach both range bounds
                    // and lookup points, including a copied source read.
                    copied->RewriteExpressions([&](const TExpression& expression) {
                        const auto& node = (*arena)[expression.GetNativeRoot()];
                        if (node.Op == O::Literal && node.Type == TType{K::Int32} && std::get<std::int64_t>(node.Value) == 1)
                            return TExpression::FromNative(arena, arena->Literal(node.Type, std::int64_t{3}), &f.Props);
                        return expression;
                    });
                    for (size_t i = 0; i < pointCount; ++i) {
                        const auto original = Evaluate(*arena,
                            extracted.RangeInfo->PointPrefixes.front().NativePoints->Rows[i].Values.front().GetNativeRoot());
                        const auto rewritten = Evaluate(*arena,
                            copiedPrefixes.front().NativePoints->Rows[i].Values.front().GetNativeRoot());
                        if (original.IsNull()) UNIT_ASSERT(rewritten.IsNull());
                        else {
                            const auto value = std::get<std::int64_t>(original.Data);
                            UNIT_ASSERT_VALUES_EQUAL(std::get<std::int64_t>(rewritten.Data), value == 1 ? 3 : value);
                        }
                    }
                }
                UNIT_ASSERT(CastOperator<TOpRead>(*copied).OriginalPredicate->GetRawInputIUs().Contains(*renames.Find(key)));
                UNIT_ASSERT_VALUES_EQUAL(extracted.ToJson(0, f.Props.InfoUnitRegistry)["ReadRanges"].GetArraySafe().size(), test.Ranges);
            }
        }
    }

    Y_UNIT_TEST(NativeLookupPointsExecuteWithoutSemanticLowering) {
        using namespace NFrontend;
        using K = ETypeKind;
        constexpr ui64 big = 9007199254740993ULL;
        const std::string binary("a\0b", 3);
        // Equal parameters must collapse; adjacent integers and distinct
        // composite suffixes must survive. Single-point lowering needs no dict.
        for (const bool nullable : {false, true}) for (const bool composite : {false, true})
        for (const bool multiple : {false, true}) for (const bool equalParameters : {false, true}) {
            NTests::TIdTestContext f;
            auto arena = std::make_shared<TExpressionArena>();
            TOpRead::TPointPrefix prefix;
            prefix.Columns = {"key"};
            if (composite) prefix.Columns.push_back("suffix");
            auto& points = prefix.NativePoints.emplace();
            points.Types = {{K::Uint64, nullable}};
            if (composite) points.Types.push_back({K::String, nullable});
            const auto add = [&](TExprId key, std::string suffix) {
                TVector<TExpression> row{TExpression::FromNative(arena, key, &f.Props)};
                if (composite) row.push_back(TExpression::FromNative(arena,
                    arena->Literal({K::String}, suffix), &f.Props));
                points.Rows.push_back({std::move(row), {}});
            };
            add(arena->Parameter("p", {K::Uint64}), binary);
            if (multiple) {
                add(arena->Parameter("q", {K::Uint64}), binary);
                add(arena->Literal({K::Uint64}, std::uint64_t{big}), binary);
                if (composite) add(arena->Literal({K::Uint64}, std::uint64_t{big}), "other");
                if (nullable) {
                    add(arena->Literal({K::Uint64, true}, std::monostate{}), binary);
                    add(arena->Literal({K::Uint64, true}, std::monostate{}), binary);
                }
            }
            const auto lowered = NLookupJoinBuilder::BuildLookupPoints(prefix, f.ExprCtx, f.Pos);
            UNIT_ASSERT(lowered->IsCallable(multiple ? "DictPayloads" : "AsList"));
            UNIT_ASSERT(AreAllNodesTypeAnnotated(*lowered));
            UNIT_ASSERT(!NeedsRboCompatibilityLowering(lowered));
            NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
            auto& pb = *setup.PgmBuilder;
            NCommon::TMkqlCommonCallableCompiler compiler;
            NCommon::TMkqlBuildContext build(compiler, pb, f.ExprCtx);
            build.Parameters = pb.NewStruct({{"$p", pb.NewDataLiteral<ui64>(big)},
                {"$q", pb.NewDataLiteral<ui64>(equalParameters ? big : big + 1)}});
            auto graph = setup.BuildGraph(NCommon::MkqlBuildExpr(*lowered, build));
            const auto values = graph->GetValue();
            TVector<std::pair<std::optional<ui64>, TString>> actual, expected{{big, composite ? TString(binary) : TString{}}};
            if (multiple) {
                if (!equalParameters) expected.emplace_back(big + 1, composite ? TString(binary) : TString{});
                if (composite) expected.emplace_back(big, "other");
                if (nullable) expected.emplace_back(std::nullopt, composite ? TString(binary) : TString{});
            }
            auto iterator = values.GetListIterator();
            NUdf::TUnboxedValue row;
            while (iterator.Next(row)) {
                auto key = row.GetElement(0);
                TString suffix;
                if (composite) {
                    const auto value = row.GetElement(1);
                    suffix = TString(value.AsStringRef());
                }
                actual.emplace_back(key ? std::optional<ui64>{key.Get<ui64>()} : std::nullopt,
                    std::move(suffix));
            }
            std::sort(actual.begin(), actual.end());
            std::sort(expected.begin(), expected.end());
            UNIT_ASSERT_VALUES_EQUAL(actual.size(), expected.size());
            UNIT_ASSERT(actual == expected);
        }
    }

    Y_UNIT_TEST(NativeIntervalUnionsExecuteWithoutSemanticLowering) {
        using namespace NFrontend;
        using K = ETypeKind;
        // Duplicate, overlapping, reversed, exclusive equal and unbounded
        // endpoints. Composite suffixes must retain their infinity markers.
        for (const auto storage : {NYql::EStorageType::RowStorage, NYql::EStorageType::ColumnStorage}) {
            for (const bool nullable : {false, true}) for (const bool composite : {false, true}) {
                NTests::TIdTestContext f;
                const auto key = f.Id("key");
                auto read = f.Read({key});
                read->StorageType = storage;
                SetNativeReadType(f, *read, nullable ? TUnorderedIUs{key} : TUnorderedIUs{});
                TVector<const TItemExprType*> fields{
                    f.ExprCtx.MakeType<TItemExprType>("key", ExportNativeType({K::Int32, nullable}, f.ExprCtx))};
                if (composite) fields.push_back(f.ExprCtx.MakeType<TItemExprType>("suffix", ExportNativeType({K::Int32, nullable}, f.ExprCtx)));
                const auto* schema = f.ExprCtx.MakeType<TStructExprType>(fields);
                auto arena = std::make_shared<TExpressionArena>();
                TOpRead::TRangeInfo info;
                info.KeyColumns = {"key"};
                if (composite) info.KeyColumns.push_back("suffix");
                info.NativeRanges.emplace();
                const auto value = [&](i32 number) {
                    return TExpression::FromNative(arena, arena->Literal({K::Int32}, std::int64_t{number}), &f.Props);
                };
                for (const auto& [lo, hi, inclusive] : TVector<std::tuple<i32, i32, bool>>{
                        {3, 7, true}, {9, 10, true}, {1, 5, true}, {3, 7, true}, {8, 0, true}, {8, 8, false}}) {
                    auto& range = info.NativeRanges->emplace_back();
                    range.Lower.Values = {value(lo)};
                    range.Upper.Values = {value(hi)};
                    range.Lower.Inclusive = range.Upper.Inclusive = inclusive;
                }
                auto& below = info.NativeRanges->emplace_back();
                below.Upper.Values = {value(-3)};
                below.Upper.Inclusive = false;
                info.NativeRanges->emplace_back().Lower.Values = {value(20)};
                read->RangeInfo = std::move(info);
                const TPhysicalNames names(f.Props.InfoUnitRegistry);
                const auto physical = TPhysicalSourceBuilder(*read, f.ExprCtx, f.Pos, names,
                    f.Props.InfoUnitRegistry, "stage", false, {}, schema).BuildPhysicalOp();
                const auto ranges = FindNode(physical, [](const NYql::TExprNode::TPtr& node) { return node->IsCallable("RangeFinalize"); });
                UNIT_ASSERT(ranges && ranges->Head().IsCallable("RangeUnion"));
                UNIT_ASSERT(AreAllNodesTypeAnnotated(*ranges));
                UNIT_ASSERT(!NeedsRboCompatibilityLowering(ranges));
                NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
                NCommon::TMkqlCommonCallableCompiler compiler;
                NCommon::TMkqlBuildContext build(compiler, *setup.PgmBuilder, f.ExprCtx);
                auto graph = setup.BuildGraph(NCommon::MkqlBuildExpr(*ranges, build));
                const auto values = graph->GetValue();
                UNIT_ASSERT_VALUES_EQUAL(values.GetListLength(), 4);
                auto iterator = values.GetListIterator();
                NUdf::TUnboxedValue item;
                const auto marker = composite ? 2 : 1;
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT(!item.GetElement(0).GetElement(0));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).GetElement(0).Get<i32>(), -3);
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).GetElement(marker).Get<i32>(), 0);
                for (const auto& [lo, hi] : {std::pair{1, 7}, std::pair{9, 10}}) {
                    UNIT_ASSERT(iterator.Next(item));
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).GetElement(0).Get<i32>(), lo);
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).GetElement(0).Get<i32>(), hi);
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).GetElement(marker).Get<i32>(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).GetElement(marker).Get<i32>(), 1);
                }
                UNIT_ASSERT(iterator.Next(item));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).GetElement(0).Get<i32>(), 20);
                UNIT_ASSERT(!item.GetElement(1).GetElement(0));
                UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).GetElement(marker).Get<i32>(), 0);
                UNIT_ASSERT(!iterator.Next(item));
            }
        }
    }

    Y_UNIT_TEST(NativeColumnRangesAreTypedExecutableBackendValues) {
        using namespace NFrontend;
        using K = ETypeKind;
        const ui64 keyValue = 9007199254740993ULL;
        for (const bool nullable : {false, true}) {
            for (const unsigned scenario : {0u, 1u, 2u, 3u, 4u, 5u}) {
                if (scenario == 5 && !nullable) continue;
                NTests::TIdTestContext f;
                const auto payload = f.Id("payload");
                auto read = f.Read({payload});
                read->StorageType = NYql::EStorageType::ColumnStorage;
                // Key columns are absent from the output row. Their complete
                // storage schema must still describe the unbounded suffix.
                TVector<const TItemExprType*> fields;
                for (const auto& [name, type] : std::initializer_list<std::pair<const char*, TType>>{
                        {"key", {K::Uint64, nullable}}, {"suffix", {K::Int32, nullable}},
                        {"tail", {K::Utf8, nullable}}, {"payload", {K::Int32}}}) {
                    fields.push_back(f.ExprCtx.MakeType<TItemExprType>(name, ExportNativeType(type, f.ExprCtx)));
                }
                const auto* schema = f.ExprCtx.MakeType<TStructExprType>(fields);
                auto arena = std::make_shared<TExpressionArena>();
                const auto literal = [&](TType type, TScalar value) {
                    return TExpression::FromNative(arena, arena->Literal(type, std::move(value)), &f.Props);
                };
                const auto key = literal({K::Uint64}, std::uint64_t{keyValue});
                TOpRead::TRangeInfo info;
                info.KeyColumns = {"key", "suffix", "tail"};
                info.NativeRanges.emplace();
                auto& range = info.NativeRanges->emplace_back();
                range.Lower.Values = {key};
                range.Upper.Values = {key};
                if (scenario == 1) range.Upper.Values.clear();
                if (scenario == 2) { range.Lower.Values.clear(); range.Upper.Inclusive = false; }
                if (scenario == 3) range.Lower.Values = {literal({K::Uint64}, std::uint64_t{keyValue + 10})};
                if (scenario == 4) range.Lower.Inclusive = range.Upper.Inclusive = false;
                if (scenario == 5) {
                    range.Lower.Values.push_back(literal({K::Int32, true}, {}));
                    range.Upper.Values.push_back(literal({K::Int32}, std::int64_t{5}));
                    range.Lower.Inclusive = range.Upper.Inclusive = false;
                }
                read->RangeInfo = info;
                const TPhysicalNames names(f.Props.InfoUnitRegistry);
                const auto physical = TPhysicalSourceBuilder(*read, f.ExprCtx, f.Pos, names,
                    f.Props.InfoUnitRegistry, "stage", false, {}, schema).BuildPhysicalOp();
                const auto source = FindNode(physical, [](const NYql::TExprNode::TPtr& node) {
                    return NNodes::TKqpBlockReadOlapTableRanges::Match(node.Get());
                });
                UNIT_ASSERT(source);
                const auto ranges = NNodes::TKqpBlockReadOlapTableRanges(source).Ranges().Ptr();
                UNIT_ASSERT(ranges->IsCallable("RangeFinalize"));
                UNIT_ASSERT(AreAllNodesTypeAnnotated(*ranges));
                UNIT_ASSERT(!NeedsRboCompatibilityLowering(ranges));
                NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
                NCommon::TMkqlCommonCallableCompiler compiler;
                NCommon::TMkqlBuildContext build(compiler, *setup.PgmBuilder, f.ExprCtx);
                auto graph = setup.BuildGraph(NCommon::MkqlBuildExpr(*ranges, build));
                const auto result = graph->GetValue();
                const bool empty = scenario == 3 || scenario == 4;
                UNIT_ASSERT_VALUES_EQUAL(result.GetListLength(), empty ? 0 : 1);
                if (empty) continue;
                auto iterator = result.GetListIterator();
                NUdf::TUnboxedValue item;
                UNIT_ASSERT(iterator.Next(item));
                const auto from = item.GetElement(0), to = item.GetElement(1);
                UNIT_ASSERT_VALUES_EQUAL(bool(from.GetElement(0)), scenario != 2);
                UNIT_ASSERT_VALUES_EQUAL(bool(to.GetElement(0)), scenario != 1);
                if (scenario != 2) UNIT_ASSERT_VALUES_EQUAL(from.GetElement(0).Get<ui64>(), keyValue);
                if (scenario != 1) UNIT_ASSERT_VALUES_EQUAL(to.GetElement(0).Get<ui64>(), keyValue);
                UNIT_ASSERT_VALUES_EQUAL(from.GetElement(3).Get<i32>(), scenario == 5 ? 0 : 1);
                UNIT_ASSERT_VALUES_EQUAL(to.GetElement(3).Get<i32>(), scenario == 1 || scenario == 2 || scenario == 5 ? 0 : 1);
                if (scenario == 5) {
                    UNIT_ASSERT(from.GetElement(1));
                    UNIT_ASSERT(!from.GetElement(1).GetOptionalValue());
                    UNIT_ASSERT_VALUES_EQUAL(to.GetElement(1).Get<i32>(), 5);
                } else {
                    UNIT_ASSERT(!from.GetElement(1) && !to.GetElement(1));
                }
                UNIT_ASSERT(!from.GetElement(2) && !to.GetElement(2));
                UNIT_ASSERT(!iterator.Next(item));
                UNIT_ASSERT(read->RangeInfo->NativeRanges);
            }
        }
    }

    Y_UNIT_TEST(KeyRangesRequireExactBoundsAndPreserveCheckedExpressions) {
        using namespace NFrontend;
        using K = ETypeKind;
        using O = EOpcode;
        NTests::TIdTestContext f;
        const auto key = f.Id("key"), external = f.Id("external");
        auto metadata = MakeIntrusive<TKikimrTableMetadata>();
        metadata->DoesExist = true;
        metadata->Name = "/Root/table";
        metadata->Kind = EKikimrTableKind::Datashard;
        metadata->KeyColumnNames = {"key"};
        f.Tables->GetOrAddTable("ut", "/Root", metadata->Name).Metadata = metadata;
        auto arena = std::make_shared<TExpressionArena>();
        const auto value = arena->Column(key, {K::Uint64, true});
        auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("native key ranges invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        TPushRangesRule rule(NYql::EStorageType::RowStorage);
        const auto check = [&](TExprId predicate, bool pushed) {
            auto read = f.Read({key});
            auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            const auto result = rule.SimpleMatchAndApply(filter, context, f.Props);
            UNIT_ASSERT_VALUES_EQUAL(result != filter, pushed);
            return result;
        };
        for (const ui64 number : {0ULL, 9007199254740993ULL, 9223372036854775808ULL, 18446744073709551615ULL}) {
            const auto literal = arena->Literal({K::Uint64}, std::uint64_t{number});
            const auto result = check(arena->Binary(O::Equal, value, literal), true);
            const auto& bound = CastOperator<TOpRead>(*result).RangeInfo->NativeRanges->front().Lower.Values.front();
            UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(*arena, bound.GetNativeRoot()).Data), number);
        }
        const auto param = arena->Parameter("bound", {K::Uint64});
        check(arena->Binary(O::LessEqual, value, param), true);
        const auto guarded = check(arena->Binary(O::Equal, value, arena->Parameter("nullable", {K::Uint64, true})), true);
        const auto& guardedRange = CastOperator<TOpRead>(*guarded).RangeInfo->NativeRanges->front();
        UNIT_ASSERT(guardedRange.Condition);
        for (const bool present : {false, true}) {
            TEvaluationContext evaluation;
            evaluation.Parameter = [&](std::string_view name) {
                UNIT_ASSERT_VALUES_EQUAL(name, "nullable");
                return TValue{{K::Uint64, true}, present ? TScalar{std::uint64_t{42}} : TScalar{}};
            };
            UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(*arena, guardedRange.Condition->GetNativeRoot(), evaluation).Data), present);
            if (present) UNIT_ASSERT_VALUES_EQUAL(std::get<std::uint64_t>(Evaluate(*arena,
                guardedRange.Lower.Values.front().GetNativeRoot(), evaluation).Data), 42);
        }
        check(arena->Binary(O::Equal, value, arena->Parameter("signed", {K::Int64})), true);
        check(arena->Binary(O::Greater, value, arena->Literal({K::Int64}, std::int64_t{-1})), true);
        check(arena->Binary(O::Equal, value, arena->Null()), false);
        const auto equal = arena->Binary(O::Equal, value, param);
        const auto nullable = check(arena->Binary(O::Or, equal, arena->Unary(O::IsNull, value)), true);
        const auto& ranges = *CastOperator<TOpRead>(*nullable).RangeInfo->NativeRanges;
        UNIT_ASSERT_VALUES_EQUAL(ranges.size(), 2);
        size_t parameters = 0, nulls = 0;
        for (const auto& range : ranges) {
            UNIT_ASSERT(range.Lower.Inclusive && range.Upper.Inclusive);
            UNIT_ASSERT_VALUES_EQUAL(range.Lower.Values.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(range.Upper.Values.size(), 1);
            const auto id = range.Lower.Values.front().GetNativeRoot();
            UNIT_ASSERT_VALUES_EQUAL(range.Upper.Values.front().GetNativeRoot(), id);
            if (id == param) ++parameters;
            else { UNIT_ASSERT(Evaluate(*arena, id).IsNull()); ++nulls; }
        }
        UNIT_ASSERT_VALUES_EQUAL(parameters, 1);
        UNIT_ASSERT_VALUES_EQUAL(nulls, 1);
        check(arena->Unary(O::IsNull, arena->Cast(value, {K::Int32})), false);
        check(arena->Binary(O::Equal, arena->Cast(value, {K::Int32}), arena->Literal({K::Int32}, std::int64_t{1})), false);
        const auto checked = arena->Binary(O::Equal, arena->Ensure(value, arena->Literal({K::Bool}, false), "checked"), param);
        check(arena->Binary(O::And, equal, checked), false);
        check(arena->Binary(O::And, checked, equal), false);
        check(arena->Binary(O::And, equal, arena->Binary(O::Equal, arena->Column(external, {K::Uint64}), param)), false);
        auto shared = equal;
        for (size_t i = 0; i < 20000; ++i) shared = arena->Binary(O::And, shared, shared);
        check(shared, true);
    }

    Y_UNIT_TEST(OlapProjectionsStayNativeAndPreserveLiveJsonInputs) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (int scenario = 0; scenario < 12; ++scenario) {
            NTests::TIdTestContext f;
            f.Config->OptEnableOlapPushdown = true;
            f.Config->OptEnableOlapPushdownProjections = true;
            const auto key = f.Id("key"), doc = f.Id("doc"), unused = f.Id("unused"), result = f.Id("result"), second = f.Id("second");
            auto metadata = MakeIntrusive<TKikimrTableMetadata>();
            metadata->DoesExist = true; metadata->Name = "/Root/table"; metadata->Kind = EKikimrTableKind::Olap;
            for (const auto* name : {"key", "doc", "unused"}) {
                auto& column = metadata->Columns[name]; column.Name = name;
                column.Type = TStringBuf(name) == "doc" ? "JsonDocument" : "Int32";
                column.TypeInfo = NScheme::TTypeInfo(TStringBuf(name) == "doc" ? NScheme::NTypeIds::JsonDocument : NScheme::NTypeIds::Int32);
            }
            auto& table = f.Tables->GetOrAddTable("ut", "/Root", metadata->Name);
            table.Metadata = metadata;
            UNIT_ASSERT(table.Load(f.ExprCtx, false));
            auto arena = std::make_shared<TExpressionArena>();
            f.Props.NativeExpressions = arena;
            const auto input = arena->Column(doc, {K::JsonDocument, true});
            const auto json = arena->JsonValue(input, scenario == 7 ? "$.a[*]" :
                scenario == 8 ? "$.\"*\"" : scenario == 9 ? "$.a[1]" : "$.a");
            auto value = json;
            if (scenario == 0) value = arena->Cast(json, {K::Double});
            if (scenario == 5) value = arena->If(arena->Unary(EOpcode::IsNull, input), json, arena->Literal({K::Utf8}, std::string("fallback")));
            if (scenario == 10) value = arena->Binary(EOpcode::Concat, json,
                arena->JsonValue(arena->Column(doc, {K::JsonDocument, true}), "$.a"));
            const auto expression = [&](TExprId id) { return TExpression::FromNative(arena, id, &f.Props); };
            auto read = f.Read({key, doc, unused}); read->StorageType = NYql::EStorageType::ColumnStorage;
            read->TableRef = MakeRboTable(*metadata);
            const auto alias = scenario == 6 ? f.Id("doc") : doc;
            if (scenario == 6) read->GetColumns().Add(alias);
            auto predicate = arena->Binary(EOpcode::Greater, arena->Column(key, {K::Int32, true}), arena->Literal({K::Int32}, std::int64_t{0}));
            if (scenario == 11) predicate = arena->Binary(EOpcode::And, predicate, arena->Unary(EOpcode::IsNotNull, input));
            read->OlapProgram = TOlapProgram(expression(predicate));
            TMapIUs definitions; definitions.Add(result, TMapElement(expression(value)));
            TOrderedIUs<TString> outputs{{result, "result"}};
            if (scenario == 1) outputs.Append(doc, "original");
            if (scenario == 6) outputs.Append(alias, "original");
            if (scenario >= 2 && scenario <= 4) {
                const auto other = scenario == 2 ? input : arena->JsonValue(input, scenario == 3 ? "$.b" : "$.a");
                definitions.Add(second, TMapElement(expression(other))); outputs.Append(second, "second");
            }
            auto map = MakeIntrusive<TOpMap>(read, f.Pos, std::move(definitions));
            auto root = f.Root(map, std::move(outputs));
            auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
                UNIT_FAIL("native OLAP projection invoked YQL annotation");
                return IGraphTransformer::TStatus::Error;
            });
            TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
            UNIT_ASSERT(ComputeTypesWithoutBackend(*root, context) == IGraphTransformer::TStatus::Ok);
            TConstantFoldingStage().RunStage(*root, context);
            ComputePlanLiveness(*root);
            TPushOlapProjectionRule rule;
            const auto pushed = rule.SimpleMatchAndApply(map, context, root->PlanProps);
            const bool eligible = scenario == 0 || scenario == 4 || scenario == 8 || scenario == 9 || scenario == 10 || scenario == 11;
            UNIT_ASSERT_VALUES_EQUAL_C(pushed != map, eligible, scenario);
            root->SetChild(0, pushed); root->RecomputeOutputIUsSubtree(); root->ComputeParents();
            UNIT_ASSERT(ComputeTypesWithoutBackend(*root, context) == IGraphTransformer::TStatus::Ok);
            TGlobalPruningStage("native projections").RunStage(*root, context);
            auto& source = CastOperator<TOpRead>(*CastOperator<TOpMap>(*root->GetInput()).GetInput());
            UNIT_ASSERT(source.GetColumns() == (TUnorderedIUs{key, doc, alias}));
            UNIT_ASSERT_VALUES_EQUAL(source.OlapProgram.Native().Projections.Items().size(), eligible ? 1 : 0);
            for (const auto& op : *root) for (const auto& expr : op.Current->GetExpressions())
                UNIT_ASSERT(expr.get().IsNative());
            if (!eligible) continue;
            const auto& projected = source.OlapProgram.Native().Projections.At(doc);
            UNIT_ASSERT(projected.GetNativeArena() == arena);
            UNIT_ASSERT(projected.GetRawInputIUs() == TUnorderedIUs{doc});
            TSubstitutions renames;
            const auto copied = source.Copy(root->PlanProps, renames);
            UNIT_ASSERT(copied);
            const auto& copy = CastOperator<TOpRead>(*copied);
            UNIT_ASSERT(copy.OlapProgram.Native().Projections.At(*renames.Find(doc)).GetRawInputIUs() == TUnorderedIUs{*renames.Find(doc)});
            const auto lowered = LowerNativeOlapProgram(copy.OlapProgram, f.ExprCtx, f.Pos);
            UNIT_ASSERT_VALUES_EQUAL(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapJsonValue"); }).size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(FindNodes(lowered, [](const auto& node) { return node->IsCallable("KqpOlapFilter"); }).size(), 1);
            UNIT_ASSERT(lowered->Tail().IsCallable("KqpOlapProjections"));
            UNIT_ASSERT(lowered->Tail().Head().IsCallable("KqpOlapFilter"));
            if (scenario == 11) {
                const auto& filter = *copy.OlapProgram.Native().Predicate;
                UNIT_ASSERT(filter.GetRawInputIUs() == (TUnorderedIUs{*renames.Find(key), *renames.Find(doc)}));
                const auto& filterArena = *filter.GetNativeArena();
                const auto& documentCheck = filterArena[filterArena[filter.GetNativeRoot()].Arguments[1]];
                UNIT_ASSERT(documentCheck.Op == EOpcode::IsNotNull);
                const auto& document = filterArena[documentCheck.Arguments[0]];
                UNIT_ASSERT_VALUES_EQUAL(document.ColumnId, *renames.Find(doc));
                UNIT_ASSERT(document.Type == (TType{K::JsonDocument, true}));
            }
            UNIT_ASSERT(source.PruneOutputs({key}, f.ExprCtx));
            UNIT_ASSERT(source.OlapProgram.Native().Predicate);
            UNIT_ASSERT(source.OlapProgram.Native().Projections.Keys().Empty());
            UNIT_ASSERT(source.GetColumns() == (scenario == 11 ? TUnorderedIUs{key, doc} : TUnorderedIUs{key}));
        }
    }

    Y_UNIT_TEST(OlapPredicatesStayNativeThroughPushdownPruningAndCopy) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        f.Config->SetEnableOlapScalarApply(false);
        const auto key = f.Id("key"), payload = f.Id("payload"), unused = f.Id("unused");
        auto read = f.Read({key, payload, unused});
        read->StorageType = NYql::EStorageType::ColumnStorage;
        SetNativeReadType(f, *read, {key});
        auto arena = std::make_shared<TExpressionArena>();
        const auto value = arena->Column(key, {ETypeKind::Int32, true});
        const auto positive = arena->Binary(EOpcode::Greater, value, arena->Literal({ETypeKind::Int32}, std::int64_t{0}));
        auto expression = TExpression::FromNative(arena, positive, &f.Props);
        auto filter = MakeIntrusive<TOpFilter>(read, f.Pos, expression);
        auto forbidden = CreateFunctorTransformer([](NYql::TExprNode::TPtr, NYql::TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("native OLAP pushdown invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        TPushOlapFilterRule rule;
        const auto pushed = rule.SimpleMatchAndApply(filter, context, f.Props);
        UNIT_ASSERT(pushed->Kind == EOperator::Source);
        auto& source = CastOperator<TOpRead>(*pushed);
        UNIT_ASSERT(source.OlapProgram.IsNative());

        UNIT_ASSERT(source.GetRequiredColumns(f.ExprCtx) == TUnorderedIUs{key});
        TSubstitutions renames;
        const auto copied = source.Copy(f.Props, renames);
        UNIT_ASSERT(copied);
        const auto& copy = CastOperator<TOpRead>(*copied);
        UNIT_ASSERT(copy.OlapProgram.IsNative());
        UNIT_ASSERT(copy.OlapProgram.Native().Predicate->GetNativeArena() == arena);
        UNIT_ASSERT(copy.GetRequiredColumns(f.ExprCtx) == TUnorderedIUs{*renames.Find(key)});
        // Discarding NULL before a checked expression could suppress an error.
        const auto checked = arena->Ensure(value, arena->Literal({ETypeKind::Bool}, false), "checked");
        const auto failure = arena->Binary(EOpcode::Greater, checked, arena->Literal({ETypeKind::Int32}, std::int64_t{0}));
        const auto text = arena->Cast(value, {ETypeKind::String});
        const auto length = arena->Binary(EOpcode::Greater, arena->Unary(EOpcode::Length, text), arena->Literal({ETypeKind::Uint32}, std::uint64_t{1}));
        for (const bool canFail : {false, true}) for (const bool first : {false, true}) {
            const auto remaining = canFail ? failure : length;
            const auto predicate = arena->Binary(EOpcode::And, first ? positive : remaining, first ? remaining : positive);
            auto fresh = f.Read({key, payload}); fresh->StorageType = NYql::EStorageType::ColumnStorage;
            auto original = MakeIntrusive<TOpFilter>(fresh, f.Pos, TExpression::FromNative(arena, predicate, &f.Props));
            const auto result = rule.SimpleMatchAndApply(original, context, f.Props);
            UNIT_ASSERT(result->Kind == EOperator::Filter);
            const auto& input = CastOperator<TOpRead>(*CastOperator<TOpFilter>(*result).GetInput());
            // The pre-PR splitter keeps supported conjuncts before the first
            // non-strict term, and does not move later terms across it.
            UNIT_ASSERT_VALUES_EQUAL(bool(input.OlapProgram), !canFail || first);
        }
        const auto external = arena->Column(unused, {ETypeKind::Int32});
        auto fresh = f.Read({key, payload}); fresh->StorageType = NYql::EStorageType::ColumnStorage;
        const auto externalFilter = MakeIntrusive<TOpFilter>(fresh, f.Pos,
            TExpression::FromNative(arena, arena->Binary(EOpcode::Greater, value, external), &f.Props));
        UNIT_ASSERT(rule.SimpleMatchAndApply(externalFilter, context, f.Props) == externalFilter);
        const auto mixedExternal = MakeIntrusive<TOpFilter>(fresh, f.Pos,
            TExpression::FromNative(arena, arena->Binary(EOpcode::And, positive,
                externalFilter->GetFilterExpression().GetNativeRoot()), &f.Props));
        UNIT_ASSERT(rule.SimpleMatchAndApply(mixedExternal, context, f.Props) == mixedExternal);
        const auto parameter = arena->Parameter("flag", {ETypeKind::Bool, true});
        for (const bool first : {false, true}) {
            const auto logical = arena->Binary(EOpcode::Or, first ? parameter : positive, first ? positive : parameter);
            UNIT_ASSERT(!CanLowerNativeOlapPredicate(*arena, logical));
        }
        UNIT_ASSERT(!CanLowerNativeOlapPredicate(*arena, parameter));
        // Root takes ownership of the fixture's plan properties.
        auto root = f.Root(pushed, {{payload, "payload"}});
        TGlobalPruningStage("prune").RunStage(*root, context);
        UNIT_ASSERT(source.GetColumns() == (TUnorderedIUs{key, payload}));
    }

    Y_UNIT_TEST(IntegerOperationsLowerDirectlyForEveryWidth) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (const auto a : {K::Int8, K::Uint8, K::Int16, K::Uint16, K::Int32, K::Uint32, K::Int64, K::Uint64})
            for (const auto b : {K::Int8, K::Uint8, K::Int16, K::Uint16, K::Int32, K::Uint32, K::Int64, K::Uint64})
                for (const int test : {0, 1, 2, 3, 4}) {
                    TExpressionArena arena;
                    auto input = [&](K kind, bool right) {
                        const unsigned width = kind == K::Int8 || kind == K::Uint8 ? 8 : kind == K::Int16 || kind == K::Uint16 ? 16 :
                            kind == K::Int32 || kind == K::Uint32 ? 32 : 64;
                        const TType type{kind, test == 2 || test == (right ? 4 : 3)};
                        if (test == (right ? 4 : 3)) return arena.Literal(type, std::monostate{});
                        TScalar value;
                        if (type.IsSigned()) {
                            const auto high = static_cast<std::int64_t>((std::uint64_t{1} << (width - 1)) - 1);
                            value = test == 0 ? std::int64_t{-1} : test == 1 ? high : test == 2 ? -high - 1 : std::int64_t{0};
                        } else {
                            const auto high = width == 64 ? std::numeric_limits<std::uint64_t>::max() : (std::uint64_t{1} << width) - 1;
                            value = test == 0 ? high : test == 1 ? high / 2 + 1 : std::uint64_t{0};
                        }
                        return arena.Literal(type, value);
                    };
                    const auto left = input(a, false), right = input(b, true);
                    for (const auto op : {EOpcode::Equal, EOpcode::NotEqual, EOpcode::Less, EOpcode::LessEqual, EOpcode::Greater, EOpcode::GreaterEqual,
                            EOpcode::Add, EOpcode::Subtract, EOpcode::Multiply, EOpcode::Divide, EOpcode::Modulo}) {
                        const auto root = arena.Binary(op, left, right);
                        CheckScalar(arena, root, "heterogeneous integer operation");
                    }
                    CheckScalar(arena, arena.If(arena.Literal({K::Bool}, false), left, right), "heterogeneous conditional");
                    const TExprId alternatives[]{left, right};
                    CheckScalar(arena, arena.Coalesce(alternatives), "heterogeneous coalesce");
                }
    }

    Y_UNIT_TEST(FloatingPromotionLowersWithoutAnnotation) {
        using namespace NFrontend;
        using K = ETypeKind;
        for (const bool nullable : {false, true}) {
            TExpressionArena arena;
            const auto integer = arena.Literal({K::Uint64, nullable}, std::uint64_t{9223372586610589697ULL});
            for (const auto kind : {K::Float, K::Double}) for (const double value : {0.0, -1.0,
                    std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
                const auto number = arena.Literal({kind}, value);
                CheckScalar(arena, arena.Binary(EOpcode::Add, integer, number), "integer/floating promotion");
                CheckScalar(arena, arena.Binary(EOpcode::Add, arena.Literal({K::Float, true}, value), arena.Literal({K::Double}, 1.0)),
                    "Float/Double promotion with optional lifting");
            }
        }
    }

    Y_UNIT_TEST(CommonConjunctExtractionKeepsNativeBindingsAndAllocatesNoBackendNodes) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto left = f.Id("left"), right = f.Id("right"), first = f.Id("first"), second = f.Id("second");
        auto arena = std::make_shared<TExpressionArena>();
        f.Props.NativeExpressions = arena;
        const auto equality = [&] {
            return arena->Binary(EOpcode::Equal, arena->Column(left, {ETypeKind::Int32}), arena->Column(right, {ETypeKind::Int32}));
        };
        const auto a = arena->Binary(EOpcode::And, equality(), arena->Column(first, {ETypeKind::Bool, true}));
        const auto b = arena->Binary(EOpcode::And, equality(), arena->Column(second, {ETypeKind::Bool, true}));
        const auto expression = TExpression::FromNative(arena, arena->Binary(EOpcode::Or, a, b), &f.Props);
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        const auto factored = expression.TryExtractCommonConjuncts();
        UNIT_ASSERT(factored && factored->IsNative());
        UNIT_ASSERT(factored->GetNativeType() == expression.GetNativeType());
        const auto conjuncts = factored->SplitConjunct();
        UNIT_ASSERT_VALUES_EQUAL(conjuncts.size(), 2);
        UNIT_ASSERT(conjuncts[0].MaybeEquiJoinCondition());
        UNIT_ASSERT_VALUES_EQUAL(TEquiJoinCondition(conjuncts[0]).GetLeftIU(), left);
        UNIT_ASSERT_VALUES_EQUAL(TEquiJoinCondition(conjuncts[0]).GetRightIU(), right);
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
    }

    Y_UNIT_TEST(ComputedEqualityExtractionUsesTypedDefinitionsWithoutBackendNodes) {
        using namespace NFrontend;
        NTests::TIdTestContext f;
        const auto left = f.Id("left"), right = f.Id("right");
        auto arena = std::make_shared<TExpressionArena>();
        f.Props.NativeExpressions = arena;
        const auto a = arena->Column(left, {ETypeKind::Int32});
        const auto b = arena->Column(right, {ETypeKind::Int32, true});
        const auto one = arena->Literal({ETypeKind::Int32}, std::int64_t{1});
        const auto sum = arena->Binary(EOpcode::Add, a, one);
        const auto predicate = arena->Binary(EOpcode::Equal, sum, b);
        const auto expression = TExpression::FromNative(arena, predicate, &f.Props);
        UNIT_ASSERT(!expression.MaybeEquiJoinCondition());
        UNIT_ASSERT(expression.MaybeExprEquiJoinCondition());
        const auto nodes = f.ExprCtx.NodeAllocationCounter;
        TMappedIUs<TExpression> definitions;
        const auto rewritten = TEquiJoinCondition(expression).ExtractExpressions(definitions);
        UNIT_ASSERT(rewritten && rewritten->IsNative() && rewritten->MaybeEquiJoinCondition());
        UNIT_ASSERT_VALUES_EQUAL(definitions.Keys().Size(), 1);
        const auto id = *definitions.Keys().begin();
        UNIT_ASSERT_VALUES_EQUAL(definitions.At(id).GetNativeRoot(), sum);
        UNIT_ASSERT_VALUES_EQUAL(TEquiJoinCondition(*rewritten).GetLeftIU(), id);
        UNIT_ASSERT_VALUES_EQUAL(TEquiJoinCondition(*rewritten).GetRightIU(), right);
        UNIT_ASSERT(rewritten->GetNativeType() == expression.GetNativeType());
        UNIT_ASSERT_VALUES_EQUAL(f.ExprCtx.NodeAllocationCounter, nodes);
        UNIT_ASSERT_VALUES_EQUAL(expression.GetNativeRoot(), predicate);
        for (const auto value : {std::int64_t{-1}, std::int64_t{0}, std::int64_t{2147483647}}) {
            for (const auto match : {TScalar{}, TScalar{std::int64_t{0}}, TScalar{std::int64_t{-2147483648LL}}}) {
                TEvaluationContext source;
                source.Column = [&](TColumnId col) { return col == left ? TValue{{ETypeKind::Int32}, value} : TValue{{ETypeKind::Int32, true}, match}; };
                TEvaluationContext materialized;
                materialized.Column = [&](TColumnId col) { return col == id ? Evaluate(*arena, sum, source) : source.Column(col); };
                const auto expected = Evaluate(*arena, predicate, source), actual = Evaluate(*arena, rewritten->GetNativeRoot(), materialized);
                UNIT_ASSERT(actual.Type == expected.Type);
                UNIT_ASSERT_VALUES_EQUAL(FormatValue(actual), FormatValue(expected));
            }
        }
    }

    Y_UNIT_TEST(ComputedJoinKeysStayOnTheirOwnInputs) {
        for (const auto* kind : {"INNER", "LEFT", "RIGHT", "LEFT SEMI", "RIGHT SEMI", "LEFT ONLY", "RIGHT ONLY", "WHERE"}) {
            for (const auto* predicate : {"a.x + 1 = b.x", "a.x + a.y = b.x + b.y", "b.x + 1 = a.x", "a.x + 1 = b.x AND a.y < b.y", "a.x + 1 = b.x AND a.y + 2 = b.y"}) {
                NTests::TIdTestContext f;
                const bool where = TStringBuf(kind) == "WHERE";
                const TString sql = TStringBuilder() << "SELECT 1 AS result FROM "
                    << "(SELECT 1 AS x, CAST(2 AS Int32?) AS y) AS a " << (where ? "CROSS" : kind)
                    << " JOIN (SELECT 2 AS x, CAST(3 AS Int32?) AS y) AS b " << (where ? "WHERE " : "ON ") << predicate;
                auto roots = CompileNativeToRbo(std::string_view(sql.data(), sql.size()), NoTables, f.ExprCtx).Roots;
                auto& root = *roots.front();
                // ON normalization belongs to construction, before any RBO stage.
                if (!where) for (const auto& item : root) if (item.Current->Kind == EOperator::Join) {
                    UNIT_ASSERT_VALUES_EQUAL_C(CastOperator<TOpJoin>(*item.Current).JoinKeys.Items().size(),
                        TStringBuf(predicate).Contains("a.y + 2") ? 2 : 1, sql);
                }
                auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                    UNIT_FAIL("computed native join keys invoked YQL annotation");
                    return IGraphTransformer::TStatus::Error;
                });
                TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                TVector<std::unique_ptr<IRule>> rules;
                rules.emplace_back(std::make_unique<TExtractJoinExpressionsRule>());
                rules.emplace_back(std::make_unique<TPushMapElementsThroughInputRule>());
                rules.emplace_back(std::make_unique<TPushMapElementsIntoMapRule>());
                rules.emplace_back(std::make_unique<TPushFilterUnderMapRule>());
                rules.emplace_back(std::make_unique<TPushFilterIntoJoinRule>());
                TRuleBasedStage("native computed keys", std::move(rules)).RunStage(root, context);
                UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
                bool found = false;
                for (const auto& item : root) if (item.Current->Kind == EOperator::Join) {
                    const auto& join = CastOperator<TOpJoin>(*item.Current);
                    UNIT_ASSERT_VALUES_EQUAL_C(join.JoinKeys.Items().size(), TStringBuf(predicate).Contains("a.y + 2") ? 2 : 1, sql);
                    if (where) UNIT_ASSERT_VALUES_EQUAL_C(join.JoinKind, "Inner", sql);
                    for (const auto& key : join.JoinKeys.Items()) {
                        UNIT_ASSERT(join.GetLeftInput()->GetOutputIUs().Contains(key.first));
                        UNIT_ASSERT(join.GetRightInput()->GetOutputIUs().Contains(key.second));
                    }
                    found = true;
                }
                UNIT_ASSERT_C(found, sql);
                NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
                for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions()) {
                    UNIT_ASSERT(expression.get().IsNative());

                }
            }
        }
    }

    Y_UNIT_TEST(JoinExpressionExtractionKeepsFilterOnlyMatching) {
        NTests::TIdTestContext f;
        auto roots = CompileNativeToRbo("SELECT 1 FROM (SELECT 1 AS x) AS a "
            "CROSS JOIN (SELECT 2 AS x) AS b WHERE a.x + 1 = b.x", NoTables, f.ExprCtx).Roots;
        auto& root = *roots.front();
        auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("join expression matching invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        TExtractJoinExpressionsRule rule;
        bool checked = false;
        for (const auto& item : root) if (item.Current->Kind == EOperator::Filter) {
            auto& filter = CastOperator<TOpFilter>(*item.Current);
            auto input = filter.GetInput();
            if (input->Kind != EOperator::Join) continue;
            UNIT_ASSERT(rule.QuickMatch(TIntrusivePtr<IOperator>(item.Current)));
            auto& join = CastOperator<TOpJoin>(*input);
            // Predicates inside a join do not expand this rule's original
            // matching policy. Native ON normalization happens at handoff.
            join.JoinFilters = filter.GetFilterExpression().SplitConjunct();
            UNIT_ASSERT(!rule.QuickMatch(input));
            UNIT_ASSERT(!rule.MatchAndApply(input, context, root.PlanProps));
            UNIT_ASSERT(join.JoinKeys.Items().empty());
            UNIT_ASSERT_VALUES_EQUAL(join.JoinFilters.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(join.JoinKind, "Cross");
            checked = true;
        }
        UNIT_ASSERT(checked);
    }

    Y_UNIT_TEST(OptionalBooleanLiftsPreserveJoinKeys) {
        using namespace NFrontend;
        for (const auto& [condition, keys] : std::initializer_list<std::pair<const char*, std::size_t>>{
                {"a.id = b.id AND a.y = b.y", 2}, {"(a.id = b.id AND a.x = b.x) AND a.y = b.y", 3}}) {
            NTests::TIdTestContext f;
            const std::string sql = std::string("SELECT a.id, b.id AS right_id FROM ") +
                "(SELECT 1 AS id, 2 AS x, CAST(3 AS Int32?) AS y) AS a FULL JOIN " +
                "(SELECT 1 AS id, 2 AS x, CAST(3 AS Int32?) AS y) AS b ON " + condition;
            auto roots = CompileNativeToRbo(sql, NoTables, f.ExprCtx).Roots;
            auto& root = *roots.front();
            bool found = false;
            for (const auto& item : root) if (item.Current->Kind == EOperator::Join) {
                const auto& join = CastOperator<TOpJoin>(*item.Current);
                UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items().size(), keys);
                UNIT_ASSERT(join.JoinFilters.empty());
                found = true;
            }
            UNIT_ASSERT(found);
            auto arena = root.PlanProps.NativeExpressions;
            const auto equal = arena->Binary(EOpcode::Equal, arena->Column(0, {ETypeKind::Int32}), arena->Column(1, {ETypeKind::Int32}));
            const auto lifted = TExpression::FromNative(arena, arena->Convert(equal, {ETypeKind::Bool, true}), &root.PlanProps);
            UNIT_ASSERT(lifted.MaybeEquiJoinCondition());
            const TEquiJoinCondition equality(lifted);
            UNIT_ASSERT_VALUES_EQUAL(equality.GetLeftIU(), 0);
            UNIT_ASSERT_VALUES_EQUAL(equality.GetRightIU(), 1);
        }
    }

    Y_UNIT_TEST(CheckedUnwrapIsExecutableAndPreservesLazyFailures) {
        using namespace NFrontend;
        TExpressionArena arena;
        for (const auto value : {std::int64_t{-5}, std::int64_t{0}, std::int64_t{12}}) {
            const auto unwrapped = arena.Unary(EOpcode::Unwrap, arena.Literal({ETypeKind::Int64, true}, value));
            UNIT_ASSERT(arena[unwrapped].Type == (TType{ETypeKind::Int64}));
            CheckScalar(arena, unwrapped, "internal checked unwrap");
            const TExprId roots[]{unwrapped};
            const auto folded = arena.FoldConstants(roots).front();
            UNIT_ASSERT(arena[folded].Op == EOpcode::Literal);
            UNIT_ASSERT(arena[folded].Type == arena[unwrapped].Type);
        }
        const auto failed = arena.Unary(EOpcode::Unwrap, arena.Literal({ETypeKind::Int64, true}, std::monostate{}));
        UNIT_ASSERT_EXCEPTION(Evaluate(arena, failed), TFrontendError);
        const TExprId roots[]{failed};
        UNIT_ASSERT_VALUES_EQUAL(arena.FoldConstants(roots).front(), failed);
        const auto lazy = arena.If(arena.Literal({ETypeKind::Bool}, false), failed, arena.Literal({ETypeKind::Int64}, std::int64_t{7}));
        CheckScalar(arena, lazy, "unused failing unwrap");
        const auto failedMessage = arena.Unary(EOpcode::Unwrap, arena.Literal({ETypeKind::Utf8, true}, std::monostate{}));
        const TExprId args[]{arena.Literal({ETypeKind::Int64, true}, std::int64_t{8}), failedMessage};
        const auto withMessage = arena.Call("Unwrap", args);
        CheckScalar(arena, withMessage, "unused failing error message");
        const TExprId withMessageRoots[]{withMessage};
        CheckScalar(arena, arena.FoldConstants(withMessageRoots).front(), "folded unwrap with unused message");
    }

    Y_UNIT_TEST(WindowConversionKeepsTypedNativeExpressions) {
        for (const auto* sql : {
            "SELECT x, SUM(x) OVER() AS s, COUNT(*) OVER() AS n FROM (SELECT 1 AS x UNION ALL SELECT 2) AS t",
            "SELECT p, SUM(x) OVER(PARTITION BY p) AS s FROM (SELECT CAST(NULL AS Int32) AS p, 1 AS x UNION ALL SELECT CAST(NULL AS Int32), 2) AS t",
            "SELECT x, SUM(x) OVER(ORDER BY x ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS s, Rank() OVER(ORDER BY x) AS r FROM (SELECT 1 AS x UNION ALL SELECT 2) AS t",
            "SELECT SUM(x) OVER() + 1 IN (SELECT CAST(4 AS Int64)) FROM (SELECT 1 AS x UNION ALL SELECT 2) AS t"}) {
            NTests::TIdTestContext f;
            f.Config->SetEnableWindowFunctionsV2(true);
            auto roots = CompileNativeToRbo(sql, NoTables, f.ExprCtx).Roots;
            auto& root = *roots.front();
            auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                UNIT_FAIL("native window invoked YQL expression annotation");
                return IGraphTransformer::TStatus::Error;
            });
            TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            TVector<std::unique_ptr<IRule>> windows;
            windows.emplace_back(std::make_unique<TExpandWholePartitionWindowRule>());
            TRuleBasedStage("native whole-partition windows", std::move(windows)).RunStage(root, context);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            TVector<std::unique_ptr<IRule>> copies;
            copies.emplace_back(std::make_unique<TExpandReplicateRule>());
            TRuleBasedStage("native window copies", std::move(copies)).RunStage(root, context);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions()) {
                UNIT_ASSERT(expression.get().IsNative());

            }
            NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
        }
    }

    Y_UNIT_TEST(CorrelatedRewritesKeepNativeExpressionsAndBindings) {
        for (const auto* sql : {
            "SELECT o.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o WHERE o.id IN (SELECT 1 UNION ALL SELECT CAST(NULL AS Int32))",
            "SELECT o.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o WHERE o.id NOT IN (SELECT 1 UNION ALL SELECT CAST(NULL AS Int32))",
            "SELECT o.id + 1 IN (SELECT CAST(2 AS Int64)) AS v FROM (SELECT 1 AS id) AS o",
            "SELECT (o.id IN (SELECT 1)) IN (SELECT true) AS v FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT NULL IN (SELECT NULL) AS v",
            "SELECT o.id IN (SELECT i.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS i WHERE i.id <= o.id) AS v FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT o.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o WHERE o.id NOT IN (SELECT i.id FROM (SELECT 1 AS id) AS i WHERE i.id < o.id)",
            "SELECT Decimal('1.25',8,2) IN (SELECT Decimal('1.250',9,3)) AS v",
            "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections; SELECT o.id IN (SELECT CAST(NULL AS Int32)) AS v FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT o.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o WHERE EXISTS(SELECT i.id FROM (SELECT 1 AS id) AS i WHERE i.id = o.id)",
            "SELECT o.id FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o WHERE NOT EXISTS(SELECT i.id FROM (SELECT 1 AS id) AS i WHERE i.id = o.id)",
            "SELECT o.id FROM (SELECT 1 AS id) AS o WHERE EXISTS(SELECT 1 WHERE false) OR EXISTS(SELECT 1 WHERE o.id = 1)",
            "SELECT EXISTS(SELECT i.id FROM (SELECT 1 AS id) AS i WHERE i.id = o.id) AS yes FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT EXISTS(SELECT 1 WHERE false) AS no, NOT EXISTS(SELECT 1) AS also_no",
            "SELECT EXISTS(SELECT 1 WHERE EXISTS(SELECT 1 WHERE o.id = 1)) AS yes FROM (SELECT 1 AS id) AS o",
            "SELECT (SELECT COUNT(*) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS i WHERE i.id > o.id) AS n FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT (SELECT MAX(i.id) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS i WHERE i.id = o.id) AS n FROM (SELECT 1 AS id UNION ALL SELECT CAST(NULL AS Int32) AS id) AS o",
            "SELECT (SELECT MAX(i.id - o.v) FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS i WHERE i.id <= o.id) AS n FROM (SELECT 1 AS id, 2 AS v) AS o",
            "SELECT (SELECT i.id + o.v FROM (SELECT 1 AS id) AS i WHERE i.id = o.id) AS n FROM (SELECT 1 AS id, 2 AS v) AS o",
            "SELECT (SELECT (SELECT o.id)) AS n FROM (SELECT 1 AS id) AS o",
            "SELECT (SELECT i.id FROM (SELECT 1 AS id UNION ALL SELECT 2 AS id) AS i WHERE i.id > o.id ORDER BY i.id DESC LIMIT 1) AS n FROM (SELECT 1 AS id) AS o"}) {
            NTests::TIdTestContext f;
            const auto nodes = f.ExprCtx.NodeAllocationCounter;
            const auto types = f.ExprCtx.TypeNodes.size();
            auto roots = CompileNativeToRbo(sql, NoTables, f.ExprCtx).Roots;
            auto& root = *roots.front();
            // Value subqueries are constructed at the frontend boundary. RBO
            // keeps its original filter-only IN/EXISTS rule dispatch.
            for (const auto& item : IterateSubtreeWithSubplans(&root, root.PlanProps)) {
                if (item.Current->Kind != EOperator::Map) continue;
                const auto map = TIntrusivePtr<IOperator>(item.Current);
                TInlineGenericInExistsSubplanRule rule;
                UNIT_ASSERT(!rule.QuickMatch(map));
                UNIT_ASSERT(!rule.QuickMatch(map, root.PlanProps));
                for (const auto call : map->GetSubplanIUs(root.PlanProps.Subplans)) {
                    UNIT_ASSERT(root.PlanProps.Subplans.At(call).Type == ESubplanType::EXPR);
                }
            }
            auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
                UNIT_FAIL("native correlation invoked YQL annotation");
                return IGraphTransformer::TStatus::Error;
            });
            TRBOContext context(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
            TGlobalPruningStage("native capture pruning").RunStage(root, context);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            TVector<std::unique_ptr<IRule>> inlineRules;
            inlineRules.emplace_back(std::make_unique<TInlineSimpleInExistsSubplanRule>());
            inlineRules.emplace_back(std::make_unique<TInlineGenericInExistsSubplanRule>());
            inlineRules.emplace_back(std::make_unique<TInlineScalarSubplanRule>());
            TRuleBasedStage("native scalar inlining", std::move(inlineRules)).RunStage(root, context);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            TConstantFoldingStage().RunStage(root, context);
            TVector<std::unique_ptr<IRule>> rules;
            rules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinNoFreeVarsRule>());
            rules.emplace_back(std::make_unique<TRewriteDependentJoinToCrossJoinRule>());
            rules.emplace_back(std::make_unique<TEliminateDependentJoinDomainRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughFilterRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughMapRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughAggregateRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughUnionAllRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughJoinRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughReplicateRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughSortRule>());
            rules.emplace_back(std::make_unique<TPushDependentJoinThroughLimitRule>());
            rules.emplace_back(std::make_unique<TDependentJoinNotSupportedRule>());
            TRuleBasedStage("native decorrelation", std::move(rules)).RunStage(root, context);
            TGlobalPruningStage("native correlation pruning").RunStage(root, context);
            TVector<std::unique_ptr<IRule>> copies;
            copies.emplace_back(std::make_unique<TExpandReplicateRule>());
            TRuleBasedStage("native replicate expansion", std::move(copies)).RunStage(root, context);
            UNIT_ASSERT(ComputeTypesWithoutBackend(root, context) == IGraphTransformer::TStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL_C(f.ExprCtx.NodeAllocationCounter, nodes, sql);
            UNIT_ASSERT_VALUES_EQUAL_C(f.ExprCtx.TypeNodes.size(), types, sql);
            UNIT_ASSERT(root.PlanProps.Subplans.Empty());
            NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
            for (const auto& item : root) {
                UNIT_ASSERT(item.Current->Kind != EOperator::DependentJoin);
                UNIT_ASSERT(item.Current->Kind != EOperator::AddDependencies);
                for (const auto& expression : item.Current->GetExpressions()) {
                    UNIT_ASSERT(expression.get().IsNative());

                }
            }
        }
    }

    Y_UNIT_TEST(StableScalarKeysMatchRuntimeBytes) {
        using namespace NFrontend;
        using K = ETypeKind;
        auto check = [](TType type, const std::vector<TScalar>& values) {
            TExpressionArena arena;
            for (const auto& value : values) {
                for (const auto inputType : {type, type.Optional()}) {
                    const auto key = arena.Unary(EOpcode::StablePickle, arena.Literal(inputType, value));
                    CheckScalar(arena, key, type.ToString());
                    const TExprId roots[]{key};
                    const auto folded = arena.FoldConstants(roots).front();
                    UNIT_ASSERT(arena[folded].Op == EOpcode::Literal);
                    UNIT_ASSERT_VALUES_EQUAL(std::get<std::string>(arena[folded].Value),
                        std::get<std::string>(Evaluate(arena, key).Data));
                }
            }
            CheckScalar(arena, arena.Unary(EOpcode::StablePickle,
                arena.Literal(type.Optional(), std::monostate{})), type.ToString());
        };
        check({K::Bool}, {false, true});
        for (const auto [kind, width] : {std::pair{K::Int8, 8u}, {K::Int16, 16u}, {K::Int32, 32u}, {K::Int64, 64u}}) {
            const auto max = static_cast<std::int64_t>((std::uint64_t{1} << (width - 1)) - 1);
            std::vector<TScalar> values{std::int64_t{0}, std::int64_t{-1}, max, -max - 1};
            for (unsigned bit = 6; bit < width - 1; bit += 7) {
                const auto boundary = std::int64_t{1} << bit;
                for (const auto value : {boundary - 1, boundary, -boundary, -boundary - 1}) values.push_back(value);
            }
            check({kind}, values);
        }
        for (const auto [kind, width] : {std::pair{K::Uint8, 8u}, {K::Uint16, 16u}, {K::Uint32, 32u}, {K::Uint64, 64u}}) {
            std::vector<TScalar> values{std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max() >> (64 - width)};
            for (unsigned bit = 7; bit < width; bit += 7) {
                const auto boundary = std::uint64_t{1} << bit;
                values.push_back(boundary - 1);
                values.push_back(boundary);
            }
            check({kind}, values);
        }
        for (const auto kind : {K::Float, K::Double}) {
            check({kind}, {0.0, -0.0, 1.25, -42.5, std::numeric_limits<double>::denorm_min(),
                std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                std::numeric_limits<double>::quiet_NaN(), std::bit_cast<double>(0xfff123456789abcdULL)});
        }
        check({K::Date}, {std::uint64_t{0}, std::uint64_t{127}, std::uint64_t{128}, std::uint64_t{49672}});
        check({K::Datetime}, {std::uint64_t{0}, std::uint64_t{16384}, std::uint64_t{4000000000}});
        check({K::Timestamp}, {std::uint64_t{0}, std::uint64_t{2097152}, std::uint64_t{4000000000000000}});
        check({K::Interval}, {std::int64_t{0}, std::int64_t{-16384}, std::int64_t{4000000000000000}});
        for (const auto kind : {K::String, K::Utf8}) {
            check({kind}, {std::string{}, std::string("a\0b", 3), std::string(7, 'x'), std::string(8, 'x'), std::string(257, 'x')});
        }
        std::vector<TScalar> decimals;
        for (const __int128 value : {__int128{0}, __int128{-1}, __int128{1}, TDecimal::Inf() - 1,
                -TDecimal::Inf() + 1, TDecimal::Inf(), -TDecimal::Inf(), TDecimal::NaN()}) decimals.push_back(TDecimal{value});
        for (unsigned bit = 8; bit < 112; bit += 8) {
            const auto boundary = __int128{1} << bit;
            for (const auto value : {boundary - 1, boundary, -boundary, -boundary - 1}) decimals.push_back(TDecimal{value});
        }
        check({K::Decimal, false, 35, 2}, decimals);
        TExpressionArena arena;
        CheckScalar(arena, arena.Unary(EOpcode::StablePickle, arena.Null()), "Null key");
    }

    Y_UNIT_TEST(ScalarSubplansRemainNativeThroughTyping) {
        NTests::TIdTestContext f;
        auto roots = CompileNativeToRbo("SELECT (SELECT (SELECT 1) + 2) AS x, (SELECT 7) AS y", NoTables, f.ExprCtx).Roots;
        auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("native scalar subquery invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext nativeContext(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        auto& root = *roots.front();
        UNIT_ASSERT(!root.PlanProps.Subplans.Empty());
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, nativeContext) == IGraphTransformer::TStatus::Ok);
        for (const auto& item : IterateSubtreeWithSubplans(&root, root.PlanProps)) {
            for (const auto& expression : item.Current->GetExpressions()) {
                UNIT_ASSERT(expression.get().IsNative());

                UNIT_ASSERT(expression.get().PlanProps == &root.PlanProps);
            }
        }
    }

    Y_UNIT_TEST(NativeAssertionsExecuteWithoutCompatibilityLowering) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto value = arena.Literal({ETypeKind::Int32}, std::int64_t{42});
        const auto no = arena.Literal({ETypeKind::Bool}, false);
        const auto yes = arena.Literal({ETypeKind::Bool}, true);
        const auto failed = arena.Ensure(value, no, "Scalar subquery returned more than one row");
        CheckScalar(arena, arena.Ensure(value, yes, "unused"), "successful assertion");
        CheckScalar(arena, arena.If(no, failed, value), "assertion in an unselected branch");
        TExprContext ctx;
        const auto lambda = LowerNativeLambda(arena, failed, ctx, {});
        UNIT_ASSERT(AreAllNodesTypeAnnotated(*lambda));
        UNIT_ASSERT(!NeedsRboCompatibilityLowering(lambda));
        NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
        NYql::NCommon::TMkqlCommonCallableCompiler compiler;
        NYql::NCommon::TMkqlBuildContext build(compiler, *setup.PgmBuilder, ctx);
        auto graph = setup.BuildGraph(NYql::NCommon::MkqlBuildLambda(*lambda, build, {setup.PgmBuilder->NewEmptyStruct()}));
        UNIT_ASSERT_EXCEPTION_CONTAINS(graph->GetValue(), std::exception, "Scalar subquery returned more than one row");
    }

    Y_UNIT_TEST(ConstantFoldingRemainsNative) {
        NTests::TIdTestContext f;
        auto roots = CompileNativeToRbo("SELECT 1 + 2 AS answer WHERE 2 + 3 = 5 LIMIT 1ul + 1ul", NoTables, f.ExprCtx).Roots;
        auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("native constant folding invoked YQL annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext nativeContext(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        auto& root = *roots.front();
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, nativeContext) == IGraphTransformer::TStatus::Ok);
        TConstantFoldingStage().RunStage(root, nativeContext);
        bool folded = false, filterFolded = false, limitFolded = false;
        for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions()) {
            UNIT_ASSERT(expression.get().IsNative());

            const auto& node = (*expression.get().GetNativeArena())[expression.get().GetNativeRoot()];
            folded |= node.Op == NFrontend::EOpcode::Literal && NFrontend::FormatValue({node.Type, node.Value}) == "3";
            if (item.Current->Kind == EOperator::Filter) {
                UNIT_ASSERT(node.Op == NFrontend::EOpcode::Literal && std::get<bool>(node.Value));
                filterFolded = true;
            }
            if (item.Current->Kind == EOperator::Limit) {
                UNIT_ASSERT(node.Op == NFrontend::EOpcode::Literal && std::get<std::uint64_t>(node.Value) == 2);
                limitFolded = true;
            }
        }
        UNIT_ASSERT(folded && filterFolded && limitFolded);
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, nativeContext) == IGraphTransformer::TStatus::Ok);
    }

    Y_UNIT_TEST(DecimalIntegerOperationsMatchRuntimeArithmetic) {
        namespace D = NYql::NDecimal;
        using namespace NFrontend;
        auto check = []<class T>(ETypeKind kind) {
            for (const auto [precision, scale] : {std::pair{3, 2}, {8, 2}, {35, 2}, {35, 35}}) {
                const TType type{ETypeKind::Decimal, false, static_cast<std::uint8_t>(precision), static_cast<std::uint8_t>(scale)};
                const auto bound = D::GetDivider(precision);
                const __int128 inputs[]{0, 1, -1, 2, -2, bound - 1, 1 - bound, D::Inf(), -D::Inf(), D::Nan()};
                for (const auto a : inputs) for (const T b : {T{0}, T{1}, T{2}, T{3}, T(-1), T(-2), T(-3), std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max()}) {
                    TExpressionArena arena;
                    const auto left = arena.Literal(type, TDecimal{a});
                    const TScalar factor = [&]() -> TScalar {
                        if constexpr (std::is_signed_v<T>) return std::int64_t{b};
                        else return std::uint64_t{b};
                    }();
                    const auto right = arena.Literal({kind}, factor);
                    const auto product = D::TDecimalMultiplicator<T>(precision).Do(a, b);
                    const std::pair<TExprId, __int128> operations[]{
                        {arena.Binary(EOpcode::Multiply, left, right), product}, {arena.Binary(EOpcode::Multiply, right, left), product},
                        {arena.Binary(EOpcode::Divide, left, right), D::TDecimalDivisor<T>().Do(a, b)}};
                    for (const auto& [result, expected] : operations) {
                        UNIT_ASSERT(arena[result].Type == type);
                        const auto value = Evaluate(arena, result);
                        UNIT_ASSERT_C(std::get<TDecimal>(value.Data).Coefficient == expected,
                            TStringBuilder() << type.ToString() << " op " << int(arena[result].Op) << " " << TType{kind}.ToString() << ": "
                                << FormatValue({type, TDecimal{a}}) << ", " << FormatValue({{kind}, factor}) << " -> " << FormatValue(value));
                        const TExprId roots[]{result};
                        const auto folded = arena.FoldConstants(roots).front();
                        UNIT_ASSERT(arena[folded].Type == type);
                        UNIT_ASSERT(std::get<TDecimal>(Evaluate(arena, folded).Data).Coefficient == expected);
                    }
                }
            }
        };
        check.operator()<i8>(ETypeKind::Int8); check.operator()<ui8>(ETypeKind::Uint8);
        check.operator()<i16>(ETypeKind::Int16); check.operator()<ui16>(ETypeKind::Uint16);
        check.operator()<i32>(ETypeKind::Int32); check.operator()<ui32>(ETypeKind::Uint32);
        check.operator()<i64>(ETypeKind::Int64); check.operator()<ui64>(ETypeKind::Uint64);
        for (const auto* type : {"Int8", "Int16", "Int32", "Int64", "Uint8", "Uint16", "Uint32", "Uint64"}) {
            for (const auto* value : {"0", "0.01", "-0.01", "9.99", "inf", "nan"}) {
                const auto sql = TStringBuilder() << "SELECT Decimal('" << value << "',3,2) * " << type << "('127')";
                CheckScalar(std::string_view{sql.data(), sql.size()});
                for (const auto* divisor : {"0", "3", std::string_view(type).starts_with("Int") ? "-3" : "255"}) {
                    const auto division = TStringBuilder() << "SELECT Decimal('" << value << "',3,2) / " << type << "('" << divisor << "')";
                    CheckScalar(std::string_view{division.data(), division.size()});
                }
            }
        }
        CheckScalar("SELECT Decimal('0',3,2) * Uint64('18446744073709551615')");
        CheckScalar("SELECT IF(false, Int64('1')) * Decimal('1.25',3,2)");
        CheckScalar("SELECT Decimal('0',35,35) / Uint64('18446744073709551615')");
        CheckScalar("SELECT Decimal('1.25',3,2) / Int64('-9223372036854775808')");
        CheckScalar("SELECT Decimal('1.25',3,2) / IF(false, Int64('1'))");
        CheckScalar("SELECT IF(false, Decimal('1.25',3,2)) / 2");
    }

    Y_UNIT_TEST(DecimalValuesMatchRuntimeArithmetic) {
        namespace D = NYql::NDecimal;
        using namespace NFrontend;
        using O = EOpcode;
        for (const auto [precision, scale] : {std::pair{2, 0}, {8, 2}, {35, 2}, {35, 35}}) {
            const TType type{ETypeKind::Decimal, false, static_cast<std::uint8_t>(precision), static_cast<std::uint8_t>(scale)};
            __int128 bound = 1;
            for (int i = 0; i < precision; ++i) bound *= 10;
            const __int128 inputs[]{0, 1, -1, 2, -2, bound - 1, 1 - bound, D::Inf(), -D::Inf(), D::Nan()};
            for (const auto a : inputs) for (const auto b : inputs) {
                TExpressionArena arena;
                const auto left = arena.Literal(type, TDecimal{a}), right = arena.Literal(type, TDecimal{b});
                const std::pair<O, __int128> arithmetic[]{
                    {O::Add, D::Add(a, b, precision)}, {O::Subtract, D::Sub(a, b, precision)},
                    {O::Multiply, D::TDecimalMultiplicator<D::TInt128>(precision, scale).Do(a, b)},
                    {O::Divide, D::TDecimalDivisor<D::TInt128>(precision, scale).Do(a, b)}, {O::Modulo, D::Mod(a, b)}};
                for (const auto [op, expected] : arithmetic) {
                    const auto result = arena.Binary(op, left, right);
                    UNIT_ASSERT(arena[result].Type == type);
                    const auto value = Evaluate(arena, result);
                    UNIT_ASSERT_C(std::get<TDecimal>(value.Data).Coefficient == expected,
                        TStringBuilder() << type.ToString() << " op " << int(op) << " ("
                            << FormatValue({type, TDecimal{a}}) << ", " << FormatValue({type, TDecimal{b}}) << ") -> " << FormatValue(value));
                }
                const std::pair<O, bool> comparisons[]{
                    {O::Equal, D::IsEqual(a, b, 0)}, {O::NotEqual, !D::IsEqual(a, b, 0)},
                    {O::Less, D::IsLess(a, b, 0)}, {O::LessEqual, D::IsLessOrEqual(a, b, 0)},
                    {O::Greater, D::IsGreater(a, b, 0)}, {O::GreaterEqual, D::IsGreaterOrEqual(a, b, 0)}};
                for (const auto [op, expected] : comparisons)
                    UNIT_ASSERT_VALUES_EQUAL(std::get<bool>(Evaluate(arena, arena.Binary(op, left, right)).Data), expected);
            }
            for (const std::string text : {"0", "+0", "-0", "1", "1.25", "-.5", "0.005", "0.015", "999.995", "inf", "-INF", "+NaN",
                "99999999999999999999999999999999999", "100000000000000000000000000000000001", "0000000000000000000000001.2300"}) {
                TExpressionArena arena;
                const auto input = arena.Literal({ETypeKind::String}, text);
                const auto value = Evaluate(arena, arena.Cast(input, type));
                UNIT_ASSERT_C(std::get<TDecimal>(value.Data).Coefficient == D::FromString(TString(text), precision, scale),
                    TStringBuilder() << text << " as " << type.ToString());
            }
        }
    }

    Y_UNIT_TEST(DateTextMatchesRuntimeLexicalContract) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto parsed = arena.Cast(arena.Parameter("text", {ETypeKind::String, true}), {ETypeKind::Date});
        auto check = [&](std::string text, bool lower = false) {
            TEvaluationContext context;
            context.Parameter = [&](std::string_view) { return TValue{{ETypeKind::String, true}, text}; };
            const auto actual = Evaluate(arena, parsed, context);
            const auto expected = NMiniKQL::ValueFromString(NUdf::EDataSlot::Date, NUdf::TStringRef(text.data(), text.size()));
            UNIT_ASSERT_VALUES_EQUAL_C(actual.IsNull(), !expected, text);
            if (expected) UNIT_ASSERT_VALUES_EQUAL_C(std::get<std::uint64_t>(actual.Data), expected.Get<ui16>(), text);
            if (lower) {
                TExpressionArena direct;
                // Keep parsing as a runtime cast; literal casts fold in binding.
                const auto input = direct.Binary(EOpcode::Concat, direct.Literal({ETypeKind::String}, text),
                    direct.Literal({ETypeKind::String}, std::string{}));
                CheckScalar(direct, direct.Cast(input, {ETypeKind::Date}), text);
            }
        };
        for (int year = 1969; year <= 2106; ++year) for (int month = 0; month <= 13; ++month) for (int day = 0; day <= 32; ++day) {
            check(std::to_string(year) + '-' + std::to_string(month) + '-' + std::to_string(day));
            check(std::to_string(year) + '-' + (month < 10 ? "0" : "") + std::to_string(month) + '-' + (day < 10 ? "0" : "") + std::to_string(day));
        }
        for (const std::string text : {"", " ", "1970-01-01", "1969-12-31", "2105-12-31", "2106-01-01", "2000-02-29", "2100-02-29",
                "1998-3-18", "1998-03-8", "01998-03-18", "+1998-03-18", "1998--03-18", "1998-003-18", "1998-03-018",
                " 1998-03-18", "1998-03-18 ", "1998-03-18\n", "1998-03-18Z", "1998-03-18T00:00:00Z", "1998/03/18", "１９９８-03-18"}) check(text, true);
        check(std::string{"1998-03-18\0", 11}, true);
        check(std::string{"1998-03-18\xff", 11}, true);
        TEvaluationContext null;
        null.Parameter = [](std::string_view) { return TValue{{ETypeKind::String, true}, {}}; };
        UNIT_ASSERT(Evaluate(arena, parsed, null).IsNull());
    }

    Y_UNIT_TEST(TextCastsMatchRuntimeLexicalContract) {
        using namespace NFrontend;
        for (const auto kind : {ETypeKind::Bool, ETypeKind::Float, ETypeKind::Double}) {
            const auto slot = kind == ETypeKind::Bool ? NUdf::EDataSlot::Bool :
                kind == ETypeKind::Float ? NUdf::EDataSlot::Float : NUdf::EDataSlot::Double;
            for (const std::string text : {"", " ", " 1", "1 ", "\t1", "1\n", "0", "-0", "+0", "1", "-1", "+1",
                ".5", "1.", "1e2", "-1.25E-2", "1e9999", "-1e9999", "1e-9999", "-1e-9999",
                "0x10", "-0x10", "0x1p2", "nan", "+NaN", "-NaN", "NaN(1)", "inf", "+Inf", "-Inf",
                "infinity", " inf", " true", "true", "TRUE", "false", "FALSE", "yes", "no", "bad", "1junk",
                "3.4028234663852886e38", "1.7976931348623157e308", "2.2250738585072014e-308", "4.9406564584124654e-324"}) {
                TExpressionArena arena;
                const auto input = arena.Literal({ETypeKind::String}, text);
                const auto result = Evaluate(arena, arena.Cast(input, {kind}));
                const auto expected = NMiniKQL::ValueFromString(slot, NUdf::TStringRef(text.data(), text.size()));
                const auto context = TStringBuilder() << TType{kind}.ToString() << ": '" << text << "'";
                UNIT_ASSERT_VALUES_EQUAL_C(result.IsNull(), !expected, context);
                if (!expected) continue;
                if (kind == ETypeKind::Bool) {
                    UNIT_ASSERT_VALUES_EQUAL_C(std::get<bool>(result.Data), expected.Get<bool>(), context);
                } else {
                    const auto value = std::get<double>(result.Data);
                    const auto number = kind == ETypeKind::Float ? double(expected.Get<float>()) : expected.Get<double>();
                    UNIT_ASSERT_C((std::isnan(value) && std::isnan(number)) || value == number, context);
                    if (value == 0) UNIT_ASSERT_VALUES_EQUAL_C(std::signbit(value), std::signbit(number), context);
                }
            }
        }
    }

    Y_UNIT_TEST(RuntimeTextCastsLowerWithoutSemanticAnnotation) {
        using namespace NFrontend;
        using K = ETypeKind;
        std::vector<std::optional<std::string>> cases{std::nullopt};
        for (const char* text : {"", " ", " 1", "1 ", "\t1", "1\n", "0", "-0", "+0", "1", "-1", "+1",
                ".5", "1.", "1e2", "-1.25E-2", "1e9999", "-1e9999", "1e-9999", "-1e-9999",
                "0x10", "-0x10", "0x1p2", "nan", "+NaN", "-NaN", "NaN(1)", "inf", "+Inf", "-Inf",
                "infinity", " inf", " true", "true", "TRUE", "false", "FALSE", "yes", "no", "bad", "1junk",
                "127", "128", "255", "256", "-128", "-129", "32767", "32768", "65535", "65536",
                "2147483647", "2147483648", "4294967295", "4294967296", "-2147483648", "-2147483649",
                "9223372036854775807", "9223372036854775808", "18446744073709551615", "18446744073709551616",
                "-9223372036854775808", "-9223372036854775809", "1.000000059604644775390625",
                "1.000000059604644775390626", "3.4028234663852886e38", "4.9406564584124654e-324"}) cases.emplace_back(text);
        cases.emplace_back(std::string("1\0", 2));
        for (const auto from : {K::String, K::Utf8}) for (const bool nullable : {false, true})
            for (const auto to : {K::Bool, K::Int8, K::Int16, K::Int32, K::Int64, K::Uint8, K::Uint16, K::Uint32, K::Uint64, K::Float, K::Double}) {
                TExpressionArena arena;
                const TType inputType{from, nullable};
                const auto root = arena.Cast(arena.Parameter("text", inputType), {to});
                UNIT_ASSERT(arena[root].Type == (TType{to, true}));
                for (const auto& text : cases) {
                    if (!nullable && !text) continue;
                    TEvaluationContext evaluation;
                    evaluation.Parameter = [&](std::string_view) { return TValue{inputType, text ? TScalar{*text} : TScalar{}}; };
                    const auto label = inputType.ToString() + " -> " + TType{to}.ToString() + ": " + text.value_or("NULL");
                    CheckScalar(arena, root, label, evaluation, [&](auto& build, auto& pb) {
                        const auto bytes = text.value_or("");
                        const NUdf::TStringRef ref(bytes.data(), bytes.size());
                        auto value = from == K::String ? pb.template NewDataLiteral<NUdf::EDataSlot::String>(ref) :
                            pb.template NewDataLiteral<NUdf::EDataSlot::Utf8>(ref);
                        if (nullable) value = text ? pb.NewOptional(value) : pb.NewEmptyOptional(pb.NewOptionalType(value.GetStaticType()));
                        build.Parameters = pb.NewStruct({{"$text", value}});
                    });
                }
            }
    }

    Y_UNIT_TEST(DirectRboAndNoYqlAnnotation) {
        NTests::TIdTestContext f;
        auto roots = CompileNativeToRbo(R"(
            SELECT a.id, b.id AS right_id, a.id + 2 AS next
            FROM (SELECT 1 AS id) AS a
            LEFT JOIN (SELECT CAST(1L AS Int32) AS id) AS b ON a.id == b.id
            WHERE a.id > 0 ORDER BY next LIMIT 5
        )", NoTables, f.ExprCtx).Roots;
        UNIT_ASSERT_VALUES_EQUAL(roots.size(), 1);
        auto& root = *roots.front();
        UNIT_ASSERT(root.PlanProps.NativeExpressions);
        bool joinFound = false;
        for (const auto& item : root) {
            for (const auto& expression : item.Current->GetExpressions()) {
                UNIT_ASSERT(expression.get().IsNative());

            }
            if (item.Current->Kind == EOperator::Join) {
                const auto& join = CastOperator<TOpJoin>(*item.Current);
                UNIT_ASSERT_VALUES_EQUAL(join.JoinKeys.Items().size(), 1);
                UNIT_ASSERT(join.JoinFilters.empty());
                joinFound = true;
            }
        }
        UNIT_ASSERT(joinFound);
        auto forbidden = CreateFunctorTransformer([](TExprNode::TPtr, TExprNode::TPtr&, TExprContext&) {
            UNIT_FAIL("native expression invoked YQL type annotation");
            return IGraphTransformer::TStatus::Error;
        });
        TRBOContext nativeContext(f.KqpCtx, f.ExprCtx, f.TypeCtx, *forbidden, *f.FuncRegistry);
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, nativeContext) == IGraphTransformer::TStatus::Ok);
        NTests::AssertIdInvariants(*root.GetInput(), root.PlanProps);
        TGlobalInliningStage inlining("native inlining");
        inlining.RunStage(root, nativeContext);
        TGlobalPruningStage pruning("native pruning");
        pruning.RunStage(root, nativeContext);
        UNIT_ASSERT(ComputeTypesWithoutBackend(root, nativeContext) == IGraphTransformer::TStatus::Ok);
        for (const auto& item : root) for (const auto& expression : item.Current->GetExpressions()) {
            UNIT_ASSERT(expression.get().IsNative());

        }
    }

    Y_UNIT_TEST(AggregationAndUnionHaveNativeInputs) {
        NTests::TIdTestContext f;
        auto roots = CompileNativeToRbo(R"(
            SELECT SUM(x) AS total, COUNT(*) AS n
            FROM (SELECT 1 AS x UNION ALL SELECT 2L AS x) AS q
            HAVING SUM(x) > 0
        )", NoTables, f.ExprCtx).Roots;
        UNIT_ASSERT(ComputeTypesWithoutBackend(*roots[0], f.RboCtx) == IGraphTransformer::TStatus::Ok);
        NTests::AssertIdInvariants(*roots[0]->GetInput(), roots[0]->PlanProps);
    }

    Y_UNIT_TEST(RuntimeColumnsParametersAndNumericCasts) {
        using namespace NFrontend;
        TExpressionArena arena;
        const auto column = arena.Column(0, {ETypeKind::Int32, true});
        const auto converted = arena.Cast(column, {ETypeKind::Int64});
        const auto fallback = arena.Literal({ETypeKind::Int64}, std::int64_t{42});
        const TExprId alternatives[]{converted, fallback};
        const auto parameter = arena.Parameter("p", {ETypeKind::Uint64});
        const auto result = arena.Binary(EOpcode::Add, arena.Coalesce(alternatives), arena.Cast(parameter, {ETypeKind::Int64}));
        for (const bool present : {false, true}) for (const ui64 parameterValue : {ui64{5}, std::numeric_limits<ui64>::max()}) {
            TEvaluationContext native;
            native.Column = [&](TColumnId) { return TValue{{ETypeKind::Int32, true}, present ? TScalar{std::int64_t{3}} : TScalar{std::monostate{}}}; };
            native.Parameter = [&](std::string_view) { return TValue{{ETypeKind::Uint64}, parameterValue}; };
            const auto expected = Evaluate(arena, result, native);
            TExprContext ctx;
            const auto lambda = LowerNativeLambda(arena, result, ctx, {});
            UNIT_ASSERT(AreAllNodesTypeAnnotated(*lambda));
            UNIT_ASSERT(!NeedsRboCompatibilityLowering(lambda));
            NMiniKQL::TSetup<false> setup(NMiniKQL::GetBuiltinFactory());
            auto& pb = *setup.PgmBuilder;
            auto value = present ? pb.NewOptional(pb.NewDataLiteral<i32>(3)) :
                pb.NewEmptyOptional(pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id)));
            auto row = pb.NewStruct({{"0", value}});
            NYql::NCommon::TMkqlCommonCallableCompiler compiler;
            NYql::NCommon::TMkqlBuildContext build(compiler, pb, ctx);
            build.Parameters = pb.NewStruct({{"$p", pb.NewDataLiteral<ui64>(parameterValue)}});
            auto graph = setup.BuildGraph(NYql::NCommon::MkqlBuildLambda(*lambda, build, {row}));
            auto actual = graph->GetValue();
            if (expected.IsNull()) UNIT_ASSERT(!actual);
            else UNIT_ASSERT_VALUES_EQUAL(actual.Get<i64>(), std::get<std::int64_t>(expected.Data));
        }
    }

    Y_UNIT_TEST(TerminalScalarExecution) {
        TString failures;
        for (auto sql : {
            "SELECT 1 + 2 * 3", "SELECT 1 / 0", "SELECT 7 / 2", "SELECT 9 % 4",
            "SELECT 1.0 / 0.0", "SELECT 0.0 / 0.0", "SELECT -1.0 / 0.0", "SELECT 7.0 / 2.0", "SELECT -3 + 2L",
            "SELECT CAST('123' AS Int64)", "SELECT CAST('bad' AS Int32)",
            "SELECT CAST(1 AS Int64)", "SELECT CAST(1 AS Uint8)",
            "SELECT COALESCE(NULL, CAST('bad' AS Int32), 7)",
            "SELECT CASE WHEN true THEN 7 ELSE 1/0 END",
            "SELECT 2 IN (1, NULL)", "SELECT NULL IS NULL", "SELECT NULL == NULL", "SELECT NULL AND NULL", "SELECT 4 IS NOT NULL",
            "SELECT Length('abcd')", "SELECT String::Contains('abcd', 'bc')",
            "SELECT StartsWith('abcd', 'ab')", "SELECT EndsWith('abcd', 'cd')", "SELECT 'a' || 'b'",
            "SELECT ABS(-3)", "SELECT NOT false", "SELECT true AND NULL", "SELECT true OR NULL",
            "SELECT Date('2020-01-02')", "SELECT Timestamp('2020-01-02T03:04:05Z')",
            "SELECT CAST('12.34' AS Decimal(10,2))",
            "SELECT CAST('1.23' AS Decimal(10,2)) / CAST('2' AS Decimal(10,2))",
            "SELECT CAST('1.23' AS Decimal(10,2)) * CAST('2' AS Decimal(10,2))",
            "SELECT CAST('999' AS Decimal(2,0))",
            "SELECT CAST(CAST('9.9' AS Decimal(2,1)) AS Int32)",
            "SELECT CAST(CAST('0.9' AS Decimal(2,1)) AS Int32)",
            "SELECT CAST('inf' AS Decimal(8,2)) - CAST('inf' AS Decimal(8,2))",
            "SELECT CAST('inf' AS Decimal(8,2)) * CAST('0' AS Decimal(8,2))",
            "SELECT CAST('1' AS Decimal(8,2)) / CAST('0' AS Decimal(8,2))",
            "SELECT CAST('0' AS Decimal(8,2)) / CAST('0' AS Decimal(8,2))",
            "SELECT CAST('1' AS Decimal(8,2)) % CAST('0' AS Decimal(8,2))",
            "SELECT -CAST('nan' AS Decimal(8,2))",
            "SELECT CAST('nan' AS Decimal(8,2)) != CAST('nan' AS Decimal(8,2))",
            "SELECT CAST(CAST('inf' AS Decimal(8,2)) AS Double)",
            "SELECT CAST(CAST('nan' AS Decimal(8,2)) AS Float)",
            "SELECT CAST(CAST('inf' AS Decimal(8,2)) AS String)",
            "SELECT Date('2020-01-01') + Interval('P1D')",
            "SELECT Date('2020-01-01') + DateTime::IntervalFromDays(60)",
            "SELECT DateTime::IntervalFromDays(-49672)",
            "SELECT DateTime::IntervalFromDays(49673)",
            "SELECT DateTime::IntervalFromDays(213503982)",
            "SELECT DateTime::IntervalFromHours(1)",
            "SELECT DateTime2::IntervalFromMinutes(IF(false, 1))",
            "SELECT CAST(Date('2020-01-02') AS Timestamp)",
            "SELECT DateTime::GetYear(Timestamp('2000-02-29T12:34:56.123456Z'))",
            "SELECT DateTime::GetMinute(Timestamp('2000-02-29T12:34:56.123456Z'))",
            "SELECT DateTime::GetMinute(CAST(NULL AS Timestamp))",
            "SELECT DateTime::ToSeconds(Timestamp('2000-02-29T12:34:56.123456Z'))",
            "SELECT DateTime::ToSeconds(Timestamp64('1969-12-31T23:59:59.999999Z'))",
            "SELECT DateTime::ToSeconds(Date32('0001-01-01'))",
            "SELECT Url::CutWWW(Url::GetHost('https://www.example.org:443/a?b'))",
            "SELECT Url::CutWWW('WWW.Example.org')",
            "SELECT Url::GetHost('http:///path')", "SELECT Url::GetHost(NULL)",
            "SELECT DateTime::MakeDate(DateTime::ShiftMonths(Date('2020-01-31'), 1))",
            "SELECT DateTime::MakeDatetime(DateTime::ShiftYears(Timestamp('2000-02-29T12:34:56.123456Z'), 100))",
            "SELECT DateTime::MakeTimestamp(DateTime::ShiftMonths(Timestamp('2020-03-31T12:34:56.123456Z'), -1))",
            "SELECT DateTime::MakeTimestamp(DateTime::ShiftYears(Timestamp('2000-02-29T12:34:56.123456Z'), 4096))",
            "SELECT DateTime::MakeDate(DateTime::ShiftYears(Date('1970-01-01'), -1))",
            "SELECT DateTime::MakeDate(DateTime::ShiftMonths(Date('2105-12-31'), 1))",
            "SELECT DateTime::GetYear(DateTime::ShiftMonths(IF(false, Date('2000-02-29')), 1))",
            "SELECT DateTime::MakeTimestamp(DateTime::Split(IF(false, Timestamp('2020-01-01T00:00:00Z'))))",
            "SELECT String::AsciiToLower('AbC')", "SELECT String::AsciiToUpper('AbC')", "SELECT String::AsciiToLower(CAST(NULL AS String))",
            "SELECT [1,2][0u]", "SELECT [1,2][2u]", "SELECT [1,2][-1]",
            "SELECT [1, IF(false, 2)][0u]", "SELECT [1, IF(false, 2)][1u]", "SELECT [1, IF(false, 2)][2u]",
            "SELECT Substring('abcdef', 2, 3)", "SELECT Substring('abcdef', NULL, 3)",
            "SELECT Substring('abcdef', 4294967295u, 3)", "SELECT Substring('abcdef', 2, NULL)",
            "SELECT Substring('abcdef', IF(false, 2u), IF(false, 3u))",
            "SELECT CAST(12 AS Double)", "SELECT CAST(12 AS Bool)", "SELECT CAST(true AS Int32)",
            "SELECT CAST(9223372586610589697ul AS Float)", "SELECT CAST(4611686293305294849L AS Float)",
            "SELECT CAST(Double('-4.9406564584124654e-324') AS Uint64)",
            "SELECT CAST(Float('-1e-40') AS Uint8)",
            "SELECT CAST(12 AS String)", "SELECT CAST(Utf8('hello') AS String)"
        }) {
            try {
                CheckScalar(sql);
            } catch (const std::exception& error) {
                failures += TStringBuilder() << sql << ": " << error.what() << "\n";
            }
        }
        UNIT_ASSERT_C(failures.empty(), failures);
    }

    Y_UNIT_TEST(TerminalSqlLikeExecution) {
        // The native evaluator delegates pattern matching to a runtime service.
        // Supply independent expected values and execute the emitted graph to
        // check both the SQL binding and the actual matching implementation.
        using namespace NFrontend;
        for (const auto& [sql, expected] : std::vector<std::pair<const char*, bool>>{
            {"SELECT 'abc' LIKE '%b%'", true}, {"SELECT 'ab' LIKE '%a%b%'", true},
            {"SELECT 'ab' LIKE 'a%b'", true}, {"SELECT 'abc' LIKE 'a%b'", false},
            {"SELECT 'abb' LIKE 'a%b'", true}, {"SELECT 'ab' LIKE 'ab%b'", false},
            {"SELECT 'aba' LIKE '%ab%ba%'", false}, {"SELECT 'abba' LIKE 'a%%b%a'", true},
            {"SELECT Utf8('abc') LIKE '%a%c%'", true},
            {"SELECT IF(false, 'abc') LIKE '%a%b%'", false}, {"SELECT NULL LIKE '%a%b%'", false}}) {
            const auto compiled = CompileNative(sql, [](auto, auto pos) -> TCatalogTable {
                throw TFrontendError(pos, "unexpected table lookup");
            });
            const auto& program = compiled.Program->Get();
            TEvaluationContext evaluation;
            evaluation.Like = [&](const TValue&, std::string_view, std::string_view, bool) { return expected; };
            CheckScalar(program.Expressions, Projection(program), sql, evaluation);
        }
    }
}
} // namespace NKikimr::NKqp
