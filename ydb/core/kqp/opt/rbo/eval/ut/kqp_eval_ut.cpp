#include <ydb/core/kqp/opt/rbo/eval/kqp_eval.h>
#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/utils/utf8.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/datetime/base.h>
#include <util/string/cast.h>
#include <util/system/env.h>

#include <type_traits>

namespace NKikimr::NKqp::NEval {
namespace {
using namespace NYql;
using NUdf::EDataSlot;

TExprNode::TPtr Compile(TStringBuf text, TExprContext& ctx) {
    auto ast = ParseAst(TStringBuilder() << "((return " << text << "))");
    UNIT_ASSERT_C(ast.IsOk(), ast.Issues.ToString());
    TExprNode::TPtr result;
    UNIT_ASSERT_C(CompileExpr(*ast.Root, result, ctx, nullptr, nullptr), ctx.IssueManager.GetIssues().ToString());
    return result;
}

TValuePtr Value(TSession& session, const TExprNode::TPtr& expression) {
    auto result = session.Evaluate(expression);
    UNIT_ASSERT_C(result.GetValue(),
        (result.GetError() ? ToString(int(result.GetError()->Code)) + "/" + ToString(int(result.GetError()->Resource))
                           : "missing result"));
    return *result.GetValue();
}

void Error(const TResult<TValuePtr>& result, EError code, EResource resource = EResource::None) {
    UNIT_ASSERT(result.GetError());
    UNIT_ASSERT_VALUES_EQUAL(int(result.GetError()->Code), int(code));
    UNIT_ASSERT_VALUES_EQUAL(int(result.GetError()->Resource), int(resource));
}

TString Points(size_t count) {
    TStringBuilder text;
    text << "(RangeUnion";
    for (size_t i = 0; i < count; ++i) {
        text << " (RangeFor '== (Int64 '" << 2 * i << ") (DataType 'Int64))";
    }
    return text << ')';
}

// The same test data goes independently to the AST evaluator and real MiniKQL
// RangeCreate/Union/Intersect/Multiply/Finalize computation nodes.
struct TBound {
    TVector<std::optional<i64>> Values;
    bool Included;
};

struct TInputRange {
    TBound Left;
    TBound Right;
};

TString BoundText(const TBound& bound) {
    TStringBuilder text;
    text << "'(";
    for (const auto& value : bound.Values) {
        if (value) {
            text << "(Just (Int64 '" << *value << ")) ";
        } else {
            text << "(Nothing (OptionalType (DataType 'Int64))) ";
        }
    }
    return text << "(Int32 '" << int(bound.Included) << "))";
}

TString RangeText(const TVector<TInputRange>& ranges) {
    TStringBuilder text;
    text << "(AsRange";
    for (const auto& range : ranges) {
        text << " '(" << BoundText(range.Left) << ' ' << BoundText(range.Right) << ')';
    }
    return text << ')';
}

NMiniKQL::TRuntimeNode RuntimeRanges(NMiniKQL::TProgramBuilder& pb, const TVector<TInputRange>& ranges) {
    using namespace NMiniKQL;
    const auto optional = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i64>::Id));
    const auto boundary = [&](const TBound& bound) {
        TVector<TRuntimeNode> values;
        for (const auto& value : bound.Values) {
            values.push_back(value ? pb.NewOptional(pb.NewDataLiteral<i64>(*value)) : pb.NewEmptyOptional(optional));
        }
        values.push_back(pb.NewDataLiteral<i32>(bound.Included));
        return pb.NewTuple(values);
    };
    TVector<TRuntimeNode> list;
    for (const auto& range : ranges) {
        list.push_back(pb.NewTuple({boundary(range.Left), boundary(range.Right)}));
    }
    return pb.RangeCreate(pb.AsList(list));
}

void CompareRuntime(const TVector<TInputRange>& a, const TVector<TInputRange>& b, TStringBuf op, bool finalize) {
    using namespace NMiniKQL;
    TSetup<false> setup(GetBuiltinFactory());
    auto& pb = *setup.PgmBuilder;
    const auto ra = RuntimeRanges(pb, a);
    const auto rb = RuntimeRanges(pb, b);
    auto program = op == "RangeUnion"       ? pb.RangeUnion({ra, rb})
                   : op == "RangeIntersect" ? pb.RangeIntersect({ra, rb})
                                            : pb.RangeMultiply({pb.NewDataLiteral<ui64>(256), ra, rb});
    TString expression = TStringBuilder() << '(' << op << (op == "RangeMultiply" ? " (Uint64 '256) " : " ")
                                          << RangeText(a) << ' ' << RangeText(b) << ')';
    if (finalize) {
        program = pb.RangeFinalize(program);
        expression = "(RangeFinalize " + expression + ')';
    }
    auto graph = setup.BuildGraph(program);
    const auto expected = graph->GetValue();
    TExprContext ctx;
    TSession session;
    auto value = Value(session, Compile(expression, ctx));
    const auto* actual = value->GetRanges();
    UNIT_ASSERT(actual);
    UNIT_ASSERT_VALUES_EQUAL_C(actual->Items.size(), expected.GetListLength(), expression);
    auto it = expected.GetListIterator();
    NUdf::TUnboxedValue item;
    for (const auto& range : actual->Items) {
        UNIT_ASSERT(it.Next(item));
        for (ui32 side = 0; side < 2; ++side) {
            const auto& bound = side ? range.Right : range.Left;
            const auto expectedBound = item.GetElement(side);
            for (ui32 i = 0; i < actual->Width; ++i) {
                const auto expectedValue = expectedBound.GetElement(finalize ? i : 2 * i + 1);
                UNIT_ASSERT_VALUES_EQUAL_C(bool(expectedValue), !bound.Components[i].Inf, expression);
                if (expectedValue) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedValue.Get<i64>(), bound.Components[i].Value.Data.Get<i64>(), expression);
                }
                if (!finalize) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        expectedBound.GetElement(2 * i).Get<i32>(), bound.Components[i].Inf, expression);
                }
            }
            const auto flag = expectedBound.GetElement(finalize ? actual->Width : 2 * actual->Width).Get<i32>();
            UNIT_ASSERT_VALUES_EQUAL_C(flag, finalize ? int(bool(bound.Included)) : bound.Included, expression);
        }
    }
}
}   // namespace

Y_UNIT_TEST_SUITE(KqpBoundedEvaluation) {
Y_UNIT_TEST(ScalarResultAndOwnership) {
    TValuePtr saved;
    TValuePtr text;
    {
        TExprContext ctx;
        TSession session;
        auto expression = Compile("(Uint64 '18446744073709551615)", ctx);
        saved = Value(session, expression);
        text = Value(session, Compile("(String 'owned)", ctx));
        UNIT_ASSERT(!expression->GetTypeAnn());
        UNIT_ASSERT(ctx.IssueManager.GetIssues().Empty());
    }
    UNIT_ASSERT_VALUES_EQUAL(saved->GetScalar()->Data.Get<ui64>(), Max<ui64>());
    UNIT_ASSERT_VALUES_EQUAL(text->GetScalar()->Text(), "owned");
}

Y_UNIT_TEST(ExplicitRefusals) {
    TExprContext ctx;
    TSession session;
    Error(session.Evaluate(Compile("(Parameter 'p (DataType 'Int64))", ctx)), EError::NonConstant);
    Error(session.Evaluate(Compile("(RandomNumber)", ctx)), EError::NonConstant);
    Error(session.Evaluate(Compile("(Unknown (Int64 '1))", ctx)), EError::Unsupported);
    Error(session.Evaluate(ctx.NewList(TPositionHandle{}, {Compile("(Int64 '1)", ctx)})), EError::Unsupported);
    Error(session.Evaluate(ctx.NewAtom(TPositionHandle{}, "value")), EError::Unsupported);
    Error(session.Evaluate(ctx.NewArgument(TPositionHandle{}, "value")), EError::NonConstant);
    Error(session.Evaluate(Compile("(Int64 'broken)", ctx)), EError::InvalidInput);
    Error(session.Evaluate(Compile("(Int64)", ctx)), EError::InvalidInput);
    Error(session.Evaluate({}), EError::InvalidInput);
    Error(session.Evaluate(Compile("(Just (Int64 '1))", ctx)), EError::Unsupported);
    Error(session.Evaluate(Compile("(ToString (Just (Utf8 'x)))", ctx)), EError::Unsupported);
    Error(session.Evaluate(Compile("(RangeFor '== (Just (Nothing (OptionalType (DataType 'Int64)))) (OptionalType "
                                   "(OptionalType (DataType 'Int64))))", ctx)), EError::Unsupported);
    Error(session.Evaluate(Compile("(RangeMultiply (Uint64 '0) (RangeFor '== (Int64 '1) (DataType 'Int64)))", ctx)),
        EError::Unsupported);
    Error(session.Evaluate(Compile("(RangeUnion (RangeFinalize (RangeFor '== (Int64 '1) (DataType 'Int64))))", ctx)),
        EError::Unsupported);
}

Y_UNIT_TEST(BinaryNumericLiteralsAreRefused) {
    TExprContext ctx;
    TSession session;
    const TPositionHandle pos{};
    // Byte 0x31 is Uint8(49), but parsing it as decimal text yields Uint8(1).
    const auto atom = ctx.NewAtom(pos, "1", TNodeFlags::BinaryContent);
    const auto number = ctx.NewCallable(pos, "Uint8", {atom});
    Error(session.Evaluate(number), EError::Unsupported);
    Error(session.Evaluate(ctx.NewCallable(pos, "RangeFor", {ctx.NewAtom(pos, "=="), number,
        ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "Uint8")})})), EError::Unsupported);

    // Binary flags are meaningful for numbers; string literals still own bytes.
    const auto text = Value(session, ctx.NewCallable(pos, "String", {atom}));
    UNIT_ASSERT_VALUES_EQUAL(text->GetScalar()->Text(), "1");
    UNIT_ASSERT_VALUES_EQUAL(Value(session, Compile("(Uint8 '49)", ctx))->GetScalar()->Data.Get<ui8>(), 49);
}

Y_UNIT_TEST(IntervalLiteralSemanticBounds) {
    const std::pair<TStringBuf, i64> types[] = {
        {"Interval", i64(NUdf::MAX_TIMESTAMP) - 1},
        {"Interval64", NUdf::MAX_INTERVAL64},
    };
    for (const auto& [type, maximum] : types) {
        TExprContext ctx;
        TSession session;
        const auto literal = [&](i64 value) {
            return Compile(TStringBuilder() << '(' << type << " '" << value << ')', ctx);
        };

        for (i64 value : {-maximum, i64(0), maximum}) {
            UNIT_ASSERT_VALUES_EQUAL(Value(session, literal(value))->GetScalar()->Data.Get<i64>(), value);
        }
        for (i64 value : {Min<i64>(), -maximum - 1, maximum + 1, Max<i64>()}) {
            Error(session.Evaluate(literal(value)), EError::InvalidInput);
        }
    }
}

Y_UNIT_TEST(EmptyBoundaryCastsPreserveOptionalDepth) {
    const TString optional = "(OptionalType (DataType 'Int64))";
    const TString nested = "(OptionalType " + optional + ')';
    for (TStringBuf cast : {"SafeCast", "StrictCast"}) {
        for (const auto& source : {optional, nested}) {
            for (const auto& target : {optional, nested}) {
                TExprContext ctx;
                TSession session;
                const bool finiteNull = source == optional && target == nested;
                const TString value = TStringBuilder() << '(' << cast << " (Nothing " << source << ") " << target << ')';
                const TString bound = TStringBuilder() << "'(" << value << " (Int32 '" << int(finiteNull) << "))";
                const auto expression = Compile("(RangeFinalize (AsRange '(" + bound + ' ' + bound + ")))", ctx);
                const auto result = Value(session, expression);
                const auto* ranges = result->GetRanges();
                UNIT_ASSERT_VALUES_EQUAL(ranges->Items.size(), 1);
                const auto& range = ranges->Items.front();
                UNIT_ASSERT_VALUES_EQUAL(range.Left.Components.front().Inf, finiteNull ? 0 : -1);
                UNIT_ASSERT_VALUES_EQUAL(range.Right.Components.front().Inf, finiteNull ? 0 : 1);
                if (finiteNull) {
                    UNIT_ASSERT(range.Left.Components.front().Value.Null);
                    UNIT_ASSERT(range.Right.Components.front().Value.Null);
                }
            }
        }
    }

    TExprContext ctx;
    TSession session;
    const auto range = [&](TStringBuf value) {
        const TString bound = TStringBuilder() << "'(" << value << " (Int32 '0))";
        return Compile("(AsRange '(" + bound + ' ' + bound + "))", ctx);
    };
    Error(session.Evaluate(range("(SafeCast (Nothing) " + nested + ')')), EError::InvalidInput);
    Error(session.Evaluate(range("(SafeCast (Nothing " + optional + ") (OptionalType (OptionalType (DataType 'String))))")),
        EError::Unsupported);
    Error(session.Evaluate(range("(StrictCast (Nothing " + nested + ") (OptionalType " + nested + "))")),
        EError::Unsupported);
}

Y_UNIT_TEST(SessionSetupIsDeferredToEvaluation) {
    static_assert(std::is_nothrow_constructible_v<TSession, TLimits>);
    TExprContext ctx;
    const auto expression = Compile("(Int64 '42)", ctx);
    TSession session;
    UNIT_ASSERT_VALUES_EQUAL(session.GetUsage().Work, 0);
    UNIT_ASSERT_VALUES_EQUAL(session.GetUsage().Bytes, 0);
    UNIT_ASSERT_VALUES_EQUAL(session.GetUsage().Memory, 0);
    UNIT_ASSERT_VALUES_EQUAL(Value(session, expression)->GetScalar()->Data.Get<i64>(), 42);
    UNIT_ASSERT(session.GetUsage().Memory > 0);

    TSession limited({.Memory = 0});
    Error(limited.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
    Error(limited.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
    UNIT_ASSERT_VALUES_EQUAL(limited.GetUsage().Memory, 0);
}

Y_UNIT_TEST(IncrementalBudgetAndUntakenBranch) {
    TExprContext ctx;
    auto expression = Compile("(If (Bool 'true) (Int64 '42) " + Points(1000) + ')', ctx);
    TSession session({.Work = 100});
    UNIT_ASSERT_VALUES_EQUAL(Value(session, expression)->GetScalar()->Data.Get<i64>(), 42);
    auto bad = Compile(Points(1000), ctx);
    Error(session.Evaluate(bad), EError::BudgetExceeded, EResource::Work);
    UNIT_ASSERT(session.GetUsage().Work <= 100);
    Error(session.Evaluate(bad), EError::BudgetExceeded, EResource::Work);
}

Y_UNIT_TEST(DifferentialPrefixSuccessors) {
    const TVector<TString> strings = {"", "abc", "\x7f", "\xdf\xbf", "\xed\x9f\xbf", "\xee\x80\x80", "\xef\xbf\xbf",
        "\xf4\x8f\xbf\xbf", "a\xf4\x8f\xbf\xbf\xf4\x8f\xbf\xbf", TString(32700, 'x')};
    for (const auto& text : strings) {
        for (TStringBuf type : {"String", "Utf8"}) {
            TExprContext ctx;
            const TPositionHandle pos{};
            auto literal = ctx.NewCallable(pos, type, {ctx.NewAtom(pos, text)});
            auto expression = ctx.NewCallable(pos, "RangeFor",
                {ctx.NewAtom(pos, "StartsWith"), literal, ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, type)})});
            const std::string_view input(text.data(), text.size());
            const auto expected = type == "String" ? NYql::NextLexicographicString(input) : NYql::NextValidUtf8(input);
            TSession session;
            const auto result = Value(session, expression);
            const auto* ranges = result->GetRanges();
            UNIT_ASSERT_VALUES_EQUAL(ranges->Items.size(), 1);
            const auto& upper = ranges->Items.front().Right.Components.front();
            if (expected) {
                UNIT_ASSERT_VALUES_EQUAL(upper.Inf, 0);
                UNIT_ASSERT_VALUES_EQUAL(upper.Value.Text(), TString(*expected));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(upper.Inf, 1);
            }
        }
    }
    TExprContext ctx;
    TSession session;
    const TPositionHandle pos{};
    auto invalid = ctx.NewCallable(pos, "Utf8", {ctx.NewAtom(pos, "\xed\xa0\x80")});   // surrogate
    Error(session.Evaluate(invalid), EError::InvalidInput);
}

Y_UNIT_TEST(ResourceGuards) {
    TExprContext ctx;
    auto expression = Compile("(RangeFinalize (RangeFor '== (Int64 '42) (DataType 'Int64)))", ctx);
    TSession measured;
    Value(measured, expression);
    const auto work = measured.GetUsage().Work;
    TSession exact({.Work = work});
    Value(exact, expression);
    TSession shortByOne({.Work = work - 1});
    Error(shortByOne.Evaluate(expression), EError::BudgetExceeded, EResource::Work);
    TSession exactBytes({.Bytes = measured.GetUsage().Bytes});
    Value(exactBytes, expression);
    TSession shortBytes({.Bytes = measured.GetUsage().Bytes - 1});
    Error(shortBytes.Evaluate(expression), EError::BudgetExceeded, EResource::Bytes);
    TSession memory({.Memory = 0});
    Error(memory.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
    TSession exactMemory({.Memory = measured.GetUsage().Memory});
    Value(exactMemory, expression);
    TSession shortMemory({.Memory = measured.GetUsage().Memory - 1});
    Error(shortMemory.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
    TSession depth({.Depth = 0});
    Error(depth.Evaluate(expression), EError::BudgetExceeded, EResource::Depth);
    TSession ranges({.Ranges = 1});
    Error(ranges.Evaluate(Compile(Points(2), ctx)), EError::BudgetExceeded, EResource::Ranges);
    TSession width({.Width = 0});
    Error(width.Evaluate(expression), EError::BudgetExceeded, EResource::Width);
    TSession bytes({.LiteralBytes = 2});
    Error(bytes.Evaluate(Compile("(String 'abc)", ctx)), EError::BudgetExceeded, EResource::LiteralBytes);
}

Y_UNIT_TEST(AllocationFailuresAndEscapingCompositeValues) {
    TExprContext ctx;
    TVector<TInputRange> input;
    // Heap-backed keys, reversed ordering, sort scratch, copied operands,
    // and cached values all allocate through the same allowance.
    for (i64 i = 12; i >= 0; --i) {
        TBound bound{{i, i + 1, i + 2, i + 3}, true};
        input.push_back({bound, bound});
    }
    const auto expression =
        Compile("(RangeFinalize (RangeUnion " + RangeText(input) + ' ' + RangeText(input) + "))", ctx);
    TSession measured;
    const auto expected = Value(measured, expression);
    const size_t memory = measured.GetUsage().Memory;
    UNIT_ASSERT(memory > 4096);
    for (size_t limit = 0; limit < memory; limit += 257) {
        TSession session({.Memory = limit});
        Error(session.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
        UNIT_ASSERT(session.GetUsage().Memory <= limit);
        const auto before = session.GetUsage().Memory;
        Error(session.Evaluate(expression), EError::BudgetExceeded, EResource::Memory);
        UNIT_ASSERT(session.GetUsage().Memory >= before && session.GetUsage().Memory <= limit);
    }
    TValuePtr saved;
    {
        TExprContext localContext;
        TSession session;
        saved = Value(session, Compile("(RangeFinalize " + RangeText(input) + ')', localContext));
    }
    UNIT_ASSERT_VALUES_EQUAL(saved->GetRanges()->Items.size(), expected->GetRanges()->Items.size());
    UNIT_ASSERT_VALUES_EQUAL(saved->GetRanges()->Items.back().Right.Components[3].Value.Data.Get<i64>(), 15);
}

Y_UNIT_TEST(ShapeAndByteGuardrails) {
    TExprContext ctx;
    const TPositionHandle pos{};
    for (size_t size : {64 * 1024, 64 * 1024 + 1}) {
        auto text = ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, TString(size, '\xff'))});
        auto prefix = ctx.NewCallable(pos, "RangeFor",
            {ctx.NewAtom(pos, "StartsWith"), text, ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "String")})});
        TSession session;
        if (size == 64 * 1024) {
            const auto value = Value(session, prefix);
            UNIT_ASSERT_VALUES_EQUAL(value->GetRanges()->Items.front().Right.Components.front().Inf, 1);
        } else {
            Error(session.Evaluate(prefix), EError::BudgetExceeded, EResource::LiteralBytes);
        }
    }
    auto longPoint = ctx.NewCallable(pos, "RangeFor",
        {ctx.NewAtom(pos, "=="), ctx.NewCallable(pos, "String", {ctx.NewAtom(pos, TString(8192, 'x'))}),
            ctx.NewCallable(pos, "DataType", {ctx.NewAtom(pos, "String")})});
    // Shared operands avoid repeated literal allocation. Comparing equal
    // long keys must still consume the byte allowance.
    TExprNode::TListType children(32, longPoint);
    auto comparisons = ctx.NewCallable(pos, "RangeIntersect", std::move(children));
    TSession bytes({.Bytes = 64 * 1024});
    Error(bytes.Evaluate(comparisons), EError::BudgetExceeded, EResource::Bytes);
    UNIT_ASSERT(bytes.GetUsage().Bytes <= 64 * 1024);
    for (size_t width : {64, 65}) {
        TBound bound;
        bound.Values.resize(width, 1);
        bound.Included = true;
        TSession session;
        const auto expression = Compile(RangeText({{bound, bound}}), ctx);
        if (width == 64) {
            UNIT_ASSERT_VALUES_EQUAL(Value(session, expression)->GetRanges()->Width, 64);
        } else {
            Error(session.Evaluate(expression), EError::BudgetExceeded, EResource::Width);
        }
    }
    for (size_t count : {16, 17}) {
        auto expression = Compile("(RangeMultiply (Uint64 '1024) " + Points(count) + ' ' + Points(16) + ')', ctx);
        TSession session;
        if (count == 16) {
            UNIT_ASSERT_VALUES_EQUAL(Value(session, expression)->GetRanges()->Items.size(), 256);
        } else {
            Error(session.Evaluate(expression), EError::BudgetExceeded, EResource::Ranges);
        }
    }
}

Y_UNIT_TEST(EmptyValuesAndRepeatedFailedAttempts) {
    TExprContext ctx;
    for (TStringBuf op : {"RangeUnion", "RangeIntersect", "RangeFinalize"}) {
        TSession session;
        const auto expression = Compile(TStringBuilder() << '(' << op << " (RangeEmpty (DataType 'Int64)))", ctx);
        UNIT_ASSERT(Value(session, expression)->GetRanges()->Items.empty());
    }
    TSession session({.Work = 32});
    auto unsupported = Compile("(Unknown)", ctx);
    for (size_t i = 0; i < 32; ++i) {
        const auto result = session.Evaluate(unsupported);
        UNIT_ASSERT(result.GetError());
        UNIT_ASSERT(
            result.GetError()->Code == EError::Unsupported || result.GetError()->Code == EError::BudgetExceeded);
    }
    Error(session.Evaluate(unsupported), EError::BudgetExceeded, EResource::Work);
    UNIT_ASSERT_VALUES_EQUAL(session.GetUsage().Work, 32);
}

Y_UNIT_TEST(SharedDagAndSessionBudget) {
    TExprContext ctx;
    auto expression = Compile(Points(20), ctx);
    TSession session;
    const auto first = Value(session, expression);
    const auto usage = session.GetUsage();
    const auto second = Value(session, expression);
    UNIT_ASSERT(first == second);
    UNIT_ASSERT_VALUES_EQUAL(usage.Memory, session.GetUsage().Memory);
    UNIT_ASSERT_VALUES_EQUAL(session.GetUsage().Work - usage.Work, 1);
    auto dag = expression;
    const auto condition = Compile("(Bool 'true)", ctx);
    for (size_t i = 0; i < 30; ++i) {
        dag = ctx.NewCallable(dag->Pos(), "If", {condition, dag, dag});
    }
    Value(session, dag);   // Exponential tree if shared nodes/branches are expanded eagerly.
}

Y_UNIT_TEST(AdversarialBudgetsAndCache) {
    const size_t repetitions = GetEnv("KQP_EVAL_BENCH").empty() ? 1 : 500;
    for (size_t count : {20, 255, 256, 257, 1024}) {
        TExprContext ctx;
        auto expression = Compile(Points(count), ctx);
        TUsage usage;
        const auto start = TInstant::Now();
        for (size_t i = 0; i < repetitions; ++i) {
            TSession session;
            auto result = session.Evaluate(expression);
            if (count <= 256) {
                UNIT_ASSERT(result.GetValue());
            } else {
                Error(result, EError::BudgetExceeded, EResource::Ranges);
            }
            usage = session.GetUsage();
            UNIT_ASSERT(
                usage.Work <= TLimits{}.Work && usage.Bytes <= TLimits{}.Bytes && usage.Memory <= TLimits{}.Memory);
        }
        if (repetitions > 1) {
            Cerr << "EVAL points=" << count
                 << " cold_us=" << double((TInstant::Now() - start).MicroSeconds()) / repetitions
                 << " work=" << usage.Work << " bytes=" << usage.Bytes << " memory=" << usage.Memory << Endl;
        }
        if (count <= 256) {
            TSession session;
            Value(session, expression);
            const auto hotStart = TInstant::Now();
            for (size_t i = 0; i < repetitions; ++i) {
                Value(session, expression);
            }
            if (repetitions > 1) {
                Cerr << "EVAL points=" << count
                     << " cached_us=" << double((TInstant::Now() - hotStart).MicroSeconds()) / repetitions << Endl;
            }
        }
    }
}

Y_UNIT_TEST(OperatorHeavyInputsStopOnWork) {
    const size_t repetitions = GetEnv("KQP_EVAL_BENCH").empty() ? 1 : 100;
    for (size_t count : {20, 256, 1024, 4096}) {
        TExprContext ctx;
        TStringBuilder text;
        text << "(RangeIntersect";
        for (size_t i = 0; i < count; ++i) {
            text << " (RangeFor '> (Int64 '" << i << ") (DataType 'Int64))";
        }
        text << ')';
        auto expression = Compile(text, ctx);
        TUsage usage;
        bool refused = false;
        const auto start = TInstant::Now();
        for (size_t i = 0; i < repetitions; ++i) {
            TSession session;
            const auto result = session.Evaluate(expression);
            refused = bool(result.GetError());
            if (refused) {
                Error(result, EError::BudgetExceeded, EResource::Work);
            } else {
                UNIT_ASSERT_VALUES_EQUAL((*result.GetValue())->GetRanges()->Items.size(), 1);
            }
            usage = session.GetUsage();
            UNIT_ASSERT(
                usage.Work <= TLimits{}.Work && usage.Bytes <= TLimits{}.Bytes && usage.Memory <= TLimits{}.Memory);
        }
        if (count == 20) {
            UNIT_ASSERT(!refused);
        }
        if (count == 4096) {
            UNIT_ASSERT(refused);
        }
        if (repetitions > 1) {
            Cerr << "EVAL intersections=" << count
                 << " cold_us=" << double((TInstant::Now() - start).MicroSeconds()) / repetitions
                 << " refused=" << refused << " work=" << usage.Work << " bytes=" << usage.Bytes << Endl;
        }
    }
}

Y_UNIT_TEST(ConstructorPreservesOrderAndDuplicates) {
    TExprContext ctx;
    TSession session;
    const TVector<TInputRange> input = {
        {{{2}, true}, {{3}, true}}, {{{1}, true}, {{2}, true}}, {{{1}, true}, {{2}, true}}};
    auto literal = Value(session, Compile(RangeText(input), ctx));
    UNIT_ASSERT_VALUES_EQUAL(literal->GetRanges()->Items.size(), 3);
    UNIT_ASSERT_VALUES_EQUAL(literal->GetRanges()->Items[0].Left.Components[0].Value.Data.Get<i64>(), 2);
    auto finalized = Value(session, Compile("(RangeFinalize " + RangeText(input) + ')', ctx));
    UNIT_ASSERT_VALUES_EQUAL(finalized->GetRanges()->Items.size(), 2);   // No union merge here.
}

Y_UNIT_TEST(DifferentialRangeAlgebra) {
    ui64 state = 0x3b7ad;
    const auto next = [&]() {
        state = state * 6364136223846793005ULL + 1;
        return state >> 32;
    };
    for (size_t trial = 0; trial < 50; ++trial) {
        const size_t width = 1 + trial % 4;
        const auto input = [&]() {
            TVector<TInputRange> ranges;
            for (size_t r = 0; r < 1 + trial % 7; ++r) {
                TInputRange range;
                for (auto* bound : {&range.Left, &range.Right}) {
                    const auto finite = next() % (width + 1);
                    for (size_t c = 0; c < width; ++c) {
                        bound->Values.push_back(c < finite ? std::optional<i64>(i64(next() % 11) - 5) : std::nullopt);
                    }
                    bound->Included = next() % 2;
                }
                ranges.push_back(range);
            }
            return ranges;
        };
        auto a = input();
        auto b = input();
        for (TStringBuf op : {"RangeUnion", "RangeIntersect", "RangeMultiply"}) {
            CompareRuntime(a, b, op, false);
            CompareRuntime(a, b, op, true);
        }
    }
}

Y_UNIT_TEST(DifferentialPointsAndTouchingBounds) {
    const TVector<TInputRange> a = {
        {{{1}, true}, {{1}, true}}, {{{3}, true}, {{4}, false}}, {{{1}, true}, {{1}, true}}};
    const TVector<TInputRange> b = {
        {{{1}, false}, {{3}, false}}, {{{3}, true}, {{7}, true}}, {{{0}, false}, {{1}, true}}};
    for (TStringBuf op : {"RangeUnion", "RangeIntersect", "RangeMultiply"}) {
        CompareRuntime(a, b, op, false);
        CompareRuntime(a, b, op, true);
    }
}
}   // Y_UNIT_TEST_SUITE(KqpBoundedEvaluation)
}   // namespace NKikimr::NKqp::NEval
