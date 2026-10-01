#include "mkql_computation_node_ut.h"
#include "mkql_block_test_helper.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/utils/strong_alias.h>

#include <util/string/builder.h>

#include <concepts>
#include <functional>
#include <variant>

namespace NKikimr::NMiniKQL {

namespace {

class TDecimalType {
public:
    explicit TDecimalType(ui8 precision, ui8 scale)
        : Precision_(precision)
        , Scale_(scale)
    {
    }

    ui8 GetPrecision() const {
        return Precision_;
    }
    ui8 GetScale() const {
        return Scale_;
    }

private:
    const ui8 Precision_;
    const ui8 Scale_;
};

class TDecimal {
public:
    explicit TDecimal(TDecimalType type, TStringBuf decimalValue)
        : Type_(type)
        , Value_(NYql::NDecimal::FromString(decimalValue, type.GetPrecision(), type.GetScale()))
        , IsOptional_(false)
    {
        MKQL_ENSURE(!NYql::NDecimal::IsError(Value_), "Invalid decimal input " << decimalValue);
    }

    TDecimal AsOptional() const {
        return TDecimal(Type_, Value_, /*isOptional=*/true);
    }

    const TDecimalType& GetType() const {
        return Type_;
    }

    NYql::NDecimal::TInt128 GetValue() const {
        return Value_;
    }

    bool IsOptional() const {
        return IsOptional_;
    }

private:
    TDecimal(TDecimalType type, NYql::NDecimal::TInt128 value, bool isOptional)
        : Type_(type)
        , Value_(value)
        , IsOptional_(isOptional)
    {
    }

    const TDecimalType Type_;
    const NYql::NDecimal::TInt128 Value_;
    const bool IsOptional_;
};

using TNullDecimal = NYql::TStrongAlias<class TNullDecimalTag, TDecimalType>;
using TPositiveInf = NYql::TStrongAlias<class TPositiveInfTag, TDecimalType>;
using TNegativeInf = NYql::TStrongAlias<class TNegativeInfTag, TDecimalType>;
using TNaN = NYql::TStrongAlias<class TNaNTag, TDecimalType>;
using TBinaryInputValue = std::variant<
    TDecimal,
    TNullDecimal,
    TPositiveInf,
    TNegativeInf,
    TNaN,
    i8,
    ui8,
    i16,
    ui16,
    i32,
    ui32,
    i64,
    ui64,
    TMaybe<i8>,
    TMaybe<ui8>,
    TMaybe<i16>,
    TMaybe<ui16>,
    TMaybe<i32>,
    TMaybe<ui32>,
    TMaybe<i64>,
    TMaybe<ui64>>;

template <typename TResult>
struct TDecimalBinaryCase {
    TBinaryInputValue Left;
    TBinaryInputValue Right;
    TStringBuf Operation;
    TMaybe<TResult> Expected;
};

using TDecimalArithmeticCase = TDecimalBinaryCase<TDecimal>;

template <typename TExpected>
struct TBinaryExpectedAdapter {
    using TComparable = TExpected;

    static TComparable Convert(const TExpected& expected) {
        return expected;
    }
};

template <>
struct TBinaryExpectedAdapter<TDecimal> {
    using TComparable = NYql::NDecimal::TInt128;

    static TComparable Convert(const TDecimal& expected) {
        return expected.GetValue();
    }
};

template <typename TExpected>
TMaybe<typename TBinaryExpectedAdapter<TExpected>::TComparable> ConvertBinaryExpected(
    const TMaybe<TExpected>& expected)
{
    if (!expected) {
        return {};
    }
    return TBinaryExpectedAdapter<TExpected>::Convert(*expected);
}

class TBinaryInput {
public:
    explicit TBinaryInput(TRuntimeNode node, bool isOptional)
        : Node_(node)
        , IsOptional_(isOptional)
    {
    }

    const TRuntimeNode& GetNode() const {
        return Node_;
    }
    bool IsOptional() const {
        return IsOptional_;
    }

private:
    const TRuntimeNode Node_;
    const bool IsOptional_;
};

class TBinaryProgram {
public:
    explicit TBinaryProgram(TRuntimeNode node, bool isOptionalResult)
        : Node_(node)
        , IsOptionalResult_(isOptionalResult)
    {
    }

    const TRuntimeNode& GetNode() const {
        return Node_;
    }
    bool IsOptionalResult() const {
        return IsOptionalResult_;
    }

private:
    const TRuntimeNode Node_;
    const bool IsOptionalResult_;
};

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TDecimal& input) {
    const auto& type = input.GetType();
    auto node = builder.NewDecimalLiteral(input.GetValue(), type.GetPrecision(), type.GetScale());
    if (input.IsOptional()) {
        node = builder.NewOptional(node);
    }
    return TBinaryInput{node, input.IsOptional()};
}

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TNullDecimal& input) {
    const auto& type = input.Value();
    auto node = builder.NewDecimalLiteral(0, type.GetPrecision(), type.GetScale());
    node = builder.NewEmptyOptional(builder.NewOptionalType(node.GetStaticType()));
    return TBinaryInput{node, /*isOptional=*/true};
}

TBinaryInput BuildSpecialDecimalInput(
    TProgramBuilder& builder,
    const TDecimalType& type,
    NYql::NDecimal::TInt128 value)
{
    return TBinaryInput{
        builder.NewDecimalLiteral(value, type.GetPrecision(), type.GetScale()), /*isOptional=*/false};
}

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TPositiveInf& input) {
    return BuildSpecialDecimalInput(builder, input.Value(), NYql::NDecimal::Inf());
}

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TNegativeInf& input) {
    return BuildSpecialDecimalInput(builder, input.Value(), -NYql::NDecimal::Inf());
}

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TNaN& input) {
    return BuildSpecialDecimalInput(builder, input.Value(), NYql::NDecimal::Nan());
}

template <std::integral T>
TBinaryInput BuildBinaryInput(TProgramBuilder& builder, T input) {
    return TBinaryInput{NTest::ConvertValueToLiteralNode(builder, input), /*isOptional=*/false};
}

template <std::integral T>
TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TMaybe<T>& input) {
    return TBinaryInput{NTest::ConvertValueToLiteralNode(builder, input), /*isOptional=*/true};
}

TBinaryInput BuildBinaryInput(TProgramBuilder& builder, const TBinaryInputValue& input) {
    return std::visit(
        [&builder](const auto& value) { return BuildBinaryInput(builder, value); }, input);
}

TString DescribeBinaryInput(const TDecimal& input) {
    const auto& type = input.GetType();
    return NYql::NDecimal::ToString(input.GetValue(), type.GetPrecision(), type.GetScale());
}

TString DescribeBinaryInput(const TNullDecimal&) {
    return "null";
}

TString DescribeSpecialDecimal(const TDecimalType& type, NYql::NDecimal::TInt128 value) {
    return NYql::NDecimal::ToString(value, type.GetPrecision(), type.GetScale());
}

TString DescribeBinaryInput(const TPositiveInf& input) {
    return DescribeSpecialDecimal(input.Value(), NYql::NDecimal::Inf());
}

TString DescribeBinaryInput(const TNegativeInf& input) {
    return DescribeSpecialDecimal(input.Value(), -NYql::NDecimal::Inf());
}

TString DescribeBinaryInput(const TNaN& input) {
    return DescribeSpecialDecimal(input.Value(), NYql::NDecimal::Nan());
}

template <std::integral T>
TString DescribeBinaryInput(T input) {
    TStringBuilder description;
    description << NUdf::GetDataTypeInfo(NUdf::GetDataSlot(NUdf::TDataType<T>::Id)).Name
                << '(' << +input << ')';
    return description;
}

template <std::integral T>
TString DescribeBinaryInput(const TMaybe<T>& input) {
    return input ? DescribeBinaryInput(*input) : "null";
}

TString DescribeBinaryInput(const TBinaryInputValue& input) {
    return std::visit([](const auto& value) { return DescribeBinaryInput(value); }, input);
}

template <typename TResult>
TString DescribeBinaryOperation(const TDecimalBinaryCase<TResult>& testCase) {
    return DescribeBinaryInput(testCase.Left) + " " + testCase.Operation + " " +
           DescribeBinaryInput(testCase.Right);
}

TRuntimeNode BuildRegularComparison(
    TProgramBuilder& builder, TStringBuf operation, TRuntimeNode left, TRuntimeNode right)
{
    if (operation == "==") {
        return builder.Equals(left, right);
    }
    if (operation == "!=") {
        return builder.NotEquals(left, right);
    }
    if (operation == "<") {
        return builder.Less(left, right);
    }
    if (operation == "<=") {
        return builder.LessOrEqual(left, right);
    }
    if (operation == ">") {
        return builder.Greater(left, right);
    }
    if (operation == ">=") {
        return builder.GreaterOrEqual(left, right);
    }
    MKQL_ENSURE(false, "Unknown comparison operation " << operation);
}

TRuntimeNode BuildBinaryOperation(
    TProgramBuilder& builder, TStringBuf operation, TRuntimeNode left, TRuntimeNode right)
{
    if (operation == "+") {
        return builder.DecimalIntegralAdd(left, right);
    }
    if (operation == "-") {
        return builder.DecimalIntegralSub(left, right);
    }
    return BuildRegularComparison(builder, operation, left, right);
}

template <typename TResult>
TBinaryProgram BuildBinaryProgram(
    TProgramBuilder& builder, const TDecimalBinaryCase<TResult>& testCase)
{
    const auto left = BuildBinaryInput(builder, testCase.Left);
    const auto right = BuildBinaryInput(builder, testCase.Right);
    const auto node = BuildBinaryOperation(
        builder, testCase.Operation, left.GetNode(), right.GetNode());
    return TBinaryProgram{node, left.IsOptional() || right.IsOptional()};
}

using TBuildGraph = std::function<THolder<IComputationGraph>(TRuntimeNode)>;

template <typename TResult>
void AssertBinaryResults(
    IComputationGraph& graph,
    bool isOptional,
    const TVector<std::tuple<TString, TResult>>& expected,
    const TVector<std::tuple<TString, TMaybe<TResult>>>& optionalExpected)
{
    if (isOptional) {
        AssertUnboxedValueElementEqual(graph.GetValue(), optionalExpected);
    } else {
        AssertUnboxedValueElementEqual(graph.GetValue(), expected);
    }
}

template <typename TResult>
void RunBinaryCasesNonBlocks(
    TProgramBuilder& builder,
    const TVector<TDecimalBinaryCase<TResult>>& cases,
    const TBuildGraph& buildGraph)
{
    using TComparableResult = typename TBinaryExpectedAdapter<TResult>::TComparable;

    MKQL_ENSURE(!cases.empty(), "Binary operation cases must not be empty");
    TVector<TRuntimeNode> nodes;
    TVector<std::tuple<TString, TComparableResult>> expected;
    TVector<std::tuple<TString, TMaybe<TComparableResult>>> optionalExpected;
    TMaybe<bool> isOptional;
    for (const auto& testCase : cases) {
        const auto program = BuildBinaryProgram(builder, testCase);
        const TString description = DescribeBinaryOperation(testCase);
        const auto comparableExpected = ConvertBinaryExpected(testCase.Expected);
        MKQL_ENSURE(!isOptional || *isOptional == program.IsOptionalResult(),
                    "A test group must have one result type");
        isOptional = program.IsOptionalResult();
        nodes.push_back(builder.NewTuple({NTest::ConvertValueToLiteralNode(builder, description), program.GetNode()}));
        if (program.IsOptionalResult()) {
            optionalExpected.emplace_back(description, comparableExpected);
        } else {
            MKQL_ENSURE(comparableExpected, "Expected a non-null binary operation result");
            expected.emplace_back(description, *comparableExpected);
        }
    }
    const auto graph = buildGraph(builder.NewList(nodes.front().GetStaticType(), nodes));
    AssertBinaryResults(*graph, *isOptional, expected, optionalExpected);
}

template <bool UseLLVM, typename TResult>
void RunBinaryCases(const TVector<TDecimalBinaryCase<TResult>>& cases) {
    TSetup<UseLLVM> setup;
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

using TBlockLeftDecimal = NTest::TDecimalLiteral<3, 2>;
using TBlockRightDecimal = NTest::TDecimalLiteral<5, 4>;
using TBlockComparisonCase =
    std::tuple<TString, TBlockLeftDecimal, TBlockRightDecimal, bool>;

void RunComparisonCasesBlocks(
    TStringBuf callable, const TVector<TBlockComparisonCase>& cases)
{
    TVector<std::tuple<TString, TBlockLeftDecimal>> left;
    TVector<TBlockRightDecimal> right;
    TVector<std::tuple<TString, bool>> expected;
    left.reserve(cases.size());
    right.reserve(cases.size());
    expected.reserve(cases.size());
    for (const auto& [description, leftValue, rightValue, result] : cases) {
        left.emplace_back(description, leftValue);
        right.push_back(rightValue);
        expected.emplace_back(description, result);
    }

    TBlockHelper().TestKernelFuzzied(
        left, right, expected,
        [callable](TSetup<false>& setup, TRuntimeNode describedLeft, TRuntimeNode right) {
            auto& builder = *setup.PgmBuilder;
            const auto left = builder.BlockNth(describedLeft, 1U);
            const auto shape = AS_TYPE(TBlockType, left.GetStaticType())->GetShape();
            const auto resultType = builder.NewBlockType(
                builder.NewDataType(NUdf::TDataType<bool>::Id), shape);
            return builder.BlockAsTuple({
                builder.BlockNth(describedLeft, 0U),
                builder.BlockFunc(callable, resultType, {left, right}),
            });
        },
        /*iterations=*/1);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLDecimalTest) {
Y_UNIT_TEST_LLVM(TestNanvl) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<13, 5>{314159});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TMaybe<NTest::TDecimalLiteral<13, 5>>>{
                                                           {NTest::TDecimalLiteral<13, 5>{0}},
                                                           {NTest::TDecimalLiteral<13, 5>{NYql::NDecimal::Nan()}},
                                                           {NTest::TDecimalLiteral<13, 5>{+NYql::NDecimal::Inf()}},
                                                           {NTest::TDecimalLiteral<13, 5>{-NYql::NDecimal::Inf()}},
                                                           {},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Nanvl(item, data);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<NYql::NDecimal::TInt128>>{
                                                          TMaybe<NYql::NDecimal::TInt128>{NYql::NDecimal::TInt128(0)},
                                                          TMaybe<NYql::NDecimal::TInt128>{NYql::NDecimal::TInt128(314159)},
                                                          TMaybe<NYql::NDecimal::TInt128>{+NYql::NDecimal::Inf()},
                                                          TMaybe<NYql::NDecimal::TInt128>{-NYql::NDecimal::Inf()},
                                                          TMaybe<NYql::NDecimal::TInt128>{}});
}

Y_UNIT_TEST_LLVM(TestToIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 1>>{
                                                           {0},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                           {1270},
                                                           {-1280},
                                                           {2550},
                                                           {-2560},
                                                           {2560},
                                                           {-2570},
                                                           {327670},
                                                           {-327680},
                                                           {655350},
                                                           {-655360},
                                                           {21474836470},
                                                           {-21474836480},
                                                           {21474836480},
                                                           {-21474836490},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i8>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui8>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i16>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui16>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i32>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui32>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<i64>::Id, /*optional=*/true)),
                                                          pb.ToIntegral(item, pb.NewDataType(NUdf::TDataType<ui64>::Id, /*optional=*/true))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<TMaybe<i8>, TMaybe<ui8>, TMaybe<i16>, TMaybe<ui16>, TMaybe<i32>, TMaybe<ui32>, TMaybe<i64>, TMaybe<ui64>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          {i8(0), ui8(0), i16(0), ui16(0), i32(0), ui32(0), i64(0), ui64(0)},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}, {}, {}},
                                                          {i8(127), ui8(127), i16(127), ui16(127), i32(127), ui32(127), i64(127), ui64(127)},
                                                          {i8(-128), {}, i16(-128), {}, i32(-128), {}, i64(-128), {}},
                                                          {{}, ui8(255), i16(255), ui16(255), i32(255), ui32(255), i64(255), ui64(255)},
                                                          {{}, {}, i16(-256), {}, i32(-256), {}, i64(-256), {}},
                                                          {{}, {}, i16(256), ui16(256), i32(256), ui32(256), i64(256), ui64(256)},
                                                          {{}, {}, i16(-257), {}, i32(-257), {}, i64(-257), {}},
                                                          {{}, {}, i16(32767), ui16(32767), i32(32767), ui32(32767), i64(32767), ui64(32767)},
                                                          {{}, {}, i16(-32768), {}, i32(-32768), {}, i64(-32768), {}},
                                                          {{}, {}, {}, ui16(65535), i32(65535), ui32(65535), i64(65535), ui64(65535)},
                                                          {{}, {}, {}, {}, i32(-65536), {}, i64(-65536), {}},
                                                          {{}, {}, {}, {}, i32(2147483647), ui32(2147483647), i64(2147483647), ui64(2147483647)},
                                                          {{}, {}, {}, {}, i32(-2147483648), {}, i64(-2147483648), {}},
                                                          {{}, {}, {}, {}, {}, ui32(2147483648U), i64(2147483648), ui64(2147483648)},
                                                          {{}, {}, {}, {}, {}, {}, i64(-2147483649LL), {}},
                                                      });
}

Y_UNIT_TEST_LLVM(TestToFloat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 3>>{
                                                           {2123},
                                                           {233},
                                                           {0},
                                                           {-3277823},
                                                           {-1},
                                                           {7128},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, pb.NewDataType(NUdf::TDataType<float>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 2.123F);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.233F);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0F);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -3277.823F);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -0.001F);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 7.128F);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isnan(item.template Get<float>()));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isinf(item.template Get<float>()) && item.template Get<float>() > 0.0F);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isinf(item.template Get<float>()) && item.template Get<float>() < 0.0F);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestToDouble) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 5>>{
                                                           {2123},
                                                           {233},
                                                           {0},
                                                           {-3277823},
                                                           {-1},
                                                           {7128},
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(item, pb.NewDataType(NUdf::TDataType<double>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.02123);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.00233);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.0);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -32.77823);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, -0.00001);
    UNIT_ASSERT(iterator.Next(item));
    AssertUnboxedValueElementEqual(item, 0.07128);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isnan(item.template Get<double>()));
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isinf(item.template Get<double>()) && item.template Get<double>() > 0.0);
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(std::isinf(item.template Get<double>()) && item.template Get<double>() < 0.0);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestDiv) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 0>{2});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 0>>{
                                                           {2},
                                                           {23},
                                                           {-23},
                                                           {25},
                                                           {-25},
                                                           {1},
                                                           {-1},
                                                           {3},
                                                           {-3},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalDiv(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{1, 12, -12, 12, -12, 0, 0, 2, -2});
}

Y_UNIT_TEST_LLVM(TestDivInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<9, 3>{-238973});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i8>{i8(0), i8(-1), i8(-128), i8(3), i8(5), i8(-7), i8(13), i8(-19), i8(42)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalDiv(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          -NYql::NDecimal::Inf(), 238973, 1866, -79658, -47795, 34139, -18383, 12577, -5690});
}

Y_UNIT_TEST_LLVM(TestMod) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<5, 2>{-12323});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<5, 2>>{
                                                           {-12323},
                                                           {0},
                                                           {NYql::NDecimal::Inf()},
                                                           {-1},
                                                           {2},
                                                           {-3},
                                                           {NYql::NDecimal::Nan()},
                                                           {7},
                                                           {-10000},
                                                           {12329},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMod(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, NYql::NDecimal::Nan(), NYql::NDecimal::Nan(), 0, -1, -2, NYql::NDecimal::Nan(), -3, -2323, -12323});
}

Y_UNIT_TEST_LLVM(TestModInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<3, 2>{-743});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i16>{i16(0), i16(1), i16(-2), i16(3), i16(4), i16(-5), i16(8), i16(10), i16(-10)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMod(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          NYql::NDecimal::Nan(), -43, -143, -143, -343, -243, -743, -743, -743});
}

Y_UNIT_TEST_LLVM(TestMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 2>{333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {122},
                                                           {1223},
                                                           {-999},
                                                           {0},
                                                           {-3003003003LL},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          1109, -333, -400, 10, 256, 406, 4073, -3327, 0, -NYql::NDecimal::Inf()});
}

Y_UNIT_TEST_LLVM(TestMulUInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<7, 2>{-333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<ui16>{ui16(0), ui16(1), ui16(2), ui16(3), ui16(10), ui16(100), ui16(1000), ui16(10000), ui16(65535)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, -333, -666, -999, -3330, -33300, -333000, -3330000, -NYql::NDecimal::Inf()});
}

Y_UNIT_TEST_LLVM(TestMulTinyInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<32, 4>{3631400});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<i8>{i8(0), i8(1), i8(-1), i8(3), i8(-3), i8(100), i8(-100), i8(127), i8(-128)});

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(data0, item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          0, 3631400, -3631400, 10894200, -10894200, 363140000, -363140000, 461187800, -464819200});
}

Y_UNIT_TEST_LLVM(TestCastAndMulTinyInt) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, i8(1));
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<32, 4>>{
                                                           {3145926},
                                                           {-3145926},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.DecimalMul(item, data0), pb.DecimalMul(item, pb.ToDecimal(data0, 32, 4))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{3145926, 3145926},
                                                          TRow{-3145926, -3145926},
                                                      });
}

Y_UNIT_TEST_LLVM(TestLongintMul) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data0 = NTest::ConvertValueToLiteralNode(pb, NTest::TDecimalLiteral<10, 0>{333});
    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 0>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {NYql::NDecimal::Nan()},
                                                           {30030031},
                                                           {-30030031},
                                                           {0},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.DecimalMul(item, data0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          110889, -33300, -39960, 999, 25641, NYql::NDecimal::Nan(), NYql::NDecimal::Inf(), -NYql::NDecimal::Inf(), 0});
}

Y_UNIT_TEST_LLVM(TestScaleUp) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {333},
                                                           {-100},
                                                           {-120},
                                                           {3},
                                                           {77},
                                                           {122},
                                                           {1223},
                                                           {-999},
                                                           {0},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToDecimal(item, 12, 4);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          33300, -10000, -12000, 300, 7700, 12200, 122300, -99900, 0});
}

Y_UNIT_TEST_LLVM(TestScaleDown) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 2>>{
                                                           {-251},
                                                           {-250},
                                                           {-150},
                                                           {-51},
                                                           {50},
                                                           {50},
                                                           {51},
                                                           {150},
                                                           {250},
                                                           {251},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToDecimal(item, 8, 0);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          -3, -2, -2, -1, 0, 0, 1, 2, 2, 3});
}

Y_UNIT_TEST_LLVM(TestMinMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.Min(left, right), pb.Max(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, 314},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-213, -213},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-213, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestAggrMinMax) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.AggrMin(left, right), pb.AggrMax(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{314, NYql::NDecimal::Nan()},
                                                          TRow{-213, NYql::NDecimal::Nan()},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{314, NYql::NDecimal::Nan()},
                                                          TRow{314, +NYql::NDecimal::Inf()},
                                                          TRow{314, 314},
                                                          TRow{-213, 314},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-213, NYql::NDecimal::Nan()},
                                                          TRow{-213, +NYql::NDecimal::Inf()},
                                                          TRow{-213, 314},
                                                          TRow{-213, -213},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), 314},
                                                          TRow{-NYql::NDecimal::Inf(), -213},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestAddSub) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<13, 2>>{
                                                           {NYql::NDecimal::Nan()},
                                                           {+NYql::NDecimal::Inf()},
                                                           {314},
                                                           {-213},
                                                           {-NYql::NDecimal::Inf()},
                                                       });
    const auto pgmReturn = pb.FlatMap(list,
                                      [&](TRuntimeNode left) {
                                          return pb.Map(list,
                                                        [&](TRuntimeNode right) {
                                                            return pb.NewTuple({pb.Add(left, right), pb.Sub(left, right)});
                                                        });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Inf(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{628, 0},
                                                          TRow{101, 527},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{+NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{101, -527},
                                                          TRow{-426, 0},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                          TRow{NYql::NDecimal::Nan(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{-NYql::NDecimal::Inf(), NYql::NDecimal::Nan()},
                                                      });
}

Y_UNIT_TEST_LLVM(TestRunBinaryCasesAcceptsDecimalExpected) {
    const TDecimalType decimalType{3, 2};
    const TVector<TDecimalBinaryCase<TDecimal>> cases = {
        {.Left = TDecimal{decimalType, "1.23"},
         .Right = i8{5},
         .Operation = "+",
         .Expected = TDecimal{decimalType, "6.23"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddInt8Boundaries) {
    const TDecimalType decimalType{3, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = i8{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TDecimal{decimalType, "1.23"}, .Right = i8{5}, .Operation = "+", .Expected = TDecimal{decimalType, "6.23"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{-1}, .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{10}, .Operation = "+", .Expected = TDecimal{decimalType, "0.01"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{-10}, .Operation = "+", .Expected = TDecimal{decimalType, "-0.01"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i8>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i8>(), .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{9}, .Operation = "+", .Expected = TDecimal{decimalType, "-0.99"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{-9}, .Operation = "+", .Expected = TDecimal{decimalType, "0.99"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubInt8Boundaries) {
    const TDecimalType decimalType{3, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = i8{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TDecimal{decimalType, "1.23"}, .Right = i8{5}, .Operation = "-", .Expected = TDecimal{decimalType, "-3.77"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{1}, .Operation = "-", .Expected = TDecimal{decimalType, "8.99"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{-1}, .Operation = "-", .Expected = TDecimal{decimalType, "-8.99"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{10}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{-10}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i8>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i8>(), .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{9}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{-9}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "9.99"}, .Right = i8{10}, .Operation = "-", .Expected = TDecimal{decimalType, "-0.01"}},
        {.Left = TDecimal{decimalType, "-9.99"}, .Right = i8{-10}, .Operation = "-", .Expected = TDecimal{decimalType, "0.01"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddInt64Boundaries) {
    const TDecimalType decimalType{19, 0};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "9223372036854775807"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-9223372036854775808"}},
        {.Left = TDecimal{decimalType, "1"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "9223372036854775808"}},
        {.Left = TDecimal{decimalType, "-1"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-9223372036854775809"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubInt64Boundaries) {
    const TDecimalType decimalType{19, 0};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-9223372036854775807"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "9223372036854775808"}},
        {.Left = TDecimal{decimalType, "1"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-9223372036854775806"}},
        {.Left = TDecimal{decimalType, "-1"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "9223372036854775807"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddChecksFinalResult) {
    const TDecimalType decimalType{18, 0};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "-999999999999999999"}, .Right = i64{1000000000000000000LL}, .Operation = "+", .Expected = TDecimal{decimalType, "1"}},
        {.Left = TDecimal{decimalType, "999999999999999999"}, .Right = i64{-1000000000000000000LL}, .Operation = "+", .Expected = TDecimal{decimalType, "-1"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubChecksFinalResult) {
    const TDecimalType decimalType{18, 0};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "999999999999999999"}, .Right = i64{1000000000000000000LL}, .Operation = "-", .Expected = TDecimal{decimalType, "-1"}},
        {.Left = TDecimal{decimalType, "-999999999999999999"}, .Right = i64{-1000000000000000000LL}, .Operation = "-", .Expected = TDecimal{decimalType, "1"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddMaximumPrecisionScaling) {
    const TDecimalType decimalType{35, 16};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.0000000000000001"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "9223372036854775807.0000000000000001"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-9223372036854775808"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubMaximumPrecisionScaling) {
    const TDecimalType decimalType{35, 16};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.0000000000000001"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-9223372036854775806.9999999999999999"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "9223372036854775808"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddMaximumScaleZero) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.5"}, .Right = i64{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0.5"}},
        {.Left = TDecimal{decimalType, "-0.5"}, .Right = i64{0}, .Operation = "+", .Expected = TDecimal{decimalType, "-0.5"}},
        {.Left = TPositiveInf{decimalType}, .Right = i64{0}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNaN{decimalType}, .Right = i64{0}, .Operation = "+", .Expected = TDecimal{decimalType, "nan"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubMaximumScaleZero) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.5"}, .Right = i64{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0.5"}},
        {.Left = TDecimal{decimalType, "-0.5"}, .Right = i64{0}, .Operation = "-", .Expected = TDecimal{decimalType, "-0.5"}},
        {.Left = TPositiveInf{decimalType}, .Right = i64{0}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNaN{decimalType}, .Right = i64{0}, .Operation = "-", .Expected = TDecimal{decimalType, "nan"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddMaximumScaleUint64) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.5"}, .Right = ui64{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0.5"}},
        {.Left = TDecimal{decimalType, "0.5"}, .Right = ui64{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "-0.5"}, .Right = ui64{1}, .Operation = "+", .Expected = TDecimal{decimalType, "0.5"}},
        {.Left = TDecimal{decimalType, "0.5"}, .Right = Max<ui64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubMaximumScaleUint64) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.5"}, .Right = ui64{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0.5"}},
        {.Left = TDecimal{decimalType, "0.5"}, .Right = ui64{1}, .Operation = "-", .Expected = TDecimal{decimalType, "-0.5"}},
        {.Left = TDecimal{decimalType, "0.5"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddMaximumScaleOverflow) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{-1}, .Operation = "+", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{2}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{-2}, .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubMaximumScaleOverflow) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{1}, .Operation = "-", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{-1}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{2}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = i64{-2}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddMaximumScaleOptionalIntegral) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0.999999999999999999999999999"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{-1}, .Operation = "+", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{2}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{-2}, .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{}, .Operation = "+", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubMaximumScaleOptionalIntegral) {
    const TDecimalType decimalType{35, 35};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0.999999999999999999999999999"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{1}, .Operation = "-", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{2}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{-2}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = TMaybe<i64>{}, .Operation = "-", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddScaleBoundary) {
    const TDecimalType decimalType{35, 27};
    const i64 wholeBound = static_cast<i64>(NYql::NDecimal::GetDivider(8U));
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Max<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Min<i64>(), .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = wholeBound - 1, .Operation = "+", .Expected = TDecimal{decimalType, "99999999.999999999999999999999999999"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = -wholeBound + 1, .Operation = "+", .Expected = TDecimal{decimalType, "-99999998.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "-99999999.999999999999999999999999999"}, .Right = wholeBound, .Operation = "+", .Expected = TDecimal{decimalType, "0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "99999999.999999999999999999999999999"}, .Right = -wholeBound, .Operation = "+", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubScaleBoundary) {
    const TDecimalType decimalType{35, 27};
    const i64 wholeBound = static_cast<i64>(NYql::NDecimal::GetDivider(8U));
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Max<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = Min<i64>(), .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = wholeBound - 1, .Operation = "-", .Expected = TDecimal{decimalType, "-99999998.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "0.999999999999999999999999999"}, .Right = -wholeBound + 1, .Operation = "-", .Expected = TDecimal{decimalType, "99999999.999999999999999999999999999"}},
        {.Left = TDecimal{decimalType, "-99999999.999999999999999999999999999"}, .Right = wholeBound, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "99999999.999999999999999999999999999"}, .Right = -wholeBound, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "99999999.999999999999999999999999999"}, .Right = wholeBound, .Operation = "-", .Expected = TDecimal{decimalType, "-0.000000000000000000000000001"}},
        {.Left = TDecimal{decimalType, "-99999999.999999999999999999999999999"}, .Right = -wholeBound, .Operation = "-", .Expected = TDecimal{decimalType, "0.000000000000000000000000001"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddUint64InBounds) {
    const TDecimalType decimalType{35, 15};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<ui64>(), .Operation = "+", .Expected = TDecimal{decimalType, "18446744073709551615"}},
        {.Left = TDecimal{decimalType, "99999999999999999999.999999999999999"}, .Right = Max<ui64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubUint64InBounds) {
    const TDecimalType decimalType{35, 15};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-18446744073709551615"}},
        {.Left = TDecimal{decimalType, "99999999999999999999.999999999999999"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "81553255926290448384.999999999999999"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddUint64OutOfBounds) {
    const TDecimalType decimalType{35, 16};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<ui64>(), .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "-9999999999999999999.9999999999999999"}, .Right = Max<ui64>(), .Operation = "+", .Expected = TDecimal{decimalType, "8446744073709551615.0000000000000001"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubUint64OutOfBounds) {
    const TDecimalType decimalType{35, 16};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "-9999999999999999999.9999999999999999"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TDecimal{decimalType, "9999999999999999999.9999999999999999"}, .Right = Max<ui64>(), .Operation = "-", .Expected = TDecimal{decimalType, "-8446744073709551615.0000000000000001"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddOptionalDecimal) {
    const TDecimalType decimalType{5, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "12.34"}.AsOptional(), .Right = i16{5}, .Operation = "+", .Expected = TDecimal{decimalType, "17.34"}},
        {.Left = TDecimal{decimalType, "0"}.AsOptional(), .Right = i16{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TNullDecimal{decimalType}, .Right = i16{5}, .Operation = "+", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubOptionalDecimal) {
    const TDecimalType decimalType{5, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "12.34"}.AsOptional(), .Right = ui16{5}, .Operation = "-", .Expected = TDecimal{decimalType, "7.34"}},
        {.Left = TDecimal{decimalType, "0"}.AsOptional(), .Right = ui16{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TNullDecimal{decimalType}, .Right = ui16{5}, .Operation = "-", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddOptionalIntegral) {
    const TDecimalType decimalType{5, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "12.34"}, .Right = TMaybe<i32>{5}, .Operation = "+", .Expected = TDecimal{decimalType, "17.34"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = TMaybe<i32>{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TDecimal{decimalType, "12.34"}, .Right = TMaybe<i32>{}, .Operation = "+", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubOptionalIntegral) {
    const TDecimalType decimalType{5, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "12.34"}, .Right = TMaybe<ui32>{5}, .Operation = "-", .Expected = TDecimal{decimalType, "7.34"}},
        {.Left = TDecimal{decimalType, "0"}, .Right = TMaybe<ui32>{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TDecimal{decimalType, "12.34"}, .Right = TMaybe<ui32>{}, .Operation = "-", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddBothOptional) {
    const TDecimalType decimalType{3, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "9.99"}.AsOptional(), .Right = TMaybe<i8>{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TDecimal{decimalType, "0"}.AsOptional(), .Right = TMaybe<i8>{0}, .Operation = "+", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TNullDecimal{decimalType}, .Right = TMaybe<i8>{1}, .Operation = "+", .Expected = {}},
        {.Left = TDecimal{decimalType, "9.99"}.AsOptional(), .Right = TMaybe<i8>{}, .Operation = "+", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubBothOptional) {
    const TDecimalType decimalType{3, 2};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "9.99"}.AsOptional(), .Right = TMaybe<i8>{1}, .Operation = "-", .Expected = TDecimal{decimalType, "8.99"}},
        {.Left = TDecimal{decimalType, "0"}.AsOptional(), .Right = TMaybe<i8>{0}, .Operation = "-", .Expected = TDecimal{decimalType, "0"}},
        {.Left = TNullDecimal{decimalType}, .Right = TMaybe<i8>{1}, .Operation = "-", .Expected = {}},
        {.Left = TDecimal{decimalType, "9.99"}.AsOptional(), .Right = TMaybe<i8>{}, .Operation = "-", .Expected = {}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralAddSpecialValues) {
    const TDecimalType decimalType{1, 1};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = i8{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TPositiveInf{decimalType}, .Right = i8{1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNegativeInf{decimalType}, .Right = i8{1}, .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TNaN{decimalType}, .Right = i8{1}, .Operation = "+", .Expected = TDecimal{decimalType, "nan"}},
        {.Left = TPositiveInf{decimalType}, .Right = i8{-1}, .Operation = "+", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNegativeInf{decimalType}, .Right = i8{-1}, .Operation = "+", .Expected = TDecimal{decimalType, "-inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestDecimalIntegralSubSpecialValues) {
    const TDecimalType decimalType{1, 1};
    const TVector<TDecimalArithmeticCase> cases = {
        {.Left = TDecimal{decimalType, "0"}, .Right = i8{1}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TPositiveInf{decimalType}, .Right = i8{1}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNegativeInf{decimalType}, .Right = i8{1}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
        {.Left = TNaN{decimalType}, .Right = i8{1}, .Operation = "-", .Expected = TDecimal{decimalType, "nan"}},
        {.Left = TPositiveInf{decimalType}, .Right = i8{-1}, .Operation = "-", .Expected = TDecimal{decimalType, "inf"}},
        {.Left = TNegativeInf{decimalType}, .Right = i8{-1}, .Operation = "-", .Expected = TDecimal{decimalType, "-inf"}},
    };
    RunBinaryCases<LLVM>(cases);
}

Y_UNIT_TEST_LLVM(TestCompares) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<NTest::TDecimalLiteral<10, 0>, NTest::TDecimalLiteral<7, 2>>>{
                                                           {{-7}, {-700}},
                                                           {{-7}, {300}},
                                                           {{-7}, {NYql::NDecimal::Nan()}},
                                                           {{-7}, {-NYql::NDecimal::Inf()}},
                                                           {{-7}, {+NYql::NDecimal::Inf()}},

                                                           {{3}, {-700}},
                                                           {{3}, {300}},
                                                           {{3}, {NYql::NDecimal::Nan()}},
                                                           {{3}, {-NYql::NDecimal::Inf()}},
                                                           {{3}, {+NYql::NDecimal::Inf()}},

                                                           {{NYql::NDecimal::Nan()}, {-700}},
                                                           {{NYql::NDecimal::Nan()}, {300}},
                                                           {{NYql::NDecimal::Nan()}, {NYql::NDecimal::Nan()}},
                                                           {{NYql::NDecimal::Nan()}, {-NYql::NDecimal::Inf()}},
                                                           {{NYql::NDecimal::Nan()}, {+NYql::NDecimal::Inf()}},

                                                           {{-NYql::NDecimal::Inf()}, {-700}},
                                                           {{-NYql::NDecimal::Inf()}, {300}},
                                                           {{-NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{-NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{-NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},

                                                           {{+NYql::NDecimal::Inf()}, {-700}},
                                                           {{+NYql::NDecimal::Inf()}, {300}},
                                                           {{+NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{+NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{+NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<bool, bool, bool, bool, bool, bool>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                      });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonFiniteEquality) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "!=", .Expected = false},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "-1.23"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "==", .Expected = false},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 0}, "123"}, .Right = TDecimal{TDecimalType{7, 4}, "123.0"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 14}, "-749999999999999999999.99999999999999"},
         .Right = TDecimal{TDecimalType{35, 14}, "-749999999999999999999.99999999999999"},
         .Operation = "==",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonFiniteOrdering) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2299"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2299"}, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "<", .Expected = false},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "-1.2301"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "-1.2299"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 17}, "-499999999999999999.99999999999999999"},
         .Right = TDecimal{TDecimalType{35, 17}, "-499999999999999999.99999999999999998"},
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonScaleDirection) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Right = TDecimal{TDecimalType{3, 2}, "1.23"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2299"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 4}, "1.2299"}, .Right = TDecimal{TDecimalType{3, 2}, "1.23"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "-1.2301"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 4}, "-1.2301"}, .Right = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 14}, "249999999999999999999.99999999999999"},
         .Right = TDecimal{TDecimalType{35, 12}, "249999999999999999999.999999999999"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonPrecisionExtremes) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {
            .Left = TDecimal{TDecimalType{35, 35}, "0.00000000000000000000000000000000001"},
            .Right = TDecimal{TDecimalType{1, 1}, "0.1"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 1}, "0.1"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.00000000000000000000000000000000001"},
            .Operation = ">",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 35}, "0.1"}, .Right = TDecimal{TDecimalType{1, 1}, "0.1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 34}, "1.0"}, .Right = TDecimal{TDecimalType{1, 0}, "1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 2}, "100.0"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 2}, "999.99"}, .Right = TDecimal{TDecimalType{3, 2}, "1.0"}, .Operation = ">", .Expected = true},
        {
            .Left = TDecimal{TDecimalType{1, 0}, "9"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "-9"},
            .Right = TDecimal{TDecimalType{35, 35}, "-0.99999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 23}, "714285714285.71428571428571428571427"},
         .Right = TDecimal{TDecimalType{35, 23}, "714285714285.71428571428571428571426"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonEqualPrecisionAndScale) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{1, 0}, "1"}, .Right = TDecimal{TDecimalType{1, 0}, "2"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 0}, "9"}, .Right = TDecimal{TDecimalType{1, 0}, "-9"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 0}, "0"}, .Right = TDecimal{TDecimalType{1, 0}, "0"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 1}, "0.1"}, .Right = TDecimal{TDecimalType{1, 1}, "0.2"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 1}, "-0.9"}, .Right = TDecimal{TDecimalType{1, 1}, "-0.8"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{3, 2}, "1.24"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.23"}, .Right = TDecimal{TDecimalType{3, 2}, "-1.24"}, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 27}, "-99999999.999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 27}, "-99999999.999999999999999999999999999"},
         .Operation = "==",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonSameScaleDifferentPrecision) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{1, 0}, "1"}, .Right = TDecimal{TDecimalType{35, 0}, "1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 0}, "-1"}, .Right = TDecimal{TDecimalType{35, 0}, "-1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 1}, "1.2"}, .Right = TDecimal{TDecimalType{35, 1}, "1.2"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{35, 2}, "1.23"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "9.99"}, .Right = TDecimal{TDecimalType{5, 2}, "999.99"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-9.99"}, .Right = TDecimal{TDecimalType{5, 2}, "-999.99"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 30}, "74999.999999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 30}, "74999.999999999999999999999999999998"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonSamePrecisionDifferentScale) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{3, 2}, "1.2"}, .Right = TDecimal{TDecimalType{3, 1}, "1.2"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "-1.2"}, .Right = TDecimal{TDecimalType{3, 1}, "-1.2"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 3}, "12.3"}, .Right = TDecimal{TDecimalType{5, 1}, "12.3"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 3}, "-12.3"}, .Right = TDecimal{TDecimalType{5, 1}, "-12.3"}, .Operation = "!=", .Expected = false},
        {.Left = TDecimal{TDecimalType{35, 0}, "1"},
         .Right = TDecimal{TDecimalType{35, 34}, "1.0"},
         .Operation = "==",
         .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 0}, "99999999999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
         .Operation = ">",
         .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 33}, "49.999999999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 30}, "49.999999999999999999999999999999"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonPrecisionEqualsScale) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{1, 1}, "0.1"}, .Right = TDecimal{TDecimalType{2, 2}, "0.1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 2}, "0.1"}, .Right = TDecimal{TDecimalType{3, 3}, "0.1"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 1}, "-0.1"}, .Right = TDecimal{TDecimalType{2, 2}, "-0.1"}, .Operation = "!=", .Expected = false},
        {.Left = TDecimal{TDecimalType{1, 1}, "0.1"}, .Right = TDecimal{TDecimalType{35, 35}, "0.1"}, .Operation = "==", .Expected = true},
        {
            .Left = TDecimal{TDecimalType{35, 35}, "0.00000000000000000000000000000000001"},
            .Right = TDecimal{TDecimalType{1, 1}, "0.1"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{1, 1}, "0.9"},
            .Operation = ">",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{7, 7}, "0.2499999"}, .Right = TDecimal{TDecimalType{7, 7}, "0.2499998"}, .Operation = ">", .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonScaleGapCorners) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{35, 0}, "1"},
         .Right = TDecimal{TDecimalType{35, 34}, "1.0"},
         .Operation = "==",
         .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 0}, "99999999999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
         .Operation = ">",
         .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 0}, "-99999999999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 35}, "-0.99999999999999999999999999999999999"},
         .Operation = "<",
         .Expected = true},
        {
            .Left = TDecimal{TDecimalType{1, 0}, "1"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "-1"},
            .Right = TDecimal{TDecimalType{35, 35}, "-0.99999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 14}, "71.42857142857142"},
         .Right = TDecimal{TDecimalType{35, 33}, "71.428571428571428571428571428571427"},
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonMultiplicationLimits) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {
            .Left = TDecimal{TDecimalType{34, 0}, "9999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 1}, "9999999999999999999999999999999999.0"},
            .Operation = "==",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{34, 0}, "9999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 1}, "9999999999999999999999999999999999.9"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{35, 0}, "10000000000000000000000000000000000"},
            .Right = TDecimal{TDecimalType{35, 1}, "9999999999999999999999999999999999.9"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{35, 1}, "9999999999999999999999999999999999.9"},
            .Right = TDecimal{TDecimalType{35, 0}, "10000000000000000000000000000000000"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{35, 0}, "-10000000000000000000000000000000000"},
            .Right = TDecimal{TDecimalType{35, 1}, "-9999999999999999999999999999999999.9"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{18, 0}, "999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 17}, "999999999999999999.0"},
            .Operation = "==",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{19, 0}, "1000000000000000000"},
            .Right = TDecimal{TDecimalType{35, 17}, "999999999999999999.99999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "9"},
            .Right = TDecimal{TDecimalType{35, 34}, "9.0"},
            .Operation = "==",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "9"},
            .Right = TDecimal{TDecimalType{35, 34}, "9.9999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{2, 0}, "10"},
            .Right = TDecimal{TDecimalType{35, 34}, "9.9999999999999999999999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{2, 0}, "-10"},
            .Right = TDecimal{TDecimalType{35, 34}, "-9.9999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "0"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "1"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{1, 0}, "-1"},
            .Right = TDecimal{TDecimalType{35, 35}, "-0.99999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 17}, "-999999999999999999.99999999999999999"},
         .Right = TDecimal{TDecimalType{35, 0}, "-1000000000000000000"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonPositiveBounds) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {
            .Left = TDecimal{TDecimalType{35, 0}, "99999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"},
            .Operation = ">",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{34, 0}, "9999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 1}, "9999999999999999999999999999999999.0"},
            .Operation = "==",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 12}, "99999999999999999999999.999999999999"},
         .Right = TDecimal{TDecimalType{35, 12}, "99999999999999999999999.999999999998"},
         .Operation = ">",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonNegativeBounds) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {
            .Left = TDecimal{TDecimalType{35, 0}, "-99999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 35}, "-0.99999999999999999999999999999999999"},
            .Operation = "<",
            .Expected = true,
        },
        {
            .Left = TDecimal{TDecimalType{34, 0}, "-9999999999999999999999999999999999"},
            .Right = TDecimal{TDecimalType{35, 1}, "-9999999999999999999999999999999999.0"},
            .Operation = "==",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 23}, "-999999999999.99999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 23}, "-999999999999.99999999999999999999998"},
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonZeroAndSigns) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{2, 2}, "-0.0"}, .Right = TDecimal{TDecimalType{4, 4}, "0.0"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 2}, "-0.0"}, .Right = TDecimal{TDecimalType{4, 4}, "0.0"}, .Operation = "!=", .Expected = false},
        {.Left = TDecimal{TDecimalType{1, 0}, "0"}, .Right = TDecimal{TDecimalType{4, 4}, "0.0001"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 0}, "0"}, .Right = TDecimal{TDecimalType{4, 4}, "-0.0001"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 2}, "-0.01"}, .Right = TDecimal{TDecimalType{1, 0}, "0"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 2}, "0.01"}, .Right = TDecimal{TDecimalType{1, 0}, "0"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 27}, "-49999999.999999999999999999999999999"},
         .Right = TDecimal{TDecimalType{35, 27}, "49999999.999999999999999999999999999"},
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonSpecialValues) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = "==", .Expected = false},
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = "!=", .Expected = true},
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = "<", .Expected = false},
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = "<=", .Expected = false},
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = ">", .Expected = false},
        {.Left = TNaN{TDecimalType{35, 0}}, .Right = TNaN{TDecimalType{5, 2}}, .Operation = ">=", .Expected = false},
        {.Left = TPositiveInf{TDecimalType{35, 0}}, .Right = TDecimal{TDecimalType{5, 2}, "999.99"}, .Operation = ">", .Expected = true},
        {.Left = TNegativeInf{TDecimalType{35, 0}}, .Right = TDecimal{TDecimalType{5, 2}, "-999.99"}, .Operation = "<", .Expected = true},
        {.Left = TPositiveInf{TDecimalType{35, 0}}, .Right = TPositiveInf{TDecimalType{35, 0}}, .Operation = "==", .Expected = true},
        {.Left = TNegativeInf{TDecimalType{35, 0}}, .Right = TNegativeInf{TDecimalType{35, 0}}, .Operation = "==", .Expected = true},
        {.Left = TPositiveInf{TDecimalType{35, 0}}, .Right = TNegativeInf{TDecimalType{35, 0}}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 2}, "999.99"}, .Right = TPositiveInf{TDecimalType{35, 0}}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{5, 2}, "-999.99"}, .Right = TNegativeInf{TDecimalType{35, 0}}, .Operation = ">", .Expected = true},
        {.Left = TNegativeInf{TDecimalType{35, 30}},
         .Right = TDecimal{TDecimalType{35, 30}, "-74999.999999999999999999999999999999"},
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonOptionalValues) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}.AsOptional(), .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}.AsOptional(), .Right = TDecimal{TDecimalType{5, 4}, "1.2301"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}.AsOptional(), .Right = TDecimal{TDecimalType{5, 4}, "1.2299"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}.AsOptional(), .Operation = "<=", .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TDecimal{TDecimalType{5, 4}, "1.2299"}.AsOptional(), .Operation = "<=", .Expected = false},
        {
            .Left = TDecimal{TDecimalType{3, 2}, "-1.23"}.AsOptional(),
            .Right = TDecimal{TDecimalType{5, 4}, "-1.2301"}.AsOptional(),
            .Operation = ">=",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{1, 0}, "0"}.AsOptional(), .Right = TDecimal{TDecimalType{4, 4}, "0.0"}.AsOptional(), .Operation = "!=", .Expected = false},
        {.Left = TDecimal{TDecimalType{4, 4}, "0.0001"}.AsOptional(), .Right = TDecimal{TDecimalType{1, 0}, "0"}.AsOptional(), .Operation = ">", .Expected = true},
        {.Left = TDecimal{TDecimalType{35, 33}, "-24.999999999999999999999999999999999"}.AsOptional(),
         .Right = TDecimal{TDecimalType{35, 33}, "-24.999999999999999999999999999999998"}.AsOptional(),
         .Operation = "<",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonOptionalPrecisionScaleCorners) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TDecimal{TDecimalType{1, 0}, "1"}.AsOptional(), .Right = TDecimal{TDecimalType{35, 0}, "1"}.AsOptional(), .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{2, 1}, "1.2"}.AsOptional(), .Right = TDecimal{TDecimalType{35, 1}, "1.2"}.AsOptional(), .Operation = "==", .Expected = true},
        {.Left = TDecimal{TDecimalType{1, 1}, "0.1"}.AsOptional(), .Right = TDecimal{TDecimalType{35, 35}, "0.1"}.AsOptional(), .Operation = "==", .Expected = true},
        {
            .Left = TDecimal{TDecimalType{35, 35}, "0.00000000000000000000000000000000001"}.AsOptional(),
            .Right = TDecimal{TDecimalType{1, 1}, "0.1"}.AsOptional(),
            .Operation = "<",
            .Expected = true,
        },
        {.Left = TDecimal{TDecimalType{35, 0}, "99999999999999999999999999999999999"}.AsOptional(),
         .Right = TDecimal{TDecimalType{35, 35}, "0.99999999999999999999999999999999999"}.AsOptional(),
         .Operation = ">",
         .Expected = true},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}.AsOptional(), .Right = TDecimal{TDecimalType{35, 2}, "1.24"}.AsOptional(), .Operation = "<", .Expected = true},
        {.Left = TNullDecimal{TDecimalType{35, 35}}, .Right = TDecimal{TDecimalType{1, 1}, "0.1"}.AsOptional(), .Operation = "<", .Expected = {}},
        {.Left = TDecimal{TDecimalType{1, 1}, "0.1"}.AsOptional(), .Right = TNullDecimal{TDecimalType{35, 35}}, .Operation = ">=", .Expected = {}},
        {.Left = TDecimal{TDecimalType{35, 7}, "7142857142857142857142857142.8571427"}.AsOptional(),
         .Right = TDecimal{TDecimalType{35, 7}, "7142857142857142857142857142.8571427"}.AsOptional(),
         .Operation = "==",
         .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestDecimalComparisonOptionalNulls) {
    TSetup<LLVM> setup;
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "==", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "!=", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "<", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TDecimal{TDecimalType{5, 4}, "1.23"}, .Operation = "<=", .Expected = {}},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TNullDecimal{TDecimalType{5, 4}}, .Operation = ">", .Expected = {}},
        {.Left = TDecimal{TDecimalType{3, 2}, "1.23"}, .Right = TNullDecimal{TDecimalType{5, 4}}, .Operation = ">=", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TNullDecimal{TDecimalType{5, 4}}, .Operation = "==", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{3, 2}}, .Right = TDecimal{TDecimalType{5, 4}, "-1.23"}.AsOptional(), .Operation = "<", .Expected = {}},
        {.Left = TNullDecimal{TDecimalType{35, 14}},
         .Right = TDecimal{TDecimalType{35, 14}, "-499999999999999999999.99999999999999"}.AsOptional(),
         .Operation = "==",
         .Expected = {}},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST(TestDecimalComparisonBlocks) {
    const TVector<TBlockComparisonCase> equalsCases = {
        {"1.23 == 1.2300", {123}, {12300}, true},
    };
    RunComparisonCasesBlocks("Equals", equalsCases);

    const TVector<TBlockComparisonCase> notEqualsCases = {
        {"1.23 != 1.2301", {123}, {12301}, true},
    };
    RunComparisonCasesBlocks("NotEquals", notEqualsCases);

    const TVector<TBlockComparisonCase> lessCases = {
        {"1.23 < 1.2301", {123}, {12301}, true},
    };
    RunComparisonCasesBlocks("Less", lessCases);

    const TVector<TBlockComparisonCase> greaterCases = {
        {"1.23 > 1.2299", {123}, {12299}, true},
    };
    RunComparisonCasesBlocks("Greater", greaterCases);

    const TVector<TBlockComparisonCase> lessOrEqualCases = {
        {"1.23 <= 1.2300", {123}, {12300}, true},
    };
    RunComparisonCasesBlocks("LessOrEqual", lessOrEqualCases);

    const TVector<TBlockComparisonCase> greaterOrEqualCases = {
        {"1.23 >= 1.2300", {123}, {12300}, true},
    };
    RunComparisonCasesBlocks("GreaterOrEqual", greaterOrEqualCases);
}

Y_UNIT_TEST_LLVM(TestComparesWithIntegral) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<TMaybe<i64>, NTest::TDecimalLiteral<20, 18>>>{
                                                           {{-7LL}, {-7000000000000000000LL}},
                                                           {{-7LL}, {3000000000000000000LL}},
                                                           {{-7LL}, {NYql::NDecimal::Nan()}},
                                                           {{-7LL}, {-NYql::NDecimal::Inf()}},
                                                           {{-7LL}, {+NYql::NDecimal::Inf()}},

                                                           {{3LL}, {-7000000000000000000LL}},
                                                           {{3LL}, {3000000000000000000LL}},
                                                           {{3LL}, {NYql::NDecimal::Nan()}},
                                                           {{3LL}, {-NYql::NDecimal::Inf()}},
                                                           {{3LL}, {+NYql::NDecimal::Inf()}},

                                                           {TMaybe<i64>{}, {-7000000000000000000LL}},
                                                           {TMaybe<i64>{}, {3000000000000000000LL}},
                                                           {TMaybe<i64>{}, {NYql::NDecimal::Nan()}},
                                                           {TMaybe<i64>{}, {-NYql::NDecimal::Inf()}},
                                                           {TMaybe<i64>{}, {+NYql::NDecimal::Inf()}},

                                                           {{Min<i64>()}, {-7000000000000000000LL}},
                                                           {{Min<i64>()}, {3000000000000000000LL}},
                                                           {{Min<i64>()}, {NYql::NDecimal::Nan()}},
                                                           {{Min<i64>()}, {-NYql::NDecimal::Inf()}},
                                                           {{Min<i64>()}, {+NYql::NDecimal::Inf()}},

                                                           {{Max<i64>()}, {-7000000000000000000LL}},
                                                           {{Max<i64>()}, {3000000000000000000LL}},
                                                           {{Max<i64>()}, {NYql::NDecimal::Nan()}},
                                                           {{Max<i64>()}, {-NYql::NDecimal::Inf()}},
                                                           {{Max<i64>()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Equals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.NotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Less(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.LessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.Greater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.GreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>, TMaybe<bool>>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          {true, false, false, true, false, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, true, true},
                                                          {true, false, false, true, false, true},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {{}, {}, {}, {}, {}, {}},
                                                          {false, true, true, true, false, false},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, false, false, true, true},
                                                          {false, true, false, false, false, false},
                                                          {false, true, false, false, true, true},
                                                          {false, true, true, true, false, false},
                                                      });
}

Y_UNIT_TEST_LLVM(TestEqualsWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TDecimalType overflowDecimalType{20, 18};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = Max<i8>(), .Right = TDecimal{decimalType, "127"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "127"}, .Right = Max<i8>(), .Operation = "==", .Expected = true},
        {.Left = Max<ui8>(), .Right = TDecimal{decimalType, "255"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "255"}, .Right = Max<ui8>(), .Operation = "==", .Expected = true},
        {.Left = Max<i16>(), .Right = TDecimal{decimalType, "32767"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "32767"}, .Right = Max<i16>(), .Operation = "==", .Expected = true},
        {.Left = Max<ui16>(), .Right = TDecimal{decimalType, "65535"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "65535"}, .Right = Max<ui16>(), .Operation = "==", .Expected = true},
        {.Left = Max<i32>(), .Right = TDecimal{decimalType, "2147483647"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "2147483647"}, .Right = Max<i32>(), .Operation = "==", .Expected = true},
        {.Left = Max<ui32>(), .Right = TDecimal{decimalType, "4294967295"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "4294967295"}, .Right = Max<ui32>(), .Operation = "==", .Expected = true},
        {.Left = Max<i64>(), .Right = TDecimal{decimalType, "9223372036854775807"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "9223372036854775807"}, .Right = Max<i64>(), .Operation = "==", .Expected = true},
        {.Left = Max<ui64>(), .Right = TDecimal{decimalType, "18446744073709551615"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "18446744073709551615"}, .Right = Max<ui64>(), .Operation = "==", .Expected = true},
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-7"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "-128"}, .Right = Min<i8>(), .Operation = "==", .Expected = true},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "127"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "10922"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = "==", .Expected = true},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65534"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = static_cast<i32>(Max<i32>() - 7), .Operation = "==", .Expected = true},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483647"}, .Operation = "==", .Expected = true},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>() - 1, .Operation = "==", .Expected = true},
        {.Left = Max<ui64>() - 7, .Right = TDecimal{decimalType, "18446744073709551608"}, .Operation = "==", .Expected = true},
        {.Left = i8{7}, .Right = TDecimal{decimalType, "7.5"}, .Operation = "==", .Expected = false},
        {.Left = TDecimal{decimalType, "-7.5"}, .Right = i16{-7}, .Operation = "==", .Expected = false},
        {.Left = Max<i64>(), .Right = TPositiveInf{overflowDecimalType}, .Operation = "==", .Expected = false},
        {.Left = TPositiveInf{overflowDecimalType}, .Right = Max<ui64>(), .Operation = "==", .Expected = false},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestNotEqualsWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TDecimalType overflowDecimalType{20, 18};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-6"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{decimalType, "-7"}, .Right = i16{-7}, .Operation = "!=", .Expected = false},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "126"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{decimalType, "10922"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = "!=", .Expected = false},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65535"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = static_cast<i32>(Max<i32>() - 7), .Operation = "!=", .Expected = false},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483646"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>() - 1, .Operation = "!=", .Expected = false},
        {.Left = Max<ui64>() - 7, .Right = TDecimal{decimalType, "18446744073709551614"}, .Operation = "!=", .Expected = true},
        {.Left = i16{42}, .Right = TDecimal{decimalType, "42.25"}, .Operation = "!=", .Expected = true},
        {.Left = TDecimal{decimalType, "-42.25"}, .Right = i32{-42}, .Operation = "!=", .Expected = true},
        {.Left = Max<i64>(), .Right = TPositiveInf{overflowDecimalType}, .Operation = "!=", .Expected = true},
        {.Left = TPositiveInf{overflowDecimalType}, .Right = Max<ui64>(), .Operation = "!=", .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestLessWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TDecimalType overflowDecimalType{20, 18};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-6"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{decimalType, "-7"}, .Right = i16{-7}, .Operation = "<", .Expected = false},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "128"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{decimalType, "10921"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = "<", .Expected = true},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65533"}, .Operation = "<", .Expected = false},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = Max<i32>() - 1, .Operation = "<", .Expected = true},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483647"}, .Operation = "<", .Expected = false},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>(), .Operation = "<", .Expected = true},
        {.Left = Max<ui64>() - 7, .Right = TDecimal{decimalType, "18446744073709551614"}, .Operation = "<", .Expected = true},
        {.Left = ui8{7}, .Right = TDecimal{decimalType, "7.5"}, .Operation = "<", .Expected = true},
        {.Left = TDecimal{decimalType, "-7.5"}, .Right = i64{-7}, .Operation = "<", .Expected = true},
        {.Left = Max<i64>(), .Right = TPositiveInf{overflowDecimalType}, .Operation = "<", .Expected = true},
        {.Left = Max<ui64>(), .Right = TPositiveInf{overflowDecimalType}, .Operation = "<", .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestLessOrEqualWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-7"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{decimalType, "-6"}, .Right = i16{-7}, .Operation = "<=", .Expected = false},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "127"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{decimalType, "10921"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = "<=", .Expected = true},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65533"}, .Operation = "<=", .Expected = false},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = Max<i32>() - 7, .Operation = "<=", .Expected = true},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483646"}, .Operation = "<=", .Expected = false},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>(), .Operation = "<=", .Expected = true},
        {.Left = Max<ui64>() - 7, .Right = TDecimal{decimalType, "18446744073709551608"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{decimalType, "18446744073709551614"}, .Right = Max<ui64>() - 7, .Operation = "<=", .Expected = false},
        {.Left = i32{7}, .Right = TDecimal{decimalType, "7.5"}, .Operation = "<=", .Expected = true},
        {.Left = TDecimal{decimalType, "-6.5"}, .Right = i16{-7}, .Operation = "<=", .Expected = false},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestGreaterWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TDecimalType overflowDecimalType{20, 18};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-8"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{decimalType, "-7"}, .Right = i16{-7}, .Operation = ">", .Expected = false},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "126"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{decimalType, "10923"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = ">", .Expected = true},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65535"}, .Operation = ">", .Expected = false},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = Max<i32>() - 1, .Operation = ">", .Expected = false},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483647"}, .Operation = ">", .Expected = false},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>() - 7, .Operation = ">", .Expected = true},
        {.Left = Max<ui64>() - 1, .Right = TDecimal{decimalType, "18446744073709551608"}, .Operation = ">", .Expected = true},
        {.Left = TDecimal{decimalType, "7.5"}, .Right = ui16{7}, .Operation = ">", .Expected = true},
        {.Left = i64{-7}, .Right = TDecimal{decimalType, "-7.5"}, .Operation = ">", .Expected = true},
        {.Left = TPositiveInf{overflowDecimalType}, .Right = Max<i64>(), .Operation = ">", .Expected = true},
        {.Left = TPositiveInf{overflowDecimalType}, .Right = Max<ui64>(), .Operation = ">", .Expected = true},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestGreaterOrEqualWithIntegral) {
    TSetup<LLVM> setup;
    const TDecimalType decimalType{35, 15};
    const TVector<TDecimalBinaryCase<bool>> cases = {
        {.Left = i8{-7}, .Right = TDecimal{decimalType, "-7"}, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{decimalType, "-8"}, .Right = i16{-7}, .Operation = ">=", .Expected = false},
        {.Left = static_cast<ui8>(Max<ui8>() / 2), .Right = TDecimal{decimalType, "127"}, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{decimalType, "10923"}, .Right = static_cast<i16>(Max<i16>() / 3), .Operation = ">=", .Expected = true},
        {.Left = static_cast<ui16>(Max<ui16>() - 1), .Right = TDecimal{decimalType, "65535"}, .Operation = ">=", .Expected = false},
        {.Left = TDecimal{decimalType, "2147483640"}, .Right = Max<i32>() - 7, .Operation = ">=", .Expected = true},
        {.Left = static_cast<ui32>(Max<ui32>() / 2), .Right = TDecimal{decimalType, "2147483648"}, .Operation = ">=", .Expected = false},
        {.Left = TDecimal{decimalType, "9223372036854775806"}, .Right = Max<i64>() - 7, .Operation = ">=", .Expected = true},
        {.Left = Max<ui64>() - 7, .Right = TDecimal{decimalType, "18446744073709551608"}, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{decimalType, "18446744073709551614"}, .Right = Max<ui64>() - 7, .Operation = ">=", .Expected = true},
        {.Left = TDecimal{decimalType, "7.5"}, .Right = ui32{7}, .Operation = ">=", .Expected = true},
        {.Left = i8{-8}, .Right = TDecimal{decimalType, "-7.5"}, .Operation = ">=", .Expected = false},
    };
    RunBinaryCasesNonBlocks(*setup.PgmBuilder, cases, [&setup](TRuntimeNode program) {
        return setup.BuildGraph(program);
    });
}

Y_UNIT_TEST_LLVM(TestAggrCompares) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<std::tuple<NTest::TDecimalLiteral<10, 0>, NTest::TDecimalLiteral<10, 0>>>{
                                                           {{-7}, {-7}},
                                                           {{-7}, {3}},
                                                           {{-7}, {NYql::NDecimal::Nan()}},
                                                           {{-7}, {-NYql::NDecimal::Inf()}},
                                                           {{-7}, {+NYql::NDecimal::Inf()}},

                                                           {{3}, {-7}},
                                                           {{3}, {3}},
                                                           {{3}, {NYql::NDecimal::Nan()}},
                                                           {{3}, {-NYql::NDecimal::Inf()}},
                                                           {{3}, {+NYql::NDecimal::Inf()}},

                                                           {{NYql::NDecimal::Nan()}, {-7}},
                                                           {{NYql::NDecimal::Nan()}, {3}},
                                                           {{NYql::NDecimal::Nan()}, {NYql::NDecimal::Nan()}},
                                                           {{NYql::NDecimal::Nan()}, {-NYql::NDecimal::Inf()}},
                                                           {{NYql::NDecimal::Nan()}, {+NYql::NDecimal::Inf()}},

                                                           {{-NYql::NDecimal::Inf()}, {-7}},
                                                           {{-NYql::NDecimal::Inf()}, {3}},
                                                           {{-NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{-NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{-NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},

                                                           {{+NYql::NDecimal::Inf()}, {-7}},
                                                           {{+NYql::NDecimal::Inf()}, {3}},
                                                           {{+NYql::NDecimal::Inf()}, {NYql::NDecimal::Nan()}},
                                                           {{+NYql::NDecimal::Inf()}, {-NYql::NDecimal::Inf()}},
                                                           {{+NYql::NDecimal::Inf()}, {+NYql::NDecimal::Inf()}},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.AggrEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrNotEquals(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrLess(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrLessOrEqual(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrGreater(pb.Nth(item, 0), pb.Nth(item, 1)),
                                                          pb.AggrGreaterOrEqual(pb.Nth(item, 0), pb.Nth(item, 1))});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<bool, bool, bool, bool, bool, bool>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{true, false, false, true, false, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{false, true, true, true, false, false},
                                                          TRow{false, true, false, false, true, true},
                                                          TRow{true, false, false, true, false, true},
                                                      });
}

Y_UNIT_TEST_LLVM(TestIncDec) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<4, 1>>{
                                                           {-NYql::NDecimal::Inf()},
                                                           {-9999},
                                                           {-7},
                                                           {0},
                                                           {13},
                                                           {9999},
                                                           {+NYql::NDecimal::Inf()},
                                                           {NYql::NDecimal::Nan()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Increment(item), pb.Decrement(item)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{-NYql::NDecimal::Inf(), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(-9998), -NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(-6), NYql::NDecimal::TInt128(-8)},
                                                          TRow{NYql::NDecimal::TInt128(1), NYql::NDecimal::TInt128(-1)},
                                                          TRow{NYql::NDecimal::TInt128(14), NYql::NDecimal::TInt128(12)},
                                                          TRow{+NYql::NDecimal::Inf(), NYql::NDecimal::TInt128(9998)},
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                      });
}
Y_UNIT_TEST_LLVM(TestMinusAbs) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 1>>{
                                                           {-NYql::NDecimal::Inf()},
                                                           {-7},
                                                           {0},
                                                           {13},
                                                           {+NYql::NDecimal::Inf()},
                                                           {NYql::NDecimal::Nan()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.NewTuple({pb.Minus(item), pb.Abs(item)});
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    using TRow = std::tuple<NYql::NDecimal::TInt128, NYql::NDecimal::TInt128>;
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                          TRow{+NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::TInt128(7), NYql::NDecimal::TInt128(7)},
                                                          TRow{NYql::NDecimal::TInt128(0), NYql::NDecimal::TInt128(0)},
                                                          TRow{NYql::NDecimal::TInt128(-13), NYql::NDecimal::TInt128(13)},
                                                          TRow{-NYql::NDecimal::Inf(), +NYql::NDecimal::Inf()},
                                                          TRow{NYql::NDecimal::Nan(), NYql::NDecimal::Nan()},
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TStringBuf>{
                                                           "0.0", "NAN", "1.0", "-.1", "3.1415926", "+inf", "-INF", ".123E+2", "56.78e-3",
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.StrictFromString(item, pb.NewDecimalType(10, 7));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<NYql::NDecimal::TInt128>{
                                                          NYql::NDecimal::TInt128(0),
                                                          NYql::NDecimal::Nan(),
                                                          NYql::NDecimal::TInt128(10000000),
                                                          NYql::NDecimal::TInt128(-1000000),
                                                          NYql::NDecimal::TInt128(31415926),
                                                          +NYql::NDecimal::Inf(),
                                                          -NYql::NDecimal::Inf(),
                                                          NYql::NDecimal::TInt128(123000000),
                                                          NYql::NDecimal::TInt128(567800),
                                                      });
}
Y_UNIT_TEST_LLVM(TestToString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TDecimalLiteral<10, 7>>{
                                                           {0},
                                                           {NYql::NDecimal::Nan()},
                                                           {10000000},
                                                           {-1000000},
                                                           {31415926},
                                                           {+NYql::NDecimal::Inf()},
                                                           {-NYql::NDecimal::Inf()},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.ToString(item);
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "0", "nan", "1", "-0.1", "3.1415926", "inf", "-inf",
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromStringToDouble) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<TStringBuf>{
                                                           "0", "+3.332873", "-3.332873", "+3.1415926", "-3.1415926",
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(pb.FromString(item, pb.NewDecimalType(35, 25)), pb.NewDataType(NUdf::TDataType<double>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<double>{
                                                          0., +3.332873, -3.332873, +3.1415926, -3.1415926,
                                                      });
}
Y_UNIT_TEST_LLVM(TestFromUtf8ToFloat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb,
                                                       TVector<NTest::TUtf8>{
                                                           NTest::TUtf8{TString("0")},
                                                           NTest::TUtf8{TString("+24.75")},
                                                           NTest::TUtf8{TString("-24.75")},
                                                           NTest::TUtf8{TString("+42.42")},
                                                           NTest::TUtf8{TString("-42.42")},
                                                       });

    const auto pgmReturn = pb.Map(list,
                                  [&](TRuntimeNode item) {
                                      return pb.Convert(pb.FromString(item, pb.NewDecimalType(35, 25)), pb.NewDataType(NUdf::TDataType<float>::Id));
                                  });

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<float>{
                                                          0.F, +24.75F, -24.75F, +42.42F, -42.42F,
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLDecimalTest)

} // namespace NKikimr::NMiniKQL
