#pragma once

#include "mkql_block_fuzzer.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/stream_view.h>
#include <yql/essentials/public/udf/udf_type_printer.h>

#include <tuple>
#include <variant>

namespace NKikimr::NMiniKQL {

arrow::Datum ConvertDatumToArrowFormat(arrow::Datum datum, arrow::MemoryPool& pool);

template <class T>
struct TIsVector: std::false_type {};

template <class T>
struct TIsVector<TVector<T>>: std::true_type {};

template <class T>
constexpr bool TIsVectorV = TIsVector<std::remove_cvref_t<T>>::value;

template <class... Ts>
TVector<std::tuple<Ts...>> TupleZip(const TVector<Ts>&... vecs) {
    const auto& first = std::get<0>(std::tie(vecs...));
    const std::size_t n = first.size();
    MKQL_ENSURE(((vecs.size() == n) && ...), "Vectors must have same size.");
    TVector<std::tuple<Ts...>> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        out.emplace_back(vecs[i]...);
    }
    return out;
}

class TBlockHelper {
public:
    constexpr static size_t SignleIteration = 1;

#if defined(_asan_enabled_) || defined(_msan_enabled_)
    constexpr static size_t ManyIterations = 100;
#else
    constexpr static size_t ManyIterations = 1000;
#endif

    explicit TBlockHelper(NYql::EDatumValidationMode validationMode = NYql::EDatumValidationMode::Expensive)
        : Setup_(GetNodeTestFactory())
        , Pb_(*Setup_.PgmBuilder)
    {
        Setup_.RuntimeSettings->DatumValidation.Set(validationMode);
    }

    TProgramBuilder& ProgramBuilder() {
        return Pb_;
    }

    void ValidateDatum(arrow::Datum datum, TMaybe<arrow::ValueDescr> expectedDescription, const TType* type) {
        ::NKikimr::NMiniKQL::ValidateDatum(datum, expectedDescription, type, Setup_.RuntimeSettings->DatumValidation.Get());
    }

    template <typename T>
    TRuntimeNode ConvertNodeWithSpecificFuzzer(const TVector<T>& nodes, ui64 fuzzId) {
        return ConvertLiteralListToDatum(NTest::ConvertValueToLiteralNode(Pb_, nodes), fuzzId);
    }

    template <typename T>
        requires(!TIsVectorV<T>)
    TRuntimeNode ConvertNodeFuzzied(const T& node, bool) {
        return Pb_.AsScalar(NTest::ConvertValueToLiteralNode(Pb_, node));
    }

    template <typename T>
    TRuntimeNode ConvertNodeFuzzied(const TVector<T>& nodes, bool chunked) {
        ui64 fuzzId = FuzzerHolder_.ReserveFuzzer();
        auto convertedNode = ConvertNodeWithSpecificFuzzer(nodes, fuzzId);
        FuzzerHolder_.CreateFuzzers(fuzzId, convertedNode.GetStaticType(), Pb_.GetTypeEnvironment(),
                                    Setup_.RuntimeSettings->DatumValidation.Get(), chunked);
        return convertedNode;
    }

    template <typename T>
        requires(!TIsVectorV<T>)
    TRuntimeNode ConvertNodeUnfuzzied(const T& node) {
        return Pb_.AsScalar(NTest::ConvertValueToLiteralNode(Pb_, node));
    }

    template <typename T>
    TRuntimeNode ConvertNodeUnfuzzied(const TVector<T>& nodes) {
        return ConvertNodeWithSpecificFuzzer(nodes, TFuzzerHolder::EmptyFuzzerId);
    }

    TRuntimeNode ConvertLiteralListToDatum(TRuntimeNode list, ui64 fuzzId);

    template <typename T, typename U, typename V>
    void TestKernel(const T& left, const U& right, const V& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp, TMaybe<size_t> iterations = Nothing()) {
        size_t iterationCount = (TIsVectorV<T> || TIsVectorV<U>) ? ManyIterations : SignleIteration;
        if (iterations) {
            iterationCount = *iterations;
        }
        for (size_t i = 0; i < iterationCount; i++) {
            NYql::TExprContext exprCtx;
            auto leftNode = ConvertNodeFuzzied(left, /*chunked=*/false);
            auto rightNode = ConvertNodeFuzzied(right, /*chunked=*/false);
            auto expectedNode = ConvertNodeUnfuzzied(expected);

            auto resultValue = Setup_.BuildGraph(binaryOp(Setup_, leftNode, rightNode))->GetValue();
            auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();
            auto outDatum = ConvertDatumToArrowFormat(TArrowBlock::From(resultValue).GetDatum(), *arrow::default_memory_pool());
            auto expectedDatum = ConvertDatumToArrowFormat(TArrowBlock::From(expectedValue).GetDatum(), *arrow::default_memory_pool());

            UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << DatumToString(expectedDatum) << "\n but got : " << DatumToString(outDatum));
        }
    }

    template <typename T>
    std::tuple<THolder<IComputationGraph>, NUdf::TUnboxedValue, TType*, TType*> GetScalarBlock(const T& value) {
        auto node = ConvertNodeFuzzied(value, /*chunked=*/false);
        auto blockType = node.GetStaticType();
        auto itemType = AS_TYPE(TBlockType, blockType)->GetItemType();
        auto graph = Setup_.BuildGraph(node);
        auto returnValue = graph->GetValue();
        return std::make_tuple(std::move(graph), std::move(returnValue), itemType, blockType);
    }

    template <typename T>
    std::pair<THolder<IComputationGraph>, NUdf::TUnboxedValue> BuildAndRunListFuzzied(
        const TVector<T>& data)
    {
        auto blockNode = ConvertNodeFuzzied(data, /*chunked=*/false);
        auto blockList = Pb_.NewList(blockNode.GetStaticType(), {blockNode});
        auto pgmReturn = Pb_.Collect(Pb_.ForwardList(Pb_.FromBlocks(Pb_.ToFlow(blockList, {}))));
        auto graph = Setup_.BuildGraph(pgmReturn);
        auto value = graph->GetValue();
        return {std::move(graph), std::move(value)};
    }

    template <typename TExpected, typename TOp, typename... TInputs>
    void RunNodeOverWideStream(const TExpected& expected, const TOp& blockOp, const TInputs&... inputs) {
        static_assert(sizeof...(TInputs) > 0, "At least one input is required");
        constexpr size_t N = sizeof...(TInputs);
        static_assert(TIsVectorV<TExpected> || (!TIsVectorV<TInputs> && ...), "A scalar expected requires all-scalar inputs");
        auto resolved = ResolveBlockOpInputs(inputs...);

        RunFuzzedWideStreamOp(expected, resolved.VectorLists, [&](TRuntimeNode fuzzed) {
            return BlockWideMap(fuzzed, [&](TRuntimeNode::TList columns) -> TRuntimeNode::TList {
                auto args = AssembleBlockOpArguments<N>(columns, resolved.ScalarNodes, resolved.ColumnIndex);
                auto resultBlock = [&]<size_t... Is>(std::index_sequence<Is...>) {
                    return blockOp(Setup_, args[Is]...);
                }(std::make_index_sequence<N>{});
                return {resultBlock, columns.back()};
            });
        });
    }

    template <typename... TExpected, typename TStreamOp, typename... TInputs>
    void RunWideStreamNode(const std::tuple<TVector<TExpected>...>& expected, TStreamOp&& streamOp,
                           bool unordered, const std::tuple<TInputs...>& inputs) {
        static_assert(sizeof...(TInputs) > 0, "At least one input column is required");
        constexpr size_t N = sizeof...(TInputs);

        auto resolved = std::apply([this](const TInputs&... cols) { return ResolveBlockOpInputs(cols...); }, inputs);
        auto fuzzed = BuildFuzzedWideStream(resolved.VectorLists);
        auto assembled = BlockWideMap(fuzzed, [&](TRuntimeNode::TList columns) -> TRuntimeNode::TList {
            auto args = AssembleBlockOpArguments<N>(columns, resolved.ScalarNodes, resolved.ColumnIndex);
            TRuntimeNode::TList result(args.begin(), args.end());
            result.push_back(columns.back());
            return result;
        });

        auto applied = streamOp(Setup_, assembled);
        auto collected = ReadWideStreamColumnsAsTuples(applied);
        auto graph = Setup_.BuildGraph(collected);
        auto value = graph->GetValue();

        const auto expectedRows = std::apply([](const TVector<TExpected>&... cols) { return TupleZip(cols...); }, expected);
        const auto expectedType = NTest::ConvertToMinikqlType<std::tuple<TExpected...>>(Pb_);
        const auto streamType = AS_TYPE(TStreamType, collected.GetStaticType());
        UNIT_ASSERT_C(expectedType->IsSameType(*streamType->GetItemType()),
                      "Expected type mismatch " << TypeToString(expectedType)
                                                << " != "
                                                << TypeToString(streamType->GetItemType()));
        if (unordered) {
            NYql::NUdf::AssertUnboxedValueElementEqualUnordered(
                value, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<TExpected...>>(expectedRows));
        } else {
            NYql::NUdf::AssertUnboxedValueElementEqual(
                value, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<TExpected...>>(expectedRows));
        }
    }

    template <typename T>
    std::tuple<THolder<IComputationGraph>, NUdf::TUnboxedValue, TType*, TType*> GetArrowBlock(const T& value) {
        auto node = ConvertNodeFuzzied(value, /*chunked=*/false);
        auto blockType = node.GetStaticType();
        auto itemType = AS_TYPE(TBlockType, blockType)->GetItemType();
        auto graph = Setup_.BuildGraph(node);
        auto returnValue = graph->GetValue();
        return std::make_tuple(std::move(graph), std::move(returnValue), itemType, blockType);
    }

    template <typename T, typename V>
    void TestKernel(const T& operand, const V& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp, TMaybe<size_t> iterations = Nothing()) {
        size_t iterationCount = (TIsVectorV<T> || TIsVectorV<V>) ? ManyIterations : SignleIteration;
        if (iterations) {
            iterationCount = *iterations;
        }
        for (size_t i = 0; i < iterationCount; i++) {
            NYql::TExprContext exprCtx;
            auto node = ConvertNodeFuzzied(operand, /*chunked=*/false);
            auto expectedNode = ConvertNodeUnfuzzied(expected);

            auto resultValue = Setup_.BuildGraph(unaryOp(Setup_, node))->GetValue();
            auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();

            auto outDatum = ConvertDatumToArrowFormat(TArrowBlock::From(resultValue).GetDatum(), *arrow::default_memory_pool());
            auto expectedDatum = ConvertDatumToArrowFormat(TArrowBlock::From(expectedValue).GetDatum(), *arrow::default_memory_pool());
            CompareDatums(expectedDatum, outDatum);
        }
    }

    template <typename T, typename V>
    void TestKernelFuzzied(const TVector<T>& operand, const TVector<V>& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp, TMaybe<size_t> iterations = Nothing()) {
        MKQL_ENSURE(operand.size() == expected.size(), "Size mismatch.");
        TestKernel(operand, expected, unaryOp, iterations);
        for (size_t i = 0; i < operand.size(); i++) {
            TestKernel(operand[i], expected[i], unaryOp, iterations);
        }
    }

    template <typename T, typename U, typename V>
    void TestKernelFuzzied(const T& left, const U& right, const V& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp, TMaybe<size_t> iterations = Nothing()) {
        if constexpr (TIsVectorV<T> && TIsVectorV<U>) {
            MKQL_ENSURE(left.size() == right.size(), "Size mismatch.");
            MKQL_ENSURE(left.size() == expected.size(), "Size mismatch.");
        }

        if constexpr (TIsVectorV<T> && TIsVectorV<U>) {
            TestKernel(left, right, expected, binaryOp, iterations);
            for (size_t i = 0; i < left.size(); i++) {
                TestKernel(left[i], right[i], expected[i], binaryOp, iterations);
            }
        } else if constexpr (TIsVectorV<T>) {
            TestKernel(left, right, expected, binaryOp, iterations);
            for (size_t i = 0; i < left.size(); i++) {
                TestKernel(left[i], right, expected[i], binaryOp, iterations);
            }
        } else if constexpr (TIsVectorV<U>) {
            TestKernel(left, right, expected, binaryOp, iterations);
            for (size_t i = 0; i < right.size(); i++) {
                TestKernel(left, right[i], expected[i], binaryOp, iterations);
            }
        } else {
            TestKernel(left, right, expected, binaryOp, iterations);
        }
    }

    template <typename F>
    void WithScopedFuzzers(F&& f, size_t iterations = ManyIterations) {
        for (size_t i = 0; i < iterations; i++) {
            Y_DEFER {
                ClearFuzzers();
            };
            f();
        }
    }

private:
    TRuntimeNode BlockWideMap(TRuntimeNode flowOrStream, const TProgramBuilder::TWideLambda& handler);

    template <size_t N>
    struct TResolvedBlockOpInputs {
        TVector<TRuntimeNode> VectorLists;
        std::array<TRuntimeNode, N> ScalarNodes{};
        std::array<int, N> ColumnIndex{};
    };

    template <typename... TInputs>
    TResolvedBlockOpInputs<sizeof...(TInputs)> ResolveBlockOpInputs(const TInputs&... inputs) {
        TResolvedBlockOpInputs<sizeof...(TInputs)> resolved;
        int nextColumn = 0;
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            ([&] {
                if constexpr (TIsVectorV<TInputs>) {
                    resolved.ColumnIndex[Is] = nextColumn++;
                    resolved.VectorLists.push_back(NTest::ConvertValueToLiteralNode(Pb_, inputs));
                } else {
                    resolved.ColumnIndex[Is] = -1;
                    resolved.ScalarNodes[Is] = ConvertNodeUnfuzzied(inputs);
                }
            }(), ...);
        }(std::index_sequence_for<TInputs...>{});
        return resolved;
    }

    template <typename TExpected, typename TApplyOp>
    void RunFuzzedWideStreamOp(const TExpected& expected, const TVector<TRuntimeNode>& vectorLists, TApplyOp&& applyOp) {
        auto fuzzed = BuildFuzzedWideStream(vectorLists);
        auto applied = applyOp(fuzzed);
        auto singleColumn = ReadSingleWideStreamColumn(applied);
        auto graph = Setup_.BuildGraph(singleColumn);
        auto value = graph->GetValue();
        const auto streamType = AS_TYPE(TStreamType, singleColumn.GetStaticType());

        if constexpr (TIsVectorV<TExpected>) {
            const auto expectedType = NTest::ConvertToMinikqlType<typename TExpected::value_type>(Pb_);
            UNIT_ASSERT_C(expectedType->IsSameType(*streamType->GetItemType()),
                          "Expected type mismatch " << TypeToString(expectedType)
                                                    << " != "
                                                    << TypeToString(streamType->GetItemType()));
            NYql::NUdf::AssertUnboxedValueElementEqual(
                value, NYql::NUdf::TUnboxedValueComparatorStreamView<typename TExpected::value_type>(expected));
        } else {
            const auto expectedType = NTest::ConvertToMinikqlType<TExpected>(Pb_);
            UNIT_ASSERT_C(expectedType->IsSameType(*streamType->GetItemType()),
                          "Expected type mismatch " << TypeToString(expectedType)
                                                    << " != "
                                                    << TypeToString(streamType->GetItemType()));
            const TVector<TExpected> expectedValue = {expected};
            NYql::NUdf::AssertUnboxedValueElementEqual(
                value, NYql::NUdf::TUnboxedValueComparatorStreamView<TExpected>(expectedValue));
        }
    }

    template <size_t N>
    static std::array<TRuntimeNode, N> AssembleBlockOpArguments(const TRuntimeNode::TList& columns,
                                                                const std::array<TRuntimeNode, N>& scalarNodes,
                                                                const std::array<int, N>& columnIndex) {
        std::array<TRuntimeNode, N> args;
        for (size_t i = 0; i < N; ++i) {
            args[i] = columnIndex[i] >= 0 ? columns[static_cast<size_t>(columnIndex[i])] : scalarNodes[i];
        }
        return args;
    }

    TRuntimeNode ReadWideStreamColumnsAsTuples(TRuntimeNode wideStream) {
        auto multiType = AS_TYPE(TMultiType, AS_TYPE(TStreamType, wideStream.GetStaticType())->GetItemType());
        MKQL_ENSURE(multiType->GetElementsCount() >= 1, "Expected at least one column");
        TRuntimeNode narrowFlow;
        if (multiType->GetElementType(0)->IsBlock()) {
            MKQL_ENSURE(multiType->GetElementsCount() >= 2,
                        "Expected at least one data column plus the trailing block-length scalar");
            narrowFlow = Pb_.ToFlow(Pb_.WideFromBlocks(wideStream), {});
        } else {
            narrowFlow = Pb_.ToFlow(wideStream, {});
        }
        auto narrow = Pb_.NarrowMap(narrowFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return Pb_.NewTuple(items);
        });
        return Pb_.FromFlow(narrow);
    }

    void ClearFuzzers() {
        FuzzerHolder_.ClearFuzzers();
    }

    IComputationNode* WrapFuzzStream(TCallable& callable, const TComputationNodeFactoryContext& ctx);

    IComputationNode* WrapWideFuzzStream(TCallable& callable, const TComputationNodeFactoryContext& ctx);

    TComputationNodeFactory GetNodeTestFactory();

    TString DatumToString(arrow::Datum datum);

    void CompareDatums(arrow::Datum expected, arrow::Datum got);

    TString TypeToString(TType* type);

    TRuntimeNode FuzzStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream, ui64 fuzzId, const TStreamFuzzOptions& streamFuzzOptions);

    TRuntimeNode WideFuzzStream(TProgramBuilder& pgmBuilder, TRuntimeNode wideStream, const TVector<ui64>& fuzzIds, const TStreamFuzzOptions& streamFuzzOptions);

    TRuntimeNode MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

    TRuntimeNode BuildFuzzedWideStream(const TVector<TRuntimeNode>& vectorLists);

    TRuntimeNode BuildScalarOnlyWideBlockStream();

    TVector<ui64> MakeWideStreamColumnsFuzzers(TMultiType* multiType);

    TRuntimeNode ReadSingleWideStreamColumn(TRuntimeNode wideBlocks);

    TSetup<false> Setup_;
    TProgramBuilder& Pb_;
    TFuzzerHolder FuzzerHolder_;
};

template <NTest::TTag Tag, typename T>
TVector<NTest::TTagged<T, Tag>> TagVector(const TVector<T>& values) {
    TVector<NTest::TTagged<T, Tag>> result;
    result.reserve(values.size());
    for (const auto& v : values) {
        result.emplace_back(v);
    }
    return result;
}

} // namespace NKikimr::NMiniKQL
