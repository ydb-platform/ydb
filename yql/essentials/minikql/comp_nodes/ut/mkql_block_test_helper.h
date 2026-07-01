#pragma once

#include "mkql_block_fuzzer.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

#include <variant>

namespace NKikimr::NMiniKQL {

arrow::Datum ConvertDatumToArrowFormat(arrow::Datum datum, arrow::MemoryPool& pool);

template <class T>
struct TIsVector: std::false_type {};

template <class T>
struct TIsVector<TVector<T>>: std::true_type {};

template <class T>
constexpr bool TIsVectorV = TIsVector<std::remove_cvref_t<T>>::value;

class TBlockHelper {
public:
    constexpr static size_t SignleIteration = 1;
    constexpr static size_t ManyIterations = 1000;

    explicit TBlockHelper()
        : Setup_(GetNodeTestFactory())
        , Pb_(*Setup_.PgmBuilder)
    {
    }

    template <typename T>
    TRuntimeNode ConvertNodeWithSpecificFuzzer(const TVector<T>& nodes, ui64 fuzzId) {
        return ConvertLiteralListToDatum(NTest::ConvertValueToLiteralNode(Pb_, nodes), fuzzId);
    }

    template <typename T>
        requires(!TIsVectorV<T>)
    TRuntimeNode ConvertNodeFuzzied(const T& node) {
        return Pb_.AsScalar(NTest::ConvertValueToLiteralNode(Pb_, node));
    }

    template <typename T>
    TRuntimeNode ConvertNodeFuzzied(const TVector<T>& nodes) {
        ui64 fuzzId = FuzzerHolder_.ReserveFuzzer();
        auto convertedNode = ConvertNodeWithSpecificFuzzer(nodes, fuzzId);
        FuzzerHolder_.CreateFuzzers(TFuzzOptions::FuzzAll(), fuzzId, convertedNode.GetStaticType(), Pb_.GetTypeEnvironment());
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
            auto leftNode = ConvertNodeFuzzied(left);
            auto rightNode = ConvertNodeFuzzied(right);
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
        auto node = ConvertNodeFuzzied(value);
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
        auto blockNode = ConvertNodeFuzzied(data);
        auto blockList = Pb_.NewList(blockNode.GetStaticType(), {blockNode});
        auto pgmReturn = Pb_.Collect(Pb_.ForwardList(Pb_.FromBlocks(Pb_.ToFlow(blockList))));
        auto graph = Setup_.BuildGraph(pgmReturn);
        auto value = graph->GetValue();
        return {std::move(graph), std::move(value)};
    }

    template <typename T>
    std::tuple<THolder<IComputationGraph>, NUdf::TUnboxedValue, TType*, TType*> GetArrowBlock(const T& value) {
        auto node = ConvertNodeFuzzied(value);
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
            auto node = ConvertNodeFuzzied(operand);
            auto expectedNode = ConvertNodeUnfuzzied(expected);

            auto resultValue = Setup_.BuildGraph(unaryOp(Setup_, node))->GetValue();
            auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();

            auto outDatum = ConvertDatumToArrowFormat(TArrowBlock::From(resultValue).GetDatum(), *arrow::default_memory_pool());
            auto expectedDatum = ConvertDatumToArrowFormat(TArrowBlock::From(expectedValue).GetDatum(), *arrow::default_memory_pool());

            UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "\nExpected : " << DatumToString(expectedDatum) << "\nBut got : " << DatumToString(outDatum));
        }
    }

    template <typename T, typename V>
    void TestKernelFuzzied(const TVector<T>& operand, const TVector<V>& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp, TMaybe<size_t> iterations = Nothing()) {
        MKQL_ENSURE(operand.size() == expected.size(), "Size mismatch.");
        // Test fuzzied vectors.
        TestKernel(operand, expected, unaryOp, iterations);
        for (size_t i = 0; i < operand.size(); i++) {
            // Test subsequent scalars.
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
                // Test subsequent scalars.
                TestKernel(left[i], right[i], expected[i], binaryOp, iterations);
            }
        } else if constexpr (TIsVectorV<T>) {
            TestKernel(left, right, expected, binaryOp, iterations);
            for (size_t i = 0; i < left.size(); i++) {
                // Test subsequent scalars.
                TestKernel(left[i], right, expected[i], binaryOp, iterations);
            }
        } else if constexpr (TIsVectorV<U>) {
            TestKernel(left, right, expected, binaryOp, iterations);
            for (size_t i = 0; i < right.size(); i++) {
                // Test subsequent scalars.
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
    void ClearFuzzers() {
        FuzzerHolder_.ClearFuzzers();
    }

    IComputationNode* WrapMaterializeBlockStream(TCallable& callable, const TComputationNodeFactoryContext& ctx);

    TComputationNodeFactory GetNodeTestFactory();

    TString DatumToString(arrow::Datum datum);

    TRuntimeNode MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream, ui64 fuzzId);

    TSetup<false> Setup_;
    TProgramBuilder& Pb_;
    TFuzzerHolder FuzzerHolder_;
};

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
