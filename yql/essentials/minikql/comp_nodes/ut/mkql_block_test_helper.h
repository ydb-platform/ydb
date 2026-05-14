#pragma once

#include "mkql_block_fuzzer.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <variant>

namespace NKikimr::NMiniKQL {

// Template to count TMaybe nesting levels
template <typename T>
struct TMaybeTraits {
    static constexpr ui32 value = 0;
    using ResultType = T;
};

template <typename T>
struct TMaybeTraits<TMaybe<T>> {
    static constexpr ui32 value = 1 + TMaybeTraits<T>::value;
    using ResultType = TMaybeTraits<T>::ResultType;
};

template <typename T>
const TMaybeTraits<T>::ResultType* GetInnerValue(const T& value) {
    if constexpr (TMaybeTraits<T>::value == 0) {
        return &value;
    } else {
        if (value.Defined()) {
            return GetInnerValue(value.GetRef());
        } else {
            return nullptr;
        }
    }
}

template <typename T>
ui32 GetSettedLevel(const T& value) {
    if constexpr (TMaybeTraits<T>::value == 0) {
        return 0;
    } else {
        if (value.Defined()) {
            return 1 + GetSettedLevel(value.GetRef());
        } else {
            return 0;
        }
    }
}

template <typename T>
struct TUnpackedMaybe {
    ui32 SettedLevel;
    ui32 MaybeLevel;
    const T* Value = nullptr;
};

arrow::Datum ConvertDatumToArrowFormat(arrow::Datum datum, arrow::MemoryPool& pool);

class TSingularVoid {
public:
    TSingularVoid() = default;
};

class TSingularNull {
public:
    TSingularNull() = default;
};

enum class TTag {
    A,
    B,
    C
};

template <typename T, TTag tag>
class TTagged {
public:
    TTagged()
        : Value_()
    {};

    explicit TTagged(T value)
        : Value_(std::move(value))
    {
    }

    const T& Value() const {
        return Value_;
    }

    TStringBuf Tag() const {
        switch (tag) {
            case TTag::A:
                return "A";
            case TTag::B:
                return "B";
            case TTag::C:
                return "C";
        };
    }

private:
    T Value_;
};

class TPgInt {
public:
    TPgInt() = default;
    TPgInt(ui32 value)
        : Value_(value)
    {
    }

    TMaybe<ui32> Value() {
        return Value_;
    }

private:
    TMaybe<ui32> Value_{};
};

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
    TRuntimeNode ConvertValueToLiteralNode(T simpleNode)
        requires(NYql::NUdf::TPrimitiveDataType<T>::Result)
    {
        return Pb_.NewDataLiteral<T>(simpleNode);
    }

    TRuntimeNode ConvertValueToLiteralNode(TSingularVoid simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewVoid();
    }

    TRuntimeNode ConvertValueToLiteralNode(TSingularNull simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewNull();
    }

    TRuntimeNode ConvertValueToLiteralNode(TPgInt simpleNode)
    {
        auto* type = Pb_.NewPgType(NYql::NPg::LookupType("int4").TypeId);
        if (simpleNode.Value()) {
            return Pb_.PgConst(static_cast<TPgType*>(type), std::to_string(*simpleNode.Value()));
        } else {
            return Pb_.Nop(Pb_.NewNull(), type);
        }
    }

    TRuntimeNode ConvertValueToLiteralNode(TStringBuf simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewDataLiteral<NUdf::EDataSlot::String>(simpleNode);
    }

    template <typename T, TTag Tag>
    TRuntimeNode ConvertValueToLiteralNode(const TTagged<T, Tag>& taggedNode) {
        auto node = ConvertValueToLiteralNode(taggedNode.Value());
        return Pb_.Nop(node, Pb_.NewTaggedType(node.GetStaticType(), taggedNode.Tag()));
    }

    template <typename T>
    TRuntimeNode ConvertValueToLiteralNode(const TMaybe<T>& maybeNode) {
        TUnpackedMaybe unpacked{.SettedLevel = GetSettedLevel(maybeNode), .MaybeLevel = TMaybeTraits<TMaybe<T>>::value, .Value = GetInnerValue(maybeNode)};
        decltype(*unpacked.Value) defaultValue{};
        auto data = ConvertValueToLiteralNode(unpacked.Value ? *unpacked.Value : defaultValue);

        for (ui32 i = unpacked.SettedLevel; i < unpacked.MaybeLevel; i++) {
            data = Pb_.NewEmptyOptional(Pb_.NewOptionalType(data.GetStaticType()));
        }

        for (ui32 i = 0; i < unpacked.SettedLevel; i++) {
            data = Pb_.NewOptional(data);
        }

        return data;
    }

    template <typename... TArgs, std::size_t... Is>
    TRuntimeNode ConvertValueToLiteralNodeTuple(const std::tuple<TArgs...>& maybeNode, std::index_sequence<Is...>) {
        auto data = TVector<TRuntimeNode>{ConvertValueToLiteralNode(std::get<Is>(maybeNode))...};
        return Pb_.NewTuple(data);
    }

    template <typename... TArgs>
    TRuntimeNode ConvertValueToLiteralNode(const std::tuple<TArgs...>& node) {
        return ConvertValueToLiteralNodeTuple(node, std::index_sequence_for<TArgs...>{});
    }

    template <typename... Args>
    TRuntimeNode ConvertValueToLiteralNode(const std::variant<Args...>& v) {
        TVector<TType*> types = {ConvertValueToLiteralNode(Args{}).GetStaticType()...};
        TType* varType = Pb_.NewVariantType(Pb_.NewTupleType(types));
        const ui32 idx = static_cast<ui32>(v.index());
        TRuntimeNode result;
        [&]<size_t... Is>(std::index_sequence<Is...>) {
            Y_UNUSED(((Is == idx && (result = Pb_.NewVariant(ConvertValueToLiteralNode(std::get<Is>(v)), idx, varType), true)) || ...));
        }(std::index_sequence_for<Args...>{});
        return result;
    }

    template <typename T>
    TRuntimeNode ConvertNodeWithSpecificFuzzer(const TVector<T>& nodes, ui64 fuzzId) {
        TRuntimeNode::TList convertedNodes;
        for (auto& node : nodes) {
            convertedNodes.push_back(ConvertValueToLiteralNode(node));
        }
        auto* type = ConvertValueToLiteralNode(T{}).GetStaticType();
        return ConvertLiteralListToDatum(std::move(convertedNodes), type, fuzzId);
    }

    template <typename T> requires(!TIsVectorV<T>)
    TRuntimeNode ConvertNodeFuzzied(const T& node) {
        return Pb_.AsScalar(ConvertValueToLiteralNode(node));
    }

    template <typename T>
    TRuntimeNode ConvertNodeFuzzied(const TVector<T>& nodes) {
        ui64 fuzzId = FuzzerHolder_.ReserveFuzzer();
        auto convertedNode = ConvertNodeWithSpecificFuzzer(nodes, fuzzId);
        FuzzerHolder_.CreateFuzzers(TFuzzOptions::FuzzAll(), fuzzId, convertedNode.GetStaticType(), Pb_.GetTypeEnvironment());
        return convertedNode;
    }

    template <typename T> requires(!TIsVectorV<T>)
    TRuntimeNode ConvertNodeUnfuzzied(const T& node) {
        return Pb_.AsScalar(ConvertValueToLiteralNode(node));
    }

    template <typename T>
    TRuntimeNode ConvertNodeUnfuzzied(const TVector<T>& nodes) {
        return ConvertNodeWithSpecificFuzzer(nodes, TFuzzerHolder::EmptyFuzzerId);
    }

    TRuntimeNode ConvertLiteralListToDatum(TRuntimeNode::TList nodes, TType* type, ui64 fuzzId);

    template <typename T, typename U, typename V>
    void TestKernel(const T& left, const U& right, const V& expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp, TMaybe<size_t> iterations = Nothing()) {
        size_t iterationCount =  (TIsVectorV<T> || TIsVectorV<U>) ? ManyIterations : SignleIteration;
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

private:
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

template <TTag Tag, typename T>
TVector<TTagged<T, Tag>> TagVector(const TVector<T>& values) {
    TVector<TTagged<T, Tag>> result;
    result.reserve(values.size());
    for (const auto& v : values) {
        result.emplace_back(v);
    }
    return result;
}

} // namespace NKikimr::NMiniKQL
