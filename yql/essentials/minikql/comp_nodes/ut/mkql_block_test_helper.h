#pragma once

#include "mkql_block_fuzzer.h"

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

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

// Template to get the value of the deepest set TMaybe or return 0
template <typename T>
TMaybeTraits<T>::ResultType GetInnerValue(const T& value) {
    if constexpr (TMaybeTraits<T>::value == 0) {
        // Base case: not a TMaybe type, return the value itself
        return value;
    } else {
        // TMaybe type
        if (value.Defined()) {
            return GetInnerValue(value.GetRef());
        } else {
            return {}; // Return 0 if not set
        }
    }
}

// Template to get the level of the set TMaybe (returns 0 if not set)
template <typename T>
ui32 GetSettedLevel(const T& value) {
    if constexpr (TMaybeTraits<T>::value == 0) {
        // Base case: not a TMaybe type, always set at level 0
        return 0;
    } else {
        // TMaybe type
        if (value.Defined()) {
            return 1 + GetSettedLevel(value.GetRef());
        } else {
            return 0; // Return 0 if not set
        }
    }
}

template <typename T>
struct TUnpackedMaybe {
    ui32 SettedLevel;
    ui32 MaybeLevel;
    T Value;
};

template <typename T>
struct TIsPairWithFuzz: std::false_type {};

template <typename T, typename U>
struct TIsPairWithFuzz<std::pair<T, U>>
    : std::bool_constant<std::is_same_v<std::remove_cv_t<U>, TFuzzOptions>> {};

template <typename U>
inline constexpr bool IsPairWithFuzz =
    TIsPairWithFuzz<std::remove_cvref_t<U>>::value;

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

    TTagged(T value)
        : Value_(std::move(value))
    {
    }

    T Value() {
        return Value_;
    }

    TString Tag() {
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
struct TIsStdVector: std::false_type {};

template <class T, class Alloc>
struct TIsStdVector<std::vector<T, Alloc>>: std::true_type {};

template <class T>
constexpr bool TIsStdVectorV = TIsStdVector<std::remove_cvref_t<T>>::value;

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
    TRuntimeNode ConvertValueToLiteralNode(T node) = delete;

    template <typename T>
    TRuntimeNode ConvertValueToLiteralNode(T simpleNode)
        requires(NYql::NUdf::TPrimitiveDataType<T>::Result)
    {
        return Pb_.NewDataLiteral<T>(simpleNode);
    }

    template <>
    TRuntimeNode ConvertValueToLiteralNode(TSingularVoid simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewVoid();
    }

    template <>
    TRuntimeNode ConvertValueToLiteralNode(TSingularNull simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewNull();
    }

    template <>
    TRuntimeNode ConvertValueToLiteralNode(TPgInt simpleNode)
    {
        auto* type = Pb_.NewPgType(NYql::NPg::LookupType("int4").TypeId);
        if (simpleNode.Value()) {
            return Pb_.PgConst(static_cast<TPgType*>(type), std::to_string(*simpleNode.Value()));
        } else {
            return Pb_.Nop(Pb_.NewNull(), type);
        }
    }

    template <>
    TRuntimeNode ConvertValueToLiteralNode(TString simpleNode)
    {
        Y_UNUSED(simpleNode);
        return Pb_.NewDataLiteral<NUdf::EDataSlot::String>(simpleNode);
    }

    template <typename T, TTag tag>
    TRuntimeNode ConvertValueToLiteralNode(TTagged<T, tag> taggedNode) {
        auto node = ConvertValueToLiteralNode(taggedNode.Value());
        return Pb_.Nop(node, Pb_.NewTaggedType(node.GetStaticType(), taggedNode.Tag()));
    }

    template <typename T>
    TRuntimeNode ConvertValueToLiteralNode(TMaybe<T> maybeNode) {
        TUnpackedMaybe unpacked{.SettedLevel = GetSettedLevel(maybeNode), .MaybeLevel = TMaybeTraits<TMaybe<T>>::value, .Value = GetInnerValue(maybeNode)};
        auto data = ConvertValueToLiteralNode(unpacked.Value);

        for (ui32 i = unpacked.SettedLevel; i < unpacked.MaybeLevel; i++) {
            data = Pb_.NewEmptyOptional(Pb_.NewOptionalType(data.GetStaticType()));
        }

        for (ui32 i = 0; i < unpacked.SettedLevel; i++) {
            data = Pb_.NewOptional(data);
        }

        return data;
    }

    template <typename... TArgs, std::size_t... Is>
    TRuntimeNode ConvertValueToLiteralNodeTuple(std::tuple<TArgs...> maybeNode, std::index_sequence<Is...>) {
        auto data = std::vector<TRuntimeNode>{ConvertValueToLiteralNode(std::get<Is>(maybeNode))...};
        return Pb_.NewTuple(data);
    }

    template <typename... TArgs>
    TRuntimeNode ConvertValueToLiteralNode(std::tuple<TArgs...> node) {
        return ConvertValueToLiteralNodeTuple(node, std::index_sequence_for<TArgs...>{});
    }

    template <typename T>
    TRuntimeNode ConvertNode(T node) {
        return Pb_.AsScalar(ConvertValueToLiteralNode(node));
    }

    template <typename T>
    TRuntimeNode ConvertNode(std::vector<T> nodes, ui64 fuzzId = TFuzzerHolder::EmptyFuzzerId) {
        TRuntimeNode::TList convertedNodes;
        for (auto& node : nodes) {
            convertedNodes.push_back(ConvertValueToLiteralNode(node));
        }
        return ConvertLiteralListToDatum(convertedNodes, fuzzId);
    }

    template <typename T>
    TRuntimeNode ConvertNode(std::pair<std::vector<T>, TFuzzOptions> nodes) {
        ui64 fuzzId = FuzzerHolder_.ReserveFuzzer();
        auto convertedNode = ConvertNode(nodes.first, fuzzId);
        FuzzerHolder_.CreateFuzzers(nodes.second, fuzzId, convertedNode.GetStaticType(), Pb_.GetTypeEnvironment());
        return convertedNode;
    }

    TRuntimeNode ConvertLiteralListToDatum(TRuntimeNode::TList nodes, ui64 fuzzId);

    template <typename T, typename U, typename V>
    void TestKernel(T left, U right, V expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp) {
        size_t iterationCount = (IsPairWithFuzz<T> || IsPairWithFuzz<U>) ? ManyIterations : SignleIteration;
        static_assert(!IsPairWithFuzz<V>, "Expected data is not allowed to be fuzzied.");
        for (size_t i = 0; i < iterationCount; i++) {
            NYql::TExprContext exprCtx;
            auto leftNode = ConvertNode(left);
            auto rightNode = ConvertNode(right);
            auto expectedNode = ConvertNode(expected);

            auto resultValue = Setup_.BuildGraph(binaryOp(Setup_, leftNode, rightNode))->GetValue();
            auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();
            auto outDatum = ConvertDatumToArrowFormat(TArrowBlock::From(resultValue).GetDatum(), *arrow::default_memory_pool());
            auto expectedDatum = ConvertDatumToArrowFormat(TArrowBlock::From(expectedValue).GetDatum(), *arrow::default_memory_pool());

            UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << DatumToString(expectedDatum) << "\n but got : " << DatumToString(outDatum));
        }
    }

    template <typename T, typename V>
    void TestKernel(T operand, V expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp) {
        size_t iterationCount = (IsPairWithFuzz<T>) ? ManyIterations : SignleIteration;
        static_assert(!IsPairWithFuzz<V>, "Expected data is not allowed to be fuzzied.");
        for (size_t i = 0; i < iterationCount; i++) {
            NYql::TExprContext exprCtx;
            auto node = ConvertNode(operand);
            auto expectedNode = ConvertNode(expected);

            auto resultValue = Setup_.BuildGraph(unaryOp(Setup_, node))->GetValue();
            auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();

            auto outDatum = ConvertDatumToArrowFormat(TArrowBlock::From(resultValue).GetDatum(), *arrow::default_memory_pool());
            auto expectedDatum = ConvertDatumToArrowFormat(TArrowBlock::From(expectedValue).GetDatum(), *arrow::default_memory_pool());

            UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "\nExpected : " << DatumToString(expectedDatum) << "\nBut got : " << DatumToString(outDatum));
        }
    }

    template <typename T, typename V>
    void TestKernelFuzzied(std::vector<T> operand, std::vector<V> expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp) {
        MKQL_ENSURE(operand.size() == expected.size(), "Size mismatch.");
        // Test fuzzied vectors.
        TestKernel(std::pair{operand, TFuzzOptions::FuzzAll()}, expected, unaryOp);
        for (size_t i = 0; i < operand.size(); i++) {
            // Test subsequent scalars.
            TestKernel(operand[i], expected[i], unaryOp);
        }
    }

    template <typename T, typename U, typename V>
    void TestKernelFuzzied(T left, U right, V expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp) {
        if constexpr (TIsStdVectorV<T> && TIsStdVectorV<U>) {
            MKQL_ENSURE(left.size() == right.size(), "Size mismatch.");
            MKQL_ENSURE(left.size() == expected.size(), "Size mismatch.");
        }

        if constexpr (TIsStdVectorV<T> && TIsStdVectorV<U>) {
            TestKernel(std::pair{left, TFuzzOptions::FuzzAll()}, std::pair{right, TFuzzOptions::FuzzAll()}, expected, binaryOp);
            for (size_t i = 0; i < left.size(); i++) {
                // Test subsequent scalars.
                TestKernel(left[i], right[i], expected[i], binaryOp);
            }
        } else if constexpr (TIsStdVectorV<T>) {
            TestKernel(std::pair{left, TFuzzOptions::FuzzAll()}, right, expected, binaryOp);
            for (size_t i = 0; i < left.size(); i++) {
                // Test subsequent scalars.
                TestKernel(left[i], right, expected[i], binaryOp);
            }
        } else if constexpr (TIsStdVectorV<U>) {
            TestKernel(left, std::pair{right, TFuzzOptions::FuzzAll()}, expected, binaryOp);
            for (size_t i = 0; i < right.size(); i++) {
                // Test subsequent scalars.
                TestKernel(left, right[i], expected[i], binaryOp);
            }
        } else {
            TestKernel(left, right, expected, binaryOp);
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
std::vector<std::tuple<Ts...>> TupleZip(const std::vector<Ts>&... vecs) {
    const auto& first = std::get<0>(std::tie(vecs...));
    const std::size_t n = first.size();
    MKQL_ENSURE(((vecs.size() == n) && ...), "Vectors must have same size.");
    std::vector<std::tuple<Ts...>> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        out.emplace_back(vecs[i]...);
    }
    return out;
}

template <TTag Tag, typename T>
std::vector<TTagged<T, Tag>> TagVector(const std::vector<T>& values) {
    std::vector<TTagged<T, Tag>> result;
    result.reserve(values.size());
    for (const auto& v : values) {
        result.emplace_back(v);
    }
    return result;
}

} // namespace NKikimr::NMiniKQL
