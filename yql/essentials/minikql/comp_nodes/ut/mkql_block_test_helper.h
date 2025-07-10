#pragma once

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

class TBlockHelper {
public:
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
    TRuntimeNode ConvertNode(std::vector<T> nodes) {
        TRuntimeNode::TList convertedNodes;
        for (auto& node : nodes) {
            convertedNodes.push_back(ConvertValueToLiteralNode(node));
        }
        return ConvertLiteralListToDatum(convertedNodes);
    }

    TRuntimeNode ConvertLiteralListToDatum(TRuntimeNode::TList nodes);

    template <typename T, typename U, typename V>
    void TestKernel(T left, U right, V expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode, TRuntimeNode)> binaryOp) {
        NYql::TExprContext exprCtx;
        auto leftNode = ConvertNode(left);
        auto rightNode = ConvertNode(right);
        auto expectedNode = ConvertNode(expected);

        auto resultValue = Setup_.BuildGraph(binaryOp(Setup_, leftNode, rightNode))->GetValue();
        auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();

        auto outDatum = TArrowBlock::From(resultValue).GetDatum();
        auto expectedDatum = TArrowBlock::From(expectedValue).GetDatum();

        UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << DatumToString(expectedDatum) << "\n but got : " << DatumToString(outDatum));
    }

    template <typename T, typename V>
    void TestKernel(T operand, V expected, std::function<TRuntimeNode(TSetup<false>&, TRuntimeNode)> unaryOp) {
        NYql::TExprContext exprCtx;
        auto node = ConvertNode(operand);
        auto expectedNode = ConvertNode(expected);

        auto resultValue = Setup_.BuildGraph(unaryOp(Setup_, node))->GetValue();
        auto expectedValue = Setup_.BuildGraph(expectedNode)->GetValue();

        auto outDatum = TArrowBlock::From(resultValue).GetDatum();
        auto expectedDatum = TArrowBlock::From(expectedValue).GetDatum();

        UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << DatumToString(expectedDatum) << "\n but got : " << DatumToString(outDatum));
    }

private:
    TComputationNodeFactory GetNodeTestFactory();

    TString DatumToString(arrow::Datum datum);

    TRuntimeNode MaterializeBlockStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream);

    TSetup<false> Setup_;
    TProgramBuilder& Pb_;
};

} // namespace NKikimr::NMiniKQL
