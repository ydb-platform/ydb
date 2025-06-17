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

template<typename T>
struct TUnpackedMaybe {
    ui32 SettedLevel;
    ui32 MaybeLevel;
    T Value;
};

template <typename T>
TRuntimeNode ConvertMaybeToNode(TUnpackedMaybe<T> maybe, TSetup<false>& setup) {
    auto data = setup.PgmBuilder->NewDataLiteral<T>(maybe.Value);

    for (ui32 i = maybe.SettedLevel; i < maybe.MaybeLevel; i++) {
        data = setup.PgmBuilder->NewEmptyOptional(setup.PgmBuilder->NewOptionalType(data.GetStaticType()));
    }

    for (ui32 i = 0; i < maybe.SettedLevel; i++) {
        data = setup.PgmBuilder->NewOptional(data);
    }

    data = setup.PgmBuilder->AsScalar(data);
    return data;
}

template <typename T, typename U, typename V>
void TestScalarKernel(T left, U right, V expected, TSetup<false>& setup, std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)> binaryOp) {
    NYql::TExprContext exprCtx;
    TUnpackedMaybe leftUnpacked{.SettedLevel = GetSettedLevel(left), .MaybeLevel = TMaybeTraits<T>::value, .Value = GetInnerValue(left)};
    TUnpackedMaybe rightUnpacked{.SettedLevel = GetSettedLevel(right), .MaybeLevel = TMaybeTraits<U>::value, .Value = GetInnerValue(right)};
    TUnpackedMaybe expectedUnpacked{.SettedLevel = GetSettedLevel(expected), .MaybeLevel = TMaybeTraits<V>::value, .Value = GetInnerValue(expected)};
    auto leftNode = ConvertMaybeToNode(leftUnpacked, setup);
    auto rightNode = ConvertMaybeToNode(rightUnpacked, setup);
    auto expectedNode = ConvertMaybeToNode(expectedUnpacked, setup);

    auto resultValue = setup.BuildGraph(binaryOp(leftNode, rightNode))->GetValue();
    auto expectedValue = setup.BuildGraph(expectedNode)->GetValue();

    auto outDatum = TArrowBlock::From(resultValue).GetDatum();
    auto expectedDatum = TArrowBlock::From(expectedValue).GetDatum();

    UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << outDatum.scalar()->ToString() << "\n but got : " << expectedDatum.scalar()->ToString());
}

template <typename T, typename V>
void TestScalarKernel(T left, V expected, TSetup<false>& setup, std::function<TRuntimeNode(TRuntimeNode)> unaryOp) {
    NYql::TExprContext exprCtx;
    TUnpackedMaybe unpacked{.SettedLevel = GetSettedLevel(left), .MaybeLevel = TMaybeTraits<T>::value, .Value = GetInnerValue(left)};
    TUnpackedMaybe expectedUnpacked{.SettedLevel = GetSettedLevel(expected), .MaybeLevel = TMaybeTraits<V>::value, .Value = GetInnerValue(expected)};
    auto node = ConvertMaybeToNode(unpacked, setup);
    auto expectedNode = ConvertMaybeToNode(expectedUnpacked, setup);

    auto resultValue = setup.BuildGraph(unaryOp(node))->GetValue();
    auto expectedValue = setup.BuildGraph(expectedNode)->GetValue();

    auto outDatum = TArrowBlock::From(resultValue).GetDatum();
    auto expectedDatum = TArrowBlock::From(expectedValue).GetDatum();

    UNIT_ASSERT_EQUAL_C(outDatum, expectedDatum, "Expected : " << outDatum.scalar()->ToString() << "\n but got : " << expectedDatum.scalar()->ToString());
}

} // namespace NKikimr::NMiniKQL
