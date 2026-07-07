#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL::NTest {

using NYql::NUdf::NTest::TStructMember;
using NYql::NUdf::NTest::TStructType;

namespace NPrivate {

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

} // namespace NPrivate

// Forward declarations so all overloads are visible to each other's template bodies.
template <typename T>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TMaybe<T>& maybeNode);
template <typename... TArgs>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const std::tuple<TArgs...>& node);
template <typename... TArgs>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TStructType<TArgs...>& node);
template <typename... Args>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const std::variant<Args...>& v);
template <typename T>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TVector<T>& nodes);

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

    explicit TPgInt(i32 value)
        : Value_(value)
    {
    }

    TMaybe<i32> Value() const {
        return Value_;
    }

private:
    TMaybe<i32> Value_{};
};

struct TUtf8 {
    TStringBuf Value;
};

template <typename T>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, T simpleNode)
    requires(NYql::NUdf::TPrimitiveDataType<T>::Result)
{
    return pb.NewDataLiteral<T>(simpleNode);
}

inline TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, TSingularVoid simpleNode)
{
    Y_UNUSED(simpleNode);
    return pb.NewVoid();
}

inline TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, TSingularNull simpleNode)
{
    Y_UNUSED(simpleNode);
    return pb.NewNull();
}

inline TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, TPgInt simpleNode)
{
    auto* type = pb.NewPgType(NYql::NPg::LookupType("int4").TypeId);
    if (simpleNode.Value()) {
        return pb.PgConst(static_cast<TPgType*>(type), std::to_string(*simpleNode.Value()));
    } else {
        return pb.Nop(pb.NewNull(), type);
    }
}

inline TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, TStringBuf simpleNode)
{
    Y_UNUSED(simpleNode);
    return pb.NewDataLiteral<NUdf::EDataSlot::String>(simpleNode);
}

inline TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TUtf8& utf8Node) {
    return pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(utf8Node.Value);
}

template <ui8 Precision, ui8 Scale>
struct TDecimalLiteral {
    NYql::NDecimal::TInt128 Value{};
};

template <ui8 Precision, ui8 Scale>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TDecimalLiteral<Precision, Scale>& decimalNode) {
    return pb.NewDecimalLiteral(decimalNode.Value, Precision, Scale);
}

template <typename T, TTag Tag>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TTagged<T, Tag>& taggedNode) {
    auto node = ConvertValueToLiteralNode(pb, taggedNode.Value());
    return pb.Nop(node, pb.NewTaggedType(node.GetStaticType(), taggedNode.Tag()));
}

template <typename T>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TMaybe<T>& maybeNode) {
    NPrivate::TUnpackedMaybe unpacked{.SettedLevel = NPrivate::GetSettedLevel(maybeNode), .MaybeLevel = NPrivate::TMaybeTraits<TMaybe<T>>::value, .Value = NPrivate::GetInnerValue(maybeNode)};
    decltype(*unpacked.Value) defaultValue{};
    auto data = ConvertValueToLiteralNode(pb, unpacked.Value ? *unpacked.Value : defaultValue);

    for (ui32 i = unpacked.SettedLevel; i < unpacked.MaybeLevel; i++) {
        data = pb.NewEmptyOptional(pb.NewOptionalType(data.GetStaticType()));
    }

    for (ui32 i = 0; i < unpacked.SettedLevel; i++) {
        data = pb.NewOptional(data);
    }

    return data;
}

template <typename... TArgs, std::size_t... Is>
TRuntimeNode ConvertValueToLiteralNodeTuple(TProgramBuilder& pb, const std::tuple<TArgs...>& maybeNode, std::index_sequence<Is...>) {
    auto data = TVector<TRuntimeNode>{ConvertValueToLiteralNode(pb, std::get<Is>(maybeNode))...};
    return pb.NewTuple(data);
}

template <typename... TArgs, std::size_t... Is>
TRuntimeNode ConvertValueToLiteralNodeStruct(TProgramBuilder& pb, const TStructType<TArgs...>& structNode, std::index_sequence<Is...>) {
    TVector<std::pair<std::string_view, TRuntimeNode>> members = {
        {std::tuple_element_t<Is, std::tuple<TArgs...>>::MemberName(),
         ConvertValueToLiteralNode(pb, std::get<Is>(structNode.Members).Value)}...};
    return pb.NewStruct(members);
}

template <typename... TArgs>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const std::tuple<TArgs...>& node) {
    return ConvertValueToLiteralNodeTuple(pb, node, std::index_sequence_for<TArgs...>{});
}

template <typename... TArgs>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TStructType<TArgs...>& node) {
    return ConvertValueToLiteralNodeStruct(pb, node, std::index_sequence_for<TArgs...>{});
}

template <typename... Args>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const std::variant<Args...>& v) {
    TVector<TType*> types = {ConvertValueToLiteralNode(pb, Args{}).GetStaticType()...};
    TType* varType = pb.NewVariantType(pb.NewTupleType(types));
    const ui32 idx = static_cast<ui32>(v.index());
    TRuntimeNode result;
    [&]<size_t... Is>(std::index_sequence<Is...>) {
        Y_UNUSED(((Is == idx && (result = pb.NewVariant(ConvertValueToLiteralNode(pb, std::get<Is>(v)), idx, varType), true)) || ...));
    }(std::index_sequence_for<Args...>{});
    return result;
}

template <typename T>
TType* ConvertToMinikqlType(TProgramBuilder& pb) {
    return ConvertValueToLiteralNode(pb, T{}).GetStaticType();
}

template <typename T>
TRuntimeNode ConvertValueToLiteralNode(TProgramBuilder& pb, const TVector<T>& nodes) {
    TRuntimeNode::TList convertedNodes;
    convertedNodes.reserve(nodes.size());
    for (const auto& node : nodes) {
        convertedNodes.push_back(ConvertValueToLiteralNode(pb, node));
    }
    TType* const type = nodes.empty()
                            ? ConvertToMinikqlType<T>(pb)
                            : convertedNodes.front().GetStaticType();
    return pb.NewList(type, std::move(convertedNodes));
}

} // namespace NKikimr::NMiniKQL::NTest

namespace NYql::NUdf::NPrivate {

template <>
struct TUnboxedValueComparator<NKikimr::NMiniKQL::NTest::TUtf8> {
    template <CComparatorUtilsUdfValue THolder>
    static TUnboxedValueComparatorResult IsEqual(const THolder& value, const NKikimr::NMiniKQL::NTest::TUtf8& expected) {
        const TStringBuf got(value.AsStringRef());
        if (got != expected.Value) {
            return std::unexpected(TStringBuilder() << "Expected utf8 string \"" << expected.Value << "\" but got \"" << got << "\"");
        }
        return {};
    }
};

} // namespace NYql::NUdf::NPrivate
