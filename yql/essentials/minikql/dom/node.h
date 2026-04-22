#pragma once

#include <yql/essentials/public/udf/udf_value_builder.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <library/cpp/deprecated/enum_codegen/enum_codegen.h>

namespace NYql::NDom {

using namespace NUdf;

constexpr char NodeResourceName[] = "Yson2.Node";

using TPair = std::pair<TUnboxedValue, TUnboxedValue>;

#define YSON_NODE_TYPE_MAP(XX) \
    XX(String, 0)              \
    XX(Bool, 1)                \
    XX(Int64, 2)               \
    XX(Uint64, 3)              \
    XX(Double, 4)              \
    XX(Entity, 5)              \
    XX(List, 6)                \
    XX(Dict, 7)                \
    XX(Attr, 8)

enum class ENodeType: ui8 {
    YSON_NODE_TYPE_MAP(ENUM_VALUE_GEN)
};

// clang-format off
enum class EPrivateNodeType: ui8 {
    YSON_NODE_TYPE_MAP(ENUM_VALUE_GEN)
    StringUtf8 = 9,
};
// clang-format on

inline ENodeType FromPrivateNodeType(EPrivateNodeType type) {
    switch (type) {
        case EPrivateNodeType::String:
            return ENodeType::String;
        case EPrivateNodeType::Bool:
            return ENodeType::Bool;
        case EPrivateNodeType::Int64:
            return ENodeType::Int64;
        case EPrivateNodeType::Uint64:
            return ENodeType::Uint64;
        case EPrivateNodeType::Double:
            return ENodeType::Double;
        case EPrivateNodeType::Entity:
            return ENodeType::Entity;
        case EPrivateNodeType::List:
            return ENodeType::List;
        case EPrivateNodeType::Dict:
            return ENodeType::Dict;
        case EPrivateNodeType::Attr:
            return ENodeType::Attr;
        case EPrivateNodeType::StringUtf8:
            return ENodeType::String;
    }
}

constexpr ui8 NodeTypeShift = 4;
constexpr ui8 NodeTypeMask = 0xf0;

template <ENodeType type>
constexpr inline TUnboxedValuePod SetNodeType(TUnboxedValuePod node) {
    static_assert(type != ENodeType::String, "Use ClearUtf8Mark instead or remove");
    const auto buffer = reinterpret_cast<ui8*>(&node);
    buffer[TUnboxedValuePod::InternalBufferSize] = ui8(type) << NodeTypeShift;
    return node;
}

inline TUnboxedValuePod SetUtf8Mark(TUnboxedValuePod node) {
    const auto buffer = reinterpret_cast<ui8*>(&node);
    buffer[TUnboxedValuePod::InternalBufferSize] |= ui8(EPrivateNodeType::StringUtf8) << NodeTypeShift;
    return node;
}

inline TUnboxedValuePod ClearUtf8Mark(TUnboxedValuePod node) {
    const auto buffer = reinterpret_cast<ui8*>(&node);
    buffer[TUnboxedValuePod::InternalBufferSize] &= ~(ui8(EPrivateNodeType::StringUtf8) << NodeTypeShift);
    return node;
}

inline ENodeType GetNodeType(const TUnboxedValuePod& node) {
    const auto* buffer = reinterpret_cast<const char*>(&node);
    const ui8 flag = (buffer[TUnboxedValuePod::InternalBufferSize] & NodeTypeMask) >> NodeTypeShift;
    return FromPrivateNodeType(static_cast<EPrivateNodeType>(flag));
}

inline bool IsNodeType(const TUnboxedValuePod& node, ENodeType type) {
    return GetNodeType(node) == type;
}

template <ENodeType type>
constexpr inline bool IsNodeType(const TUnboxedValuePod node) {
    return IsNodeType(node, type);
}

inline bool IsUtf8Node(const TUnboxedValuePod& node) {
    const auto* buffer = reinterpret_cast<const char*>(&node);
    const ui8 flag = (buffer[TUnboxedValuePod::InternalBufferSize] & NodeTypeMask) >> NodeTypeShift;
    return static_cast<EPrivateNodeType>(flag) == EPrivateNodeType::StringUtf8;
}

class TMapNode: public TManagedBoxedValue {
public:
    template <bool NoSwap>
    class TIterator: public TManagedBoxedValue {
    public:
        explicit TIterator(const TMapNode* parent);

    private:
        bool Skip() final;
        bool Next(TUnboxedValue& key) final;
        bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) final;

        const TRefCountedPtr<TMapNode> Parent_;
        ui32 Index_;
    };

    TMapNode(const TPair* items, ui32 count);

    TMapNode(TMapNode&& src);

    ~TMapNode() override;

    TUnboxedValue Lookup(const TStringRef& key) const;

private:
    ui64 GetDictLength() const final;

    TUnboxedValue GetDictIterator() const final;

    TUnboxedValue GetKeysIterator() const final;

    TUnboxedValue GetPayloadsIterator() const final;

    bool Contains(const TUnboxedValuePod& key) const final;

    TUnboxedValue Lookup(const TUnboxedValuePod& key) const final;

    bool HasDictItems() const final;

    bool IsSortedDict() const final;

    TStringRef GetResourceTag() const final;

    void* GetResource() final;

    ui32 Count_;
    ui32 UniqueCount_;
    TPair* Items_;
};

class TAttrNode: public TMapNode {
public:
    TAttrNode(const TUnboxedValue& map, NUdf::TUnboxedValue&& value);

    TAttrNode(NUdf::TUnboxedValue&& value, const TPair* items, ui32 count);

    NUdf::TUnboxedValue GetVariantItem() const final;

private:
    const NUdf::TUnboxedValue Value_;
};

inline TUnboxedValuePod MakeAttr(TUnboxedValue&& value, TPair* items, ui32 count) {
    if (count == 0) {
        return value.Release();
    }

    return SetNodeType<ENodeType::Attr>(TUnboxedValuePod(new TAttrNode(std::move(value), items, count)));
}

inline TUnboxedValuePod MakeString(const TStringBuf value, const IValueBuilder* valueBuilder) {
    return valueBuilder->NewString(value).Release();
}

inline TUnboxedValuePod MakeBool(bool value) {
    return SetNodeType<ENodeType::Bool>(TUnboxedValuePod(value));
}

inline TUnboxedValuePod MakeInt64(i64 value) {
    return SetNodeType<ENodeType::Int64>(TUnboxedValuePod(value));
}

inline TUnboxedValuePod MakeUint64(ui64 value) {
    return SetNodeType<ENodeType::Uint64>(TUnboxedValuePod(value));
}

inline TUnboxedValuePod MakeDouble(double value) {
    return SetNodeType<ENodeType::Double>(TUnboxedValuePod(value));
}

inline TUnboxedValuePod MakeEntity() {
    return SetNodeType<ENodeType::Entity>(TUnboxedValuePod::Zero());
}

inline TUnboxedValuePod MakeList(TUnboxedValue* items, ui32 count, const IValueBuilder* valueBuilder) {
    return SetNodeType<ENodeType::List>(count > 0U ? valueBuilder->NewList(items, count).Release() : TUnboxedValuePod::Zero());
}

inline TUnboxedValuePod MakeDict(const TPair* items, ui32 count) {
    return SetNodeType<ENodeType::Dict>(count > 0U ? TUnboxedValuePod(new TMapNode(items, count)) : TUnboxedValuePod::Zero());
}

struct TDebugPrinter {
    explicit TDebugPrinter(const TUnboxedValuePod& node);
    class IOutputStream& Out(class IOutputStream& o) const;
    const TUnboxedValuePod& Node;
};

} // namespace NYql::NDom

template <>
inline void Out<NYql::NDom::TDebugPrinter>(class IOutputStream& o, const NYql::NDom::TDebugPrinter& p) {
    p.Out(o);
}
