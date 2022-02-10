#pragma once

#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>

namespace NYql::NDom {

using namespace NUdf;

constexpr char NodeResourceName[] = "Yson2.Node";

using TPair = std::pair<TUnboxedValue, TUnboxedValue>;

enum class ENodeType : ui8 {
    String = 0,
    Bool = 1,
    Int64 = 2,
    Uint64 = 3,
    Double = 4,
    Entity = 5,
    List = 6,
    Dict = 7,
    Attr = 8,
};

constexpr ui8 NodeTypeShift = 4;
constexpr ui8 NodeTypeMask = 0xf0;

template<ENodeType type>
constexpr inline TUnboxedValuePod SetNodeType(TUnboxedValuePod node) {
    const auto buffer = reinterpret_cast<ui8*>(&node);
    buffer[TUnboxedValuePod::InternalBufferSize] = ui8(type) << NodeTypeShift;
    return node;
}

template<ENodeType type>
constexpr inline bool IsNodeType(const TUnboxedValuePod node) {
    const auto buffer = reinterpret_cast<const ui8*>(&node);
    const auto currentMask = buffer[TUnboxedValuePod::InternalBufferSize] & NodeTypeMask;
    constexpr ui8 expectedMask = static_cast<ui8>(type) << NodeTypeShift;
    return currentMask == expectedMask;
}

inline ENodeType GetNodeType(const TUnboxedValuePod& node) {
    const auto* buffer = reinterpret_cast<const char*>(&node);
    const ui8 flag = (buffer[TUnboxedValuePod::InternalBufferSize] & NodeTypeMask) >> NodeTypeShift;
    return static_cast<ENodeType>(flag);
}

inline bool IsNodeType(const TUnboxedValuePod& node, ENodeType type) {
    const auto* buffer = reinterpret_cast<const char*>(&node);
    const ui8 currentMask = buffer[TUnboxedValuePod::InternalBufferSize] & NodeTypeMask;
    const ui8 expectedMask = static_cast<ui8>(type) << NodeTypeShift;
    return currentMask == expectedMask;
}

class TMapNode : public TManagedBoxedValue {
public:
    template <bool NoSwap>
    class TIterator: public TManagedBoxedValue {
    public:
        TIterator(const TMapNode* parent);

    private:
        bool Skip() final;
        bool Next(TUnboxedValue& key) final;
        bool NextPair(TUnboxedValue& key, TUnboxedValue& payload) final;

        const TRefCountedPtr<TMapNode> Parent;
        ui32 Index;
    };

    TMapNode(const TPair* items, ui32 count);

    TMapNode(TMapNode&& src);

    ~TMapNode();

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

    void* GetResource() final;

    ui32 Count_;
    ui32 UniqueCount_;
    TPair * Items_;
};

class TAttrNode : public TMapNode {
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
    TDebugPrinter(const TUnboxedValuePod& node);
    class IOutputStream& Out(class IOutputStream &o) const;
    const TUnboxedValuePod& Node;
};

}

template<>
inline void Out<NYql::NDom::TDebugPrinter>(class IOutputStream &o, const NYql::NDom::TDebugPrinter& p) {
    p.Out(o);
}
