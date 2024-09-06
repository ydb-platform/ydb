#ifndef POLYMORPHIC_YSON_STRUCT_INL_H_
#error "Direct inclusion of this file is not allowed, include polymorphic_yson_struct.h"
// For the sake of sane code completion.
#include "polymorphic_yson_struct.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TEnum, TEnum Value, class TBase, class TDerived>
TIntrusivePtr<TBase> TMappingLeaf<TEnum, Value, TBase, TDerived>::CreateInstance()
{
    return New<TDerived>();
}

template <class TEnum, TEnum BaseValue, CYsonStructDerived TBase, TEnum... Values, class... TDerived>
    requires (CHierarchy<TBase, TDerived...>)
TIntrusivePtr<TBase>
TPolymorphicMapping<TEnum, TLeafTag<BaseValue, TBase>, TLeafTag<Values, TDerived>...>::
CreateInstance(TEnum value)
{
    if (value == BaseValue) {
        return TLeaf<BaseValue, TBase>::CreateInstance();
    }

    TIntrusivePtr<TBase> ret;

    ([&ret, value] {
        if (value == Values) {
            ret = TLeaf<Values, TDerived>::CreateInstance();
            return false;
        }
        return true;
    } () && ...);

    return ret;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <CPolymorphicEnumMapping TMapping>
TPolymorphicYsonStruct<TMapping>::TPolymorphicYsonStruct(TKey key)
    : TPolymorphicYsonStruct(key, TMapping::CreateInstance(key))
{ }

template <CPolymorphicEnumMapping TMapping>
TPolymorphicYsonStruct<TMapping>::TPolymorphicYsonStruct(TKey key, TIntrusivePtr<TBase> ptr) noexcept
    : Storage_(std::move(ptr))
    , HeldType_(key)
{
    YT_VERIFY(Storage_);
}

template <CPolymorphicEnumMapping TMapping>
template <CYsonStructSource TSource>
void TPolymorphicYsonStruct<TMapping>::Load(
    TSource source,
    bool postprocess,
    bool setDefaults,
    const NYPath::TYPath& path,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    using TTraits = NPrivate::TYsonSourceTraits<TSource>;

    // TODO(arkady-e1ppa): Support parsing without node
    // conversion smth? FillMap wont work since we
    // do not know the value_type of a map we would
    // parse (unless we want to slice which we don't).
    IMapNodePtr map = TTraits::AsNode(source)->AsMap();

    if (!map || map->GetChildCount() == 0) {
        // Empty struct.
        return;
    }

    auto key = map->FindChildValue<TKey>("type");
    THROW_ERROR_EXCEPTION_UNLESS(
        key.has_value(),
        "Concrete type must be specified! Use \"type\": \"concrete_type\"");

    auto type = *key;
    if (!Storage_ || HeldType_ != type) {
        // NB: We will try to merge configs if types match.
        HeldType_ = type;
        Storage_ = TMapping::CreateInstance(HeldType_);
    }

    if (recursiveUnrecognizedStrategy) {
        Storage_->SetUnrecognizedStrategy(*recursiveUnrecognizedStrategy);
    }

    // "type" must be unrecognized for the original struct
    // therefore we must delete it prior to |Load| call.
    map->RemoveChild("type");
    Storage_->Load(map, postprocess, setDefaults, path);

    // NB(arkady-e1ppa): We must not actually remove contents of the node as a postcondition
    // since it mutates serialized data which might be used for config validation.
    map->AddChild("type", ConvertToNode(HeldType_));
}

template <CPolymorphicEnumMapping TMapping>
void TPolymorphicYsonStruct<TMapping>::Save(NYson::IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();

    if (Storage_) {
        consumer->OnKeyedItem("type");
        consumer->OnStringScalar(FormatEnum(HeldType_));

        Storage_->SaveAsMapFragment(consumer);
    }

    consumer->OnEndMap();
}

template <CPolymorphicEnumMapping TMapping>
template <std::derived_from<typename TMapping::TBaseClass> TConcrete>
TIntrusivePtr<TConcrete> TPolymorphicYsonStruct<TMapping>::TryGetConcrete() const
{
    return DynamicPointerCast<TConcrete>(Storage_);
}

template <CPolymorphicEnumMapping TMapping>
template <typename TMapping::TKey Value>
TIntrusivePtr<typename TMapping::template TDerivedToEnum<Value>> TPolymorphicYsonStruct<TMapping>::TryGetConcrete() const
{
    if (Value != HeldType_) {
        return {};
    }
    return TryGetConcrete<typename TMapping::template TDerivedToEnum<Value>>();
}

template <CPolymorphicEnumMapping TMapping>
typename TPolymorphicYsonStruct<TMapping>::TKey TPolymorphicYsonStruct<TMapping>::GetCurrentType() const
{
    return HeldType_;
}

template <CPolymorphicEnumMapping TMapping>
typename TPolymorphicYsonStruct<TMapping>::TBase* TPolymorphicYsonStruct<TMapping>::operator->()
{
    return Storage_.Get();
}

template <CPolymorphicEnumMapping TMapping>
const typename TPolymorphicYsonStruct<TMapping>::TBase* TPolymorphicYsonStruct<TMapping>::operator->() const
{
    return Storage_.Get();
}

////////////////////////////////////////////////////////////////////////////////

template <CPolymorphicEnumMapping TMapping>
void Serialize(const TPolymorphicYsonStruct<TMapping>& value, NYson::IYsonConsumer* consumer)
{
    value.Save(consumer);
}

template <CPolymorphicEnumMapping TMapping, CYsonStructSource TSource>
void Deserialize(TPolymorphicYsonStruct<TMapping>& value, TSource source)
{
    value.Load(std::move(source));
}

////////////////////////////////////////////////////////////////////////////////

#undef DEFINE_POLYMORPHIC_YSON_STRUCT
#undef DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ_ELEM(item) \
    PP_LEFT_PARENTHESIS PP_ELEMENT(item, 0) PP_RIGHT_PARENTHESIS

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ(seq) \
    PP_FOR_EACH(POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ_ELEM, seq)

#define POLYMORPHIC_YSON_STRUCT_IMPL__ENUM_NAME(Struct) \
    E##Struct##Type

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM(seq) \
    DEFINE_ENUM(EType, POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ(seq))

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM_ALIAS(EnumName) \
    using EType = EnumName;

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_CLASS_ELEM(item) \
    PP_COMMA() PP_ELEMENT(item, 1)

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_LEAF_FROM_ETYPE(item) \
    PP_COMMA() ::NYT::NYTree::NDetail::TLeafTag<EType:: PP_ELEMENT(item, 0), PP_ELEMENT(item, 1)>

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_CLASS(Struct, seq) \
    using TMapping = ::NYT::NYTree::TPolymorphicEnumMapping<EType PP_FOR_EACH(POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_LEAF_FROM_ETYPE, seq)>

#define DEFINE_POLYMORPHIC_YSON_STRUCT(name, seq) \
namespace NPolymorphicYsonStructFor##name { \
 \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM(seq); \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_CLASS(name, seq); \
} /*NPolymorphicYsonStructFor##name*/ \
using POLYMORPHIC_YSON_STRUCT_IMPL__ENUM_NAME(name) = NPolymorphicYsonStructFor##name::EType; \
using T##name = ::NYT::NYTree::TPolymorphicYsonStruct<NPolymorphicYsonStructFor##name::TMapping>; \
static_assert(true)

#define DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM(name, enum, seq) \
namespace NPolymorphicYsonStructFor##name { \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM_ALIAS(enum); \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_CLASS(name, seq); \
} /*NPolymorphicYsonStructFor##name*/ \
using T##name = ::NYT::NYTree::TPolymorphicYsonStruct<NPolymorphicYsonStructFor##name::TMapping>; \
static_assert(true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
