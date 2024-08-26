#ifndef POLYMORPHIC_YSON_STRUCT_INL_H_
#error "Direct inclusion of this file is not allowed, include polymorphic_yson_struct.h"
// For the sake of sane code completion.
#include "polymorphic_yson_struct.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TEnum>
    requires TEnumTraits<TEnum>::IsEnum
template <class TBase, class TDerived>
TIntrusivePtr<TBase> TEnumTraitsExt<TEnum>::ConcreteFactory()
{
    return New<TDerived>();
}

template <class TEnum>
    requires TEnumTraits<TEnum>::IsEnum
template <class TBase, class... TDerived>
TInstanceFactory<TEnum, TBase>  TEnumTraitsExt<TEnum>::MakeFactory()
{
    static constexpr auto keys = TTraits::GetDomainValues();
    using TTuple = std::tuple<TBase, TDerived...>;

    TInstanceFactory<TEnum, TBase> mapping;

    [&] <size_t... Idx> (std::index_sequence<Idx...>) {
        ([&] {
            mapping[keys[Idx]] = &TEnumTraitsExt<TEnum>::ConcreteFactory<TBase, std::tuple_element_t<Idx, TTuple>>;
        } (), ...);
    } (std::make_index_sequence<sizeof...(TDerived) + 1>());

    return mapping;
}

////////////////////////////////////////////////////////////////////////////////

template <class TEnum, class TB, class... TD>
TIntrusivePtr<TB> TPolymorphicEnumMapping<TEnum, TB, TD...>::MakeInstance(TEnum e)
{
    static auto factory =
        TEnumTraitsExt<TEnum>::template MakeFactory<TB, TD...>();

    return factory[e]();
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

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

    auto key = map->FindChildValue<TKey>("type");
    THROW_ERROR_EXCEPTION_UNLESS(
        key.has_value(),
        "Concrete type must be specified! Use \"type\": \"concrete_type\"");

    auto type = *key;
    if (!Storage_ || HeldType_ != type) {
        // NB: We will try to merge configs if types match.
        HeldType_ = type;
        Storage_ = TMapping::MakeInstance(HeldType_);
    }

    if (recursiveUnrecognizedStrategy) {
        Storage_->SetUnrecognizedStrategy(*recursiveUnrecognizedStrategy);
    }

    // "type" must be unrecognized for the original struct
    // therefore we must delete it prior to |Load| call.
    map->RemoveChild("type");
    Storage_->Load(map, postprocess, setDefaults, path);
}

template <CPolymorphicEnumMapping TMapping>
void TPolymorphicYsonStruct<TMapping>::Save(NYson::IYsonConsumer* consumer) const
{
    consumer->OnBeginMap();

    consumer->OnKeyedItem("type");
    consumer->OnStringScalar(FormatEnum(HeldType_));

    Storage_->SaveAsMapFragment(consumer);
    consumer->OnEndMap();
}

template <CPolymorphicEnumMapping TMapping>
template <std::derived_from<typename TMapping::TBase> TConcrete>
TIntrusivePtr<TConcrete> TPolymorphicYsonStruct<TMapping>::TryGetConcrete() const
{
    return DynamicPointerCast<TConcrete>(Storage_);
}

template <CPolymorphicEnumMapping TMapping>
typename TPolymorphicYsonStruct<TMapping>::TKey TPolymorphicYsonStruct<TMapping>::GetCurrentType() const
{
    return HeldType_;
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

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ_ELEM(item) \
    PP_LEFT_PARENTHESIS PP_ELEMENT(item, 0) PP_RIGHT_PARENTHESIS

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ(seq) \
    PP_FOR_EACH(POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ_ELEM, seq)

#define POLYMORPHIC_YSON_STRUCT_IMPL__ENUM_NAME(Struct) \
    E##Struct##Type

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM(Struct, seq) \
    DEFINE_ENUM(EType, POLYMORPHIC_YSON_STRUCT_IMPL__GET_ENUM_SEQ(seq))

#define POLYMORPHIC_YSON_STRUCT_IMPL__GET_CLASS_ELEM(item) \
    PP_COMMA() PP_ELEMENT(item, 1)

#define POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_CLASS(Struct, seq) \
    using TMapping = TPolymorphicEnumMapping<EType PP_FOR_EACH(POLYMORPHIC_YSON_STRUCT_IMPL__GET_CLASS_ELEM, seq)>

#define DEFINE_POLYMORPHIC_YSON_STRUCT(name, seq) \
namespace NPolymorphicYsonStructFor##name { \
 \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_ENUM(name, seq); \
    POLYMORPHIC_YSON_STRUCT_IMPL__MAKE_MAPPING_CLASS(name, seq); \
} /*NPolymorphicYsonStructFor##name*/ \
using POLYMORPHIC_YSON_STRUCT_IMPL__ENUM_NAME(name) = NPolymorphicYsonStructFor##name::EType; \
using T##name = TPolymorphicYsonStruct<NPolymorphicYsonStructFor##name::TMapping>; \
static_assert(true)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
