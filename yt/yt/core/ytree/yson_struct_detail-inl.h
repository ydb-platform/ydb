#ifndef YSON_STRUCT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_detail.h"
// For the sake of sane code completion.
#include "yson_struct_detail.h"
#endif

#include "ypath_client.h"
#include "yson_schema.h"
#include "yson_struct.h"
#include "proto_yson_struct.h"

#include <yt/yt/core/yson/token_writer.h>

#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/wrapper_traits.h>

#include <google/protobuf/util/message_differencer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
struct TTypedYsonStructField
    : public ITypeErasedYsonStructField
{
    TYsonStructField<TStruct, TValue> Field;
};

template <class TStruct, class TValue>
ITypeErasedYsonStructFieldPtr CreateTypeErasedYsonStructField(TYsonStructField<TStruct, TValue> field)
{
    auto erasedField = New<TTypedYsonStructField<TStruct, TValue>>();
    erasedField->Field = field;
    return erasedField;
}

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CTupleLike = requires {
    std::tuple_size<T>{};
};

template <class T>
concept CContainerLike = requires {
    typename T::value_type;
};

template <class T>
struct TEqualityComparableHelper
{
    static constexpr bool Value = std::equality_comparable<T>;
};

template <class T, size_t... I>
constexpr bool IsSequenceEqualityComparable(std::index_sequence<I...> /*sequence*/)
{
    return (TEqualityComparableHelper<typename std::tuple_element<I, T>::type>::Value && ...);
}

template <CTupleLike T>
struct TEqualityComparableHelper<T>
{
    static constexpr bool Value = IsSequenceEqualityComparable<T>(std::make_index_sequence<std::tuple_size<T>::value>());
};

template <CContainerLike T>
struct TEqualityComparableHelper<T>
{
    static constexpr bool Value = TEqualityComparableHelper<typename T::value_type>::Value;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

// TODO(h0pless): Get rid of this once containers will have constraints for equality operator.
// Once this will be the case, it should be safe to use std::equality_comparable here instead.
template <class T>
concept CRecursivelyEqualityComparable = NDetail::TEqualityComparableHelper<T>::Value;

template <class T>
concept CSupportsDontSerializeDefault =
    CRecursivelyEqualityComparable<typename TWrapperTraits<T>::TRecursiveUnwrapped>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
T DeserializeMapKey(TStringBuf value)
{
    if constexpr (TEnumTraits<T>::IsEnum) {
        return ParseEnum<T>(value);
    } else if constexpr (std::is_same_v<T, TGuid>) {
        return TGuid::FromString(value);
    } else if constexpr (TStrongTypedefTraits<T>::IsStrongTypedef) {
        return T(DeserializeMapKey<typename TStrongTypedefTraits<T>::TUnderlying>(value));
    } else {
        return FromString<T>(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CNodePtr = requires (T node) {
    [] (INodePtr) { } (node);
};

template <CNodePtr TNodePtr>
struct TYsonSourceTraits<TNodePtr>
{
    static constexpr bool IsValid = true;

    static INodePtr AsNode(TNodePtr& source)
    {
        // NRVO.
        return source;
    }

    static bool IsEmpty(TNodePtr& source)
    {
        return source->GetType() == ENodeType::Entity;
    }

    static void Advance(TNodePtr& /*source*/)
    { }

    template <class... TArgs, class TFiller>
    static void FillVector(TNodePtr& source, std::vector<TArgs...>& vector, TFiller filler)
    {
        auto listNode = source->AsList();
        auto size = listNode->GetChildCount();
        vector.reserve(size);
        for (int i = 0; i < size; ++i) {
            filler(vector, std::move(listNode->GetChildOrThrow(i)));
        }
    }

    template <CAnyMap TMap, class TFiller>
    static void FillMap(TNodePtr& source, TMap& map, TFiller filler)
    {
        auto mapNode = source->AsMap();

        // NB: We iterate over temporary object anyway.
        // Might as well move key/child into the filler
        for (auto [key, child] : mapNode->GetChildren()) {
            filler(map, std::move(key), std::move(child));
        }
    }
};

template <>
struct TYsonSourceTraits<NYson::TYsonPullParserCursor*>
{
    static constexpr bool IsValid = true;

    static INodePtr AsNode(NYson::TYsonPullParserCursor*& source)
    {
        return NYson::ExtractTo<NYTree::INodePtr>(source);
    }

    static bool IsEmpty(NYson::TYsonPullParserCursor*& source)
    {
        return (*source)->GetType() == NYson::EYsonItemType::EntityValue;
    }

    static void Advance(NYson::TYsonPullParserCursor*& source)
    {
        source->Next();
    }

    template <class... TArgs, class TFiller>
    static void FillVector(NYson::TYsonPullParserCursor*& source, std::vector<TArgs...>& vector, TFiller filler)
    {
        source->ParseList([&](NYson::TYsonPullParserCursor* cursor) {
            filler(vector, cursor);
        });
    }

    template <CAnyMap TMap, class TFiller>
    static void FillMap(NYson::TYsonPullParserCursor*& source, TMap& map, TFiller filler)
    {
        source->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
            auto key = ExtractTo<std::string>(cursor);
            filler(map, std::move(key), source);
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

// NB(arkady-e1ppa): We perform forward declaration of containers
// so that we can find the correct overload for any composition of them
// e.g. std::optional<std::vector<T>>.

// std::optional
template <CYsonStructSource TSource, class T>
void LoadFromSource(
    std::optional<T>& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy);

// std::vector
template <CYsonStructSource TSource, CStdVector TVector>
void LoadFromSource(
    TVector& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy);

// any map.
template <CYsonStructSource TSource, CAnyMap TMap>
void LoadFromSource(
    TMap& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy);

////////////////////////////////////////////////////////////////////////////////

// Primitive type
template <CYsonStructSource TSource, class T>
void LoadFromSource(
    T& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> /*ignored*/)
{
    using TTraits = TYsonSourceTraits<TSource>;

    try {
        Deserialize(parameter, TTraits::AsNode(source));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", pathGetter())
            << ex;
    }
}

// TYsonString
template <CYsonStructSource TSource>
void LoadFromSource(
    ::NYT::NYson::TYsonString& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> /*ignored*/)
{
    using TTraits = TYsonSourceTraits<TSource>;

    try {
        parameter = NYson::ConvertToYsonString(TTraits::AsNode(source));
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

// INodePtr
template <CYsonStructSource TSource>
void LoadFromSource(
    INodePtr& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> /*ignored*/)
{
    using TTraits = TYsonSourceTraits<TSource>;

    try {
        auto node = TTraits::AsNode(source);
        if (!parameter) {
            parameter = std::move(node);
        } else {
            parameter = PatchNode(parameter, std::move(node));
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

// TYsonStruct
template <CYsonStructSource TSource, CYsonStructDerived T>
void LoadFromSource(
    TIntrusivePtr<T>& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    if (!parameter) {
        parameter = New<T>();
    }

    if (unrecognizedStrategy) {
        parameter->SetUnrecognizedStrategy(*unrecognizedStrategy);
    }

    parameter->Load(std::move(source), /*postprocess*/ false, /*setDefaults*/ false, pathGetter);
}

// YsonStructLite
template <CYsonStructSource TSource, std::derived_from<TYsonStructLite> T>
void LoadFromSource(
    T& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    try {
        if (unrecognizedStrategy) {
            parameter.SetUnrecognizedStrategy(*unrecognizedStrategy);
        }
        parameter.Load(std::move(source), /*postprocess*/ false, /*setDefaults*/ false, pathGetter);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", pathGetter())
            << ex;
    }
}

// ExternalizedYsonStruct
template <CYsonStructSource TSource, CExternallySerializable T>
void LoadFromSource(
    T& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    try {
        Deserialize(parameter, std::move(source), /*postprocess*/ false, /*setDefaults*/ false, unrecognizedStrategy);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", pathGetter())
            << ex;
    }
}

// CYsonStructField
// NB(arkady-e1ppa): We check for alias presence in the body so that
// partially modelled concept does not result in call to Deserialize
// (which is the default implementation) but hard CE.
template <CYsonStructSource TSource, CYsonStructLoadableFieldFor<TSource> TExtension>
void LoadFromSource(
    TExtension& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    static_assert(CYsonStructFieldFor<TExtension, TSource>, "You must add alias TImplementsYsonStructField");

    try {
        parameter.Load(
            std::move(source),
            /*postprocess*/ false,
            /*setDefaults*/ false,
            pathGetter,
            unrecognizedStrategy);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

// std::optional
template <CYsonStructSource TSource, class T>
void LoadFromSource(
    std::optional<T>& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    using TTraits = TYsonSourceTraits<TSource>;

    try {
        if (TTraits::IsEmpty(source)) {
            parameter = std::nullopt;
            TTraits::Advance(source);
            return;
        }

        if (parameter.has_value()) {
            LoadFromSource(*parameter, std::move(source), pathGetter, unrecognizedStrategy);
            return;
        }

        T value;
        LoadFromSource(value, std::move(source), pathGetter, unrecognizedStrategy);
        parameter = std::move(value);

    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

// std::vector
template <CYsonStructSource TSource, CStdVector TVector>
void LoadFromSource(
    TVector& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    using TTraits = TYsonSourceTraits<TSource>;

    try {
        parameter.clear();
        int index = 0;

        TTraits::FillVector(source, parameter, [&] (auto& vector, auto elementSource) {
            LoadFromSource(
                vector.emplace_back(),
                elementSource,
                [&] {
                    return pathGetter() + "/" + NYPath::ToYPathLiteral(index);
                },
                unrecognizedStrategy);
            ++index;
        });
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

// any map.
template <CYsonStructSource TSource, CAnyMap TMap>
void LoadFromSource(
    TMap& parameter,
    TSource source,
    const std::function<NYPath::TYPath()>& pathGetter,
    std::optional<EUnrecognizedStrategy> unrecognizedStrategy)
{
    using TTraits = TYsonSourceTraits<TSource>;
    // TODO(arkady-e1ppa): Remove "typename" when clang-14 is abolished.
    using TKey = typename TMap::key_type;
    using TValue = typename TMap::mapped_type;

    try {
        TTraits::FillMap(source, parameter, [&] (TMap& map, const std::string& key, auto childSource) {
            TValue value;
            LoadFromSource(
                value,
                childSource,
                [&] {
                    return pathGetter() + "/" + NYPath::ToYPathLiteral(key);
                },
                unrecognizedStrategy);
            map[DeserializeMapKey<TKey>(key)] = std::move(value);
        });
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", pathGetter())
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

// For all classes except descendants of TYsonStructBase and their intrusive pointers
// we do not attempt to extract unrecognized members. C++ prohibits function template specialization
// so we have to deal with static struct members.
template <class T>
struct TGetRecursiveUnrecognized
{
    static IMapNodePtr Do(const T& /*parameter*/)
    {
        return nullptr;
    }
};

template <CYsonStructDerived T>
struct TGetRecursiveUnrecognized<T>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetRecursiveUnrecognized();
    }
};

template <CYsonStructDerived T>
struct TGetRecursiveUnrecognized<TIntrusivePtr<T>>
{
    static IMapNodePtr Do(const TIntrusivePtr<T>& parameter)
    {
        return parameter ? parameter->GetRecursiveUnrecognized() : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
template <class T>
inline void PostprocessRecursive(
    T&,
    const std::function<NYPath::TYPath()>&)
{
    // Random class is not postprocessed.
}

template <CExternallySerializable T>
inline void PostprocessRecursive(
    T& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    using TTraits = TGetExternalizedYsonStructTraits<T>;
    using TSerializer = typename TTraits::TExternalSerializer;
    auto serializer = TSerializer::template CreateWritable<T, TSerializer>(parameter, false);
    serializer.Postprocess(pathGetter);
}

// TYsonStruct
template <std::derived_from<TYsonStruct> T>
inline void PostprocessRecursive(
    TIntrusivePtr<T>& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    if (parameter) {
        parameter->Postprocess(pathGetter);
    }
}

// TYsonStructLite
template <std::derived_from<TYsonStructLite> T>
inline void PostprocessRecursive(
    T& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    parameter.Postprocess(pathGetter);
}

// std::optional
template <class T>
inline void PostprocessRecursive(
    std::optional<T>& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    if (parameter.has_value()) {
        PostprocessRecursive(*parameter, pathGetter);
    }
}

// std::vector
template <CStdVector TVector>
inline void PostprocessRecursive(
    TVector& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    for (size_t i = 0; i < parameter.size(); ++i) {
        PostprocessRecursive(
            parameter[i],
            [&] {
                return pathGetter() + "/" + NYPath::ToYPathLiteral(i);
            });
    }
}

// any map
template <CAnyMap TMap>
inline void PostprocessRecursive(
    TMap& parameter,
    const std::function<NYPath::TYPath()>& pathGetter)
{
    for (auto& [key, value] : parameter) {
        PostprocessRecursive(
            value,
            [&pathGetter, &key = key] {
                return pathGetter() + "/" + NYPath::ToYPathLiteral(key);
            });
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
template <class T>
inline void ResetOnLoad(T& parameter)
{
    parameter = T();
}

// TYsonStruct
template <std::derived_from<TYsonStruct> T>
inline void ResetOnLoad(TIntrusivePtr<T>& parameter)
{
    parameter = New<T>();
}

// TYsonStructLite
template <std::derived_from<TYsonStructLite> T>
inline void ResetOnLoad(T& parameter)
{
    parameter.SetDefaults();
}

// INodePtr
template <>
inline void ResetOnLoad(INodePtr& parameter)
{
    parameter.Reset();
}

// std::optional
template <class T>
inline void ResetOnLoad(std::optional<T>& parameter)
{
    parameter.reset();
}

// std::vector
template <CStdVector TVector>
inline void ResetOnLoad(TVector& parameter)
{
    parameter.clear();
}

// any map
template <CAnyMap TMap>
inline void ResetOnLoad(TMap& parameter)
{
    parameter.clear();
}

////////////////////////////////////////////////////////////////////////////////

// Any T.
template <class T>
bool CompareValues(const T& lhs, const T& rhs);

// TIntrusivePtr.
template <class T>
bool CompareValues(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs);

// std::optional.
template <class T>
bool CompareValues(const std::optional<T>& lhs, const std::optional<T>& rhs);

// std::vector.
template <CStdVector T>
bool CompareValues(const T& lhs, const T& rhs);

// any map.
template <CAnyMap T>
bool CompareValues(const T& lhs, const T& rhs);

template <class T>
concept CNode = CNodePtr<TIntrusivePtr<T>>;

// INode, IListNode, IMapNode.
template <CNode T>
bool CompareValues(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs);

template <CProtobufMessage T>
bool CompareValues(const T& lhs, const T& rhs);

////////////////////////////////////////////////////////////////////////////////

// Any T.
template <class T>
bool CompareValues(const T& lhs, const T& rhs)
{
    if constexpr (CRecursivelyEqualityComparable<typename TWrapperTraits<T>::TRecursiveUnwrapped>) {
        return lhs == rhs;
    } else {
        return false;
    }
}

// TIntrusivePtr.
template <class T>
bool CompareValues(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    if constexpr (CRecursivelyEqualityComparable<typename TWrapperTraits<T>::TRecursiveUnwrapped>) {
        if (!lhs || !rhs) {
            return rhs == lhs;
        }

        return CompareValues(*lhs, *rhs);
    } else {
        return false;
    }
}

// std::optional.
template <class T>
bool CompareValues(const std::optional<T>& lhs, const std::optional<T>& rhs)
{
    if (lhs.has_value() != rhs.has_value()) {
        return false;
    }

    if (!lhs.has_value()) {
        return true;
    }

    return CompareValues(*lhs, *rhs);
}

// std::vector.
template <CStdVector T>
bool CompareValues(const T& lhs, const T& rhs)
{
    if (std::ssize(lhs) != std::ssize(rhs)) {
        return false;
    }

    for (int idx = 0; idx < std::ssize(lhs); ++idx) {
        if (!CompareValues(lhs[idx], rhs[idx])) {
            return false;
        }
    }

    return true;
}

// any map.
template <CAnyMap T>
bool CompareValues(const T& lhs, const T& rhs)
{
    if (std::ssize(lhs) != std::ssize(rhs)) {
        return false;
    }

    for (const auto& [key, value] : lhs) {
        auto rhsIt = rhs.find(key);
        if (rhsIt == std::end(rhs)) {
            return false;
        }

        if (!CompareValues(key, rhsIt->first)) {
            return false;
        }

        if (!CompareValues(value, rhsIt->second)) {
            return false;
        }
    }

    return true;
}

// INode, IListNode, IMapNode.
template <CNode T>
bool CompareValues(const TIntrusivePtr<T>& lhs, const TIntrusivePtr<T>& rhs)
{
    return AreNodesEqual(lhs, rhs);
}

template <CProtobufMessage T>
bool CompareValues(const T& lhs, const T& rhs)
{
    return google::protobuf::util::MessageDifferencer::Equals(lhs, rhs);
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
TYsonFieldAccessor<TStruct, TValue>::TYsonFieldAccessor(TYsonStructField<TStruct, TValue> field)
    : Field_(field)
{ }

template <class TStruct, class TValue>
TValue& TYsonFieldAccessor<TStruct, TValue>::GetValue(const TYsonStructBase* source)
{
    return TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(source)->*Field_;
}

template <class TStruct, class TValue>
bool TYsonFieldAccessor<TStruct, TValue>::HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const
{
    if (auto typedThat = DynamicPointerCast<TTypedYsonStructField<TStruct, TValue>>(erasedField)) {
        return typedThat->Field == Field_;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

template <class TStruct, class TValue>
TUniversalYsonParameterAccessor<TStruct, TValue>::TUniversalYsonParameterAccessor(std::function<TValue&(TStruct*)> accessor)
    : Accessor_(std::move(accessor))
{ }

template <class TStruct, class TValue>
bool TUniversalYsonParameterAccessor<TStruct, TValue>::HoldsField(ITypeErasedYsonStructFieldPtr /*field*/) const
{
    YT_UNIMPLEMENTED();
}

template <class TStruct, class TValue>
TValue& TUniversalYsonParameterAccessor<TStruct, TValue>::GetValue(const TYsonStructBase* source)
{
    return Accessor_(TYsonStructRegistry::Get()->template CachedDynamicCast<TStruct>(source));
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TYsonStructParameter<TValue>::TYsonStructParameter(
    std::string key,
    std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor,
    int fieldIndex)
    : Key_(std::move(key))
    , FieldAccessor_(std::move(fieldAccessor))
    , FieldIndex_(fieldIndex)
{ }

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options)
{
    if (node) {
        auto unrecognizedStrategy = options.RecursiveUnrecognizedRecursively;
        if (EnforceDefaultUnrecognizedStrategy_) {
            unrecognizedStrategy.reset();
        }
        if (!unrecognizedStrategy) {
            unrecognizedStrategy = DefaultUnrecognizedStrategy_;
        }
        if (ResetOnLoad_) {
            NPrivate::ResetOnLoad(FieldAccessor_->GetValue(self));
        }
        NPrivate::LoadFromSource(
            FieldAccessor_->GetValue(self),
            std::move(node),
            options.PathGetter,
            unrecognizedStrategy);

        if (auto* bitmap = self->GetSetFieldsBitmap()) {
            bitmap->Set(FieldIndex_);
        }
    } else if (!Optional_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.PathGetter());
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYson::TYsonPullParserCursor* cursor,
    const TLoadParameterOptions& options)
{
    if (cursor) {
        auto unrecognizedStrategy = options.RecursiveUnrecognizedRecursively;
        if (EnforceDefaultUnrecognizedStrategy_) {
            unrecognizedStrategy.reset();
        }
        if (!unrecognizedStrategy) {
            unrecognizedStrategy = DefaultUnrecognizedStrategy_;
        }
        if (ResetOnLoad_) {
            NPrivate::ResetOnLoad(FieldAccessor_->GetValue(self));
        }
        NPrivate::LoadFromSource(
            FieldAccessor_->GetValue(self),
            cursor,
            options.PathGetter,
            unrecognizedStrategy);

        if (auto* bitmap = self->GetSetFieldsBitmap()) {
            bitmap->Set(FieldIndex_);
        }
    } else if (!Optional_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.PathGetter());
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SafeLoad(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options,
    const std::function<void()>& validate)
{
    if (node) {
        TValue oldValue = FieldAccessor_->GetValue(self);
        try {
            FieldAccessor_->GetValue(self) = TValue();
            NPrivate::LoadFromSource(
                FieldAccessor_->GetValue(self),
                node,
                options.PathGetter,
                /*recursivelyUnrecognizedStrategy*/ std::nullopt);
            validate();

            if (auto* bitmap = self->GetSetFieldsBitmap()) {
                bitmap->Set(FieldIndex_);
            }
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::PostprocessParameter(const TYsonStructBase* self, const std::function<NYPath::TYPath()>& pathGetter) const
{
    TValue& value = FieldAccessor_->GetValue(self);
    NPrivate::PostprocessRecursive(value, pathGetter);

    for (const auto& validator : Validators_) {
        try {
            validator(value);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Validation failed at %v",
                !pathGetter ? "root" : pathGetter())
                    << ex;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SetDefaultsInitialized(TYsonStructBase* self)
{
    TValue& value = FieldAccessor_->GetValue(self);

    if (DefaultCtor_) {
        value = (*DefaultCtor_)();
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Save(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const
{
    using NYTree::Serialize;
    Serialize(FieldAccessor_->GetValue(self), consumer);
}

template <class TValue>
bool TYsonStructParameter<TValue>::CanOmitValue(const TYsonStructBase* self) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    if constexpr (NPrivate::CSupportsDontSerializeDefault<TValue>) {
        if (!SerializeDefault_ && value == (*DefaultCtor_)()) {
            return true;
        }
    }

    if (!DefaultCtor_) {
        return NYT::NYTree::NDetail::CanOmitValue(&value, nullptr);
    }

    if (TriviallyInitializedIntrusivePtr_) {
        return false;
    }

    auto defaultValue = (*DefaultCtor_)();
    return NYT::NYTree::NDetail::CanOmitValue(&value, &defaultValue);
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Alias(const std::string& name)
{
    Aliases_.push_back(name);
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::ResetOnLoad()
{
    ResetOnLoad_ = true;
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultUnrecognizedStrategy(EUnrecognizedStrategy strategy)
{
    DefaultUnrecognizedStrategy_.emplace(strategy);
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::EnforceDefaultUnrecognizedStrategy()
{
    EnforceDefaultUnrecognizedStrategy_ = true;
    return *this;
}

template <class TValue>
const std::vector<std::string>& TYsonStructParameter<TValue>::GetAliases() const
{
    return Aliases_;
}

template <class TValue>
bool TYsonStructParameter<TValue>::IsRequired() const
{
    return !Optional_;
}

template <class TValue>
const std::string& TYsonStructParameter<TValue>::GetKey() const
{
    return Key_;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Optional(bool init)
{
    Optional_ = true;

    if (init) {
        DefaultCtor_ = [] () { return TValue{}; };
    }

    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default(TValue defaultValue)
{
    static_assert(!std::is_convertible_v<TValue, TIntrusivePtr<TYsonStruct>>, "Use DefaultCtor to register TYsonStruct default.");
    DefaultCtor_ = [value = std::move(defaultValue)] () { return value; };
    Optional_ = true;
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default()
{
    return Optional();
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultCtor(std::function<TValue()> defaultCtor)
{
    DefaultCtor_ = std::move(defaultCtor);
    Optional_ = true;
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DontSerializeDefault()
{
    // We should check for equality-comparability here but it is rather hard
    // to do the deep validation.
    static_assert(
        NPrivate::CSupportsDontSerializeDefault<TValue>,
        "DontSerializeDefault requires |Parameter| to be std::string, TDuration, an arithmetic type or an optional of those");

    SerializeDefault_ = false;
    return *this;
}

template <class TValue>
template <class... TArgs>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultNew(TArgs&&... args)
{
    TriviallyInitializedIntrusivePtr_ = true;
    Optional_ = true;
    return DefaultCtor([=] () mutable { return New<typename TValue::TUnderlying>(std::forward<TArgs>(args)...); });
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::CheckThat(TValidator validator)
{
    Validators_.push_back(std::move(validator));
    return *this;
}

template <class TValue>
IMapNodePtr TYsonStructParameter<TValue>::GetRecursiveUnrecognized(const TYsonStructBase* self) const
{
    return NPrivate::TGetRecursiveUnrecognized<TValue>::Do(FieldAccessor_->GetValue(self));
}

template <class TValue>
void TYsonStructParameter<TValue>::WriteSchema(const TYsonStructBase* self, NYson::IYsonConsumer* consumer) const
{
    // TODO(bulatman) What about constraints: minimum, maximum, default and etc?
    NPrivate::WriteSchema(FieldAccessor_->GetValue(self), consumer);
}

template <class TValue>
bool TYsonStructParameter<TValue>::CompareParameter(const TYsonStructBase* lhsSelf, const TYsonStructBase* rhsSelf) const
{
    return NPrivate::CompareValues(FieldAccessor_->GetValue(lhsSelf), FieldAccessor_->GetValue(rhsSelf));
}

template <class TValue>
int TYsonStructParameter<TValue>::GetFieldIndex() const
{
    return FieldIndex_;
}

template <class TValue>
const TValue& TYsonStructParameter<TValue>::GetValue(const TYsonStructBase* source) const
{
    YT_VERIFY(FieldAccessor_);
    return FieldAccessor_->GetValue(source);
}

template <class TValue>
bool TYsonStructParameter<TValue>::HoldsField(ITypeErasedYsonStructFieldPtr erasedField) const
{
    return FieldAccessor_ && FieldAccessor_->HoldsField(erasedField);
}

////////////////////////////////////////////////////////////////////////////////
// Standard postprocessors

#define DEFINE_POSTPROCESSOR(method, condition, error) \
    template <class TValue> \
    TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::method \
    { \
        return CheckThat([=] (const TValue& parameter) { \
            using ::ToString; \
            std::optional<TValueType> nullableParameter(parameter); \
            if (nullableParameter) { \
                const auto& actual = *nullableParameter; \
                if (!(condition)) { \
                    THROW_ERROR error; \
                } \
            } \
        }); \
    }

DEFINE_POSTPROCESSOR(
    GreaterThan(TValueType expected),
    actual > expected,
    TError("Expected > %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    GreaterThanOrEqual(TValueType expected),
    actual >= expected,
    TError("Expected >= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThan(TValueType expected),
    actual < expected,
    TError("Expected < %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    LessThanOrEqual(TValueType expected),
    actual <= expected,
    TError("Expected <= %v, found %v", expected, actual)
)

DEFINE_POSTPROCESSOR(
    InRange(TValueType lowerBound, TValueType upperBound),
    lowerBound <= actual && actual <= upperBound,
    TError("Expected in range [%v,%v], found %v", lowerBound, upperBound, actual)
)

DEFINE_POSTPROCESSOR(
    NonEmpty(),
    actual.size() > 0,
    TError("Value must not be empty")
)

#undef DEFINE_POSTPROCESSOR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
