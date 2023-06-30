#ifndef YSON_STRUCT_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_detail.h"
// For the sake of sane code completion.
#include "yson_struct_detail.h"
#endif

#include "ypath_client.h"
#include "yson_struct.h"

#include <yt/yt/core/yson/token_writer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <class T>
concept IsYsonStructOrYsonSerializable = std::is_base_of_v<TYsonStructBase, T> || std::is_base_of_v<TYsonSerializableLite, T>;

// TODO(shakurov): get rid of this once concept support makes it into the standard
// library implementation. Use equality-comparability instead.
template <class T>
concept SupportsDontSerializeDefaultImpl =
    std::is_arithmetic_v<T> ||
    std::is_same_v<T, TString> ||
    std::is_same_v<T, TDuration> ||
    std::is_same_v<T, TGuid> ||
    std::is_same_v<T, std::optional<std::vector<TString>>> ||
    std::is_same_v<T, THashSet<TString>>;

template <class T>
concept SupportsDontSerializeDefault =
    SupportsDontSerializeDefaultImpl<T> ||
    TStdOptionalTraits<T>::IsStdOptional &&
    SupportsDontSerializeDefaultImpl<typename TStdOptionalTraits<T>::TValueType>;

////////////////////////////////////////////////////////////////////////////////

template <class T>
void LoadFromNode(
    T& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    std::optional<EUnrecognizedStrategy> /*recursiveUnrecognizedStrategy*/)
{
    try {
        Deserialize(parameter, node);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", path)
                << ex;
    }
}

// INodePtr
template <>
inline void LoadFromNode(
    NYTree::INodePtr& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& /*path*/,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> /*recursiveUnrecognizedStrategy*/)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            parameter = node;
            break;
        }

        case EMergeStrategy::Combine: {
            if (!parameter) {
                parameter = node;
            } else {
                parameter = PatchNode(parameter, node);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T>
void LoadFromNode(
    TIntrusivePtr<T>& parameterValue,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (recursiveUnrecognizedStrategy) {
        parameterValue->SetUnrecognizedStrategy(*recursiveUnrecognizedStrategy);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameterValue->Load(node, false, false, path);
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::optional
template <class T>
void LoadFromNode(
    std::optional<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                parameter = std::nullopt;
            } else {
                T value;
                LoadFromNode(value, node, path, EMergeStrategy::Overwrite, recursiveUnrecognizedStrategy);
                parameter = std::move(value);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::vector
template <class... T>
void LoadFromNode(
    std::vector<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto listNode = node->AsList();
            auto size = listNode->GetChildCount();
            parameter.resize(size);
            for (int i = 0; i < size; ++i) {
                LoadFromNode(
                    parameter[i],
                    listNode->GetChildOrThrow(i),
                    path + "/" + NYPath::ToYPathLiteral(i),
                    EMergeStrategy::Overwrite,
                    recursiveUnrecognizedStrategy);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

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

// For any map.
template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromNode(
    Map<T...>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            auto mapNode = node->AsMap();
            parameter.clear();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Overwrite,
                    recursiveUnrecognizedStrategy);
                parameter.emplace(DeserializeMapKey<typename Map<T...>::key_type>(key), std::move(value));
            }
            break;
        }
        case EMergeStrategy::Combine: {
            auto mapNode = node->AsMap();
            for (const auto& [key, child] : mapNode->GetChildren()) {
                M value;
                LoadFromNode(
                    value,
                    child,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    EMergeStrategy::Combine,
                    recursiveUnrecognizedStrategy);
                parameter[DeserializeMapKey<typename Map<T...>::key_type>(key)] = std::move(value);
            }
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void LoadFromCursor(
    T& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    std::optional<EUnrecognizedStrategy> /*recursiveUnrecognizedStrategy*/)
{
    try {
        Deserialize(parameter, cursor);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error reading parameter %v", path)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <IsYsonStructOrYsonSerializable T>
void LoadFromCursor(
    TIntrusivePtr<T>& parameterValue,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy);

template <class... T>
void LoadFromCursor(
    std::vector<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy);

// std::optional
template <class T>
void LoadFromCursor(
    std::optional<T>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy);

template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy);

////////////////////////////////////////////////////////////////////////////////

// INodePtr
template <>
inline void LoadFromCursor(
    NYTree::INodePtr& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    try {
        auto node = NYson::ExtractTo<INodePtr>(cursor);
        LoadFromNode(parameter, std::move(node), path, mergeStrategy, recursiveUnrecognizedStrategy);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T>
void LoadFromCursor(
    TIntrusivePtr<T>& parameterValue,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (recursiveUnrecognizedStrategy) {
        parameterValue->SetUnrecognizedStrategy(*recursiveUnrecognizedStrategy);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameterValue->Load(cursor, false, false, path);
            break;
        }

        default:
            YT_UNIMPLEMENTED();
    }
}

// std::optional
template <class T>
void LoadFromCursor(
    std::optional<T>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    try {
        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                if ((*cursor)->GetType() == NYson::EYsonItemType::EntityValue) {
                    parameter = std::nullopt;
                    cursor->Next();
                } else {
                    T value;
                    LoadFromCursor(value, cursor, path, EMergeStrategy::Overwrite, recursiveUnrecognizedStrategy);
                    parameter = std::move(value);
                }
                break;
            }

            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// std::vector
template <class... T>
void LoadFromCursor(
    std::vector<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    try {
        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                parameter.clear();
                int index = 0;
                cursor->ParseList([&](NYson::TYsonPullParserCursor* cursor) {
                    LoadFromCursor(
                        parameter.emplace_back(),
                        cursor,
                        path + "/" + NYPath::ToYPathLiteral(index),
                        EMergeStrategy::Overwrite,
                        recursiveUnrecognizedStrategy);
                    ++index;
                });
                break;
            }

            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
            << ex;
    }
}

// For any map.
template <template <typename...> class Map, class... T, class M>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    std::optional<EUnrecognizedStrategy> recursiveUnrecognizedStrategy)
{
    try {
        auto doParse = [&] (const auto& setterOrEmplacer, EMergeStrategy mergeStrategy) {
            cursor->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
                auto key = ExtractTo<TString>(cursor);
                M value;
                LoadFromCursor(
                    value,
                    cursor,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    mergeStrategy,
                    recursiveUnrecognizedStrategy);
                setterOrEmplacer(key, std::move(value));
            });
        };

        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                parameter.clear();
                auto emplacer = [&] (auto key, M&& value) {
                    parameter.emplace(DeserializeMapKey<typename Map<T...>::key_type>(key), std::move(value));
                };
                doParse(emplacer, EMergeStrategy::Overwrite);
                break;
            }
            case EMergeStrategy::Combine: {
                auto setter = [&] (auto key, M&& value) {
                    parameter[DeserializeMapKey<typename Map<T...>::key_type>(key)] = std::move(value);
                };
                doParse(setter, EMergeStrategy::Combine);
                break;
            }
            default:
                YT_UNIMPLEMENTED();
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error loading parameter %v", path)
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

template <IsYsonStructOrYsonSerializable T>
struct TGetRecursiveUnrecognized<T>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetRecursiveUnrecognized();
    }
};

template <IsYsonStructOrYsonSerializable T>
struct TGetRecursiveUnrecognized<TIntrusivePtr<T>>
{
    static IMapNodePtr Do(const TIntrusivePtr<T>& parameter)
    {
        return parameter ? parameter->GetRecursiveUnrecognized() : nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /*parameter*/,
    const NYPath::TYPath& /*path*/,
    const F& /*func*/)
{ }

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T, class F>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameterValue,
    const NYPath::TYPath& path,
    const F& func)
{
    func(*parameterValue, path);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(
    const std::vector<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (size_t i = 0; i < parameter->size(); ++i) {
        InvokeForComposites(
            &(*parameter)[i],
            path + "/" + NYPath::ToYPathLiteral(i),
            func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(
    const Map<T...>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(
            &value,
            path + "/" + NYPath::ToYPathLiteral(key),
            func);
    }
}

////////////////////////////////////////////////////////////////////////////////

// all
template <class F>
void InvokeForComposites(
    const void* /* parameter */,
    const F& /* func */)
{ }

// TYsonStruct or TYsonSerializable
template <IsYsonStructOrYsonSerializable T, class F>
inline void InvokeForComposites(const TIntrusivePtr<T>* parameter, const F& func)
{
    func(*parameter);
}

// std::vector
template <class... T, class F>
inline void InvokeForComposites(const std::vector<T...>* parameter, const F& func)
{
    for (const auto& item : *parameter) {
        InvokeForComposites(&item, func);
    }
}

// For any map.
template <template <typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(const Map<T...>* parameter, const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(&value, func);
    }
}

template <class T, class = void>
struct IsYsonStructPtr : std::false_type
{ };

template <class T>
struct IsYsonStructPtr<TIntrusivePtr<T>, typename std::enable_if<std::is_convertible<T&, TYsonStruct&>::value>::type> : std::true_type
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYsonStructDetail

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

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TYsonStructParameter<TValue>::TYsonStructParameter(TString key, std::unique_ptr<IYsonFieldAccessor<TValue>> fieldAccessor)
    : Key_(std::move(key))
    , FieldAccessor_(std::move(fieldAccessor))
    , MergeStrategy_(EMergeStrategy::Default)
{ }

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYTree::INodePtr node,
    const TLoadParameterOptions& options)
{
    if (node) {
        NPrivate::LoadFromNode(
            FieldAccessor_->GetValue(self),
            std::move(node),
            options.Path,
            options.MergeStrategy.value_or(MergeStrategy_),
            options.RecursiveUnrecognizedRecursively);
    } else if (!DefaultCtor_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.Path);
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
            NPrivate::LoadFromNode(
                FieldAccessor_->GetValue(self),
                node,
                options.Path,
                options.MergeStrategy.value_or(MergeStrategy_),
                /*recursivelyUnrecognizedStrategy*/ std::nullopt);
            validate();
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Load(
    TYsonStructBase* self,
    NYson::TYsonPullParserCursor* cursor,
    const TLoadParameterOptions& options)
{
    if (cursor) {
        NPrivate::LoadFromCursor(
            FieldAccessor_->GetValue(self),
            cursor,
            options.Path,
            options.MergeStrategy.value_or(MergeStrategy_),
            options.RecursiveUnrecognizedRecursively);
    } else if (!DefaultCtor_) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            options.Path);
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::SafeLoad(
    TYsonStructBase* self,
    NYson::TYsonPullParserCursor* cursor,
    const TLoadParameterOptions& options,
    const std::function<void()>& validate)
{
    if (cursor) {
        TValue oldValue = FieldAccessor_->GetValue(self);
        try {
            NPrivate::LoadFromCursor(
                FieldAccessor_->GetValue(self),
                cursor,
                options.Path,
                options.MergeStrategy.value_or(MergeStrategy_),
                /*recursivelyUnrecognizedStrategy*/ std::nullopt);
            validate();
        } catch (const std::exception ex) {
            FieldAccessor_->GetValue(self) = oldValue;
            throw;
        }
    }
}

template <class TValue>
void TYsonStructParameter<TValue>::Postprocess(const TYsonStructBase* self, const NYPath::TYPath& path) const
{
    const auto& value = FieldAccessor_->GetValue(self);
    for (const auto& postprocessor : Postprocessors_) {
        try {
            postprocessor(value);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Postprocess failed at %v",
                path.empty() ? "root" : path)
                    << ex;
        }
    }

    NPrivate::InvokeForComposites(
        &value,
        path,
        [] <NPrivate::IsYsonStructOrYsonSerializable T> (TIntrusivePtr<T> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Postprocess(subpath);
            }
        });
}

template <class TValue>
void TYsonStructParameter<TValue>::SetDefaultsInitialized(TYsonStructBase* self)
{
    TValue& value = FieldAccessor_->GetValue(self);

    if (DefaultCtor_) {
        value = (*DefaultCtor_)();
    }

    NPrivate::InvokeForComposites(
        &value,
        [] <NPrivate::IsYsonStructOrYsonSerializable T> (TIntrusivePtr<T> obj) {
            if (obj) {
                obj->SetDefaults();
            }
        });
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
    if constexpr (NPrivate::SupportsDontSerializeDefault<TValue>) {
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
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Alias(const TString& name)
{
    Aliases_.push_back(name);
    return *this;
}

template <class TValue>
const std::vector<TString>& TYsonStructParameter<TValue>::GetAliases() const
{
    return Aliases_;
}

template <class TValue>
const TString& TYsonStructParameter<TValue>::GetKey() const
{
    return Key_;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Optional()
{
    DefaultCtor_ = [] () { return TValue{}; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default(TValue defaultValue)
{
    static_assert(!std::is_convertible_v<TValue, TIntrusivePtr<TYsonStruct>>, "Use DefaultCtor to register TYsonStruct default.");
    DefaultCtor_ = [value = std::move(defaultValue)] () { return value; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::Default()
{
    DefaultCtor_ = [] () { return TValue{}; };
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultCtor(std::function<TValue()> defaultCtor)
{
    DefaultCtor_ = std::move(defaultCtor);
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DontSerializeDefault()
{
    // We should check for equality-comparability here but it is rather hard
    // to do the deep validation.
    static_assert(
        NPrivate::SupportsDontSerializeDefault<TValue>,
        "DontSerializeDefault requires |Parameter| to be TString, TDuration, an arithmetic type or an optional of those");

    SerializeDefault_ = false;
    return *this;
}

template <class TValue>
template <class... TArgs>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::DefaultNew(TArgs&&... args)
{
    TriviallyInitializedIntrusivePtr_ = true;
    return DefaultCtor([=] () mutable { return New<typename TValue::TUnderlying>(std::forward<TArgs>(args)...); });
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::CheckThat(TPostprocessor postprocessor)
{
    Postprocessors_.push_back(std::move(postprocessor));
    return *this;
}

template <class TValue>
TYsonStructParameter<TValue>& TYsonStructParameter<TValue>::MergeBy(EMergeStrategy strategy)
{
    MergeStrategy_ = strategy;
    return *this;
}

template <class TValue>
IMapNodePtr TYsonStructParameter<TValue>::GetRecursiveUnrecognized(const TYsonStructBase* self) const
{
    return NPrivate::TGetRecursiveUnrecognized<TValue>::Do(FieldAccessor_->GetValue(self));
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
