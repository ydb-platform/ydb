#ifndef YSON_SERIALIZABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_serializable.h"
// For the sake of sane code completion.
#include "yson_serializable.h"
#endif

#include "convert.h"
#include "serialize.h"
#include "tree_visitor.h"

#include <yt/yt/core/yson/consumer.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/actions/bind.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/datetime/base.h>

#include <optional>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class T>
concept IsYsonStructOrYsonSerializable = std::is_base_of_v<TYsonStructBase, T> || std::is_base_of_v<TYsonSerializableLite, T>;

template <class T>
void LoadFromNode(
    T& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy /*mergeStrategy*/,
    bool /*keepUnrecognizedRecursively*/)
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
    bool /*keepUnrecognizedRecursively*/)
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

// TYsonSerializable
template <IsYsonStructOrYsonSerializable T>
void LoadFromNode(
    TIntrusivePtr<T>& parameter,
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    if (!parameter || mergeStrategy == EMergeStrategy::Overwrite) {
        parameter = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameter->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    }

    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite:
        case EMergeStrategy::Combine: {
            parameter->Load(node, false, false, path);
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
    bool keepUnrecognizedRecursively)
{
    switch (mergeStrategy) {
        case EMergeStrategy::Default:
        case EMergeStrategy::Overwrite: {
            if (node->GetType() == NYTree::ENodeType::Entity) {
                parameter = std::nullopt;
            } else {
                T value;
                LoadFromNode(value, node, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
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
    bool keepUnrecognizedRecursively)
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
                    keepUnrecognizedRecursively);
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
    bool keepUnrecognizedRecursively)
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
                    keepUnrecognizedRecursively);
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
                    keepUnrecognizedRecursively);
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
    bool /*keepUnrecognizedRecursively*/)
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
    bool keepUnrecognizedRecursively);

template <class... T>
void LoadFromCursor(
    std::vector<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

// std::optional
template <class T>
void LoadFromCursor(
    std::optional<T>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

template <template <typename...> class Map, class... T, class M = typename Map<T...>::mapped_type>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively);

////////////////////////////////////////////////////////////////////////////////

// INodePtr
template <>
inline void LoadFromCursor(
    NYTree::INodePtr& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        auto node = NYson::ExtractTo<INodePtr>(cursor);
        LoadFromNode(parameter, std::move(node), path, mergeStrategy, keepUnrecognizedRecursively);
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
    bool keepUnrecognizedRecursively)
{
    if (!parameterValue || mergeStrategy == EMergeStrategy::Overwrite) {
        parameterValue = New<T>();
    }

    if (keepUnrecognizedRecursively) {
        parameterValue->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
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
    bool keepUnrecognizedRecursively)
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
                    LoadFromCursor(value, cursor, path, EMergeStrategy::Overwrite, keepUnrecognizedRecursively);
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
    bool keepUnrecognizedRecursively)
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
                        keepUnrecognizedRecursively);
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

template <class TMapping, class TValue, class TEmplacer, class TSetter>
void LoadMappingFromCursor(
    TMapping& mapping,
    TEmplacer emplacer,
    TSetter setter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
{
    try {
        auto doParse = [&] (auto setterOrEmplacer, EMergeStrategy mergeStrategy) {
            cursor->ParseMap([&] (NYson::TYsonPullParserCursor* cursor) {
                auto key = ExtractTo<TString>(cursor);
                TValue value;
                LoadFromCursor(
                    value,
                    cursor,
                    path + "/" + NYPath::ToYPathLiteral(key),
                    mergeStrategy,
                    keepUnrecognizedRecursively);
                setterOrEmplacer(mapping, key, std::move(value));
            });
        };

        switch (mergeStrategy) {
            case EMergeStrategy::Default:
            case EMergeStrategy::Overwrite: {
                mapping = {};
                doParse(emplacer, EMergeStrategy::Overwrite);
                break;
            }
            case EMergeStrategy::Combine: {
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

// For any map.
template <template <typename...> class Map, class... T, class M>
void LoadFromCursor(
    Map<T...>& parameter,
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    EMergeStrategy mergeStrategy,
    bool keepUnrecognizedRecursively)
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
                    keepUnrecognizedRecursively);
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

// For all classes except descendants of TYsonSerializableLite and their intrusive pointers
// we do not attempt to extract unrecognzied members. C++ prohibits function template specialization
// so we have to deal with static struct members.
template <class T>
struct TGetUnrecognizedRecursively
{
    static IMapNodePtr Do(const T& /*parameter*/)
    {
        return nullptr;
    }
};

template <IsYsonStructOrYsonSerializable T>
struct TGetUnrecognizedRecursively<T>
{
    static IMapNodePtr Do(const T& parameter)
    {
        return parameter.GetRecursiveUnrecognized();
    }
};

template <IsYsonStructOrYsonSerializable T>
struct TGetUnrecognizedRecursively<TIntrusivePtr<T>>
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
    const void* /* parameter */,
    const NYPath::TYPath& /* path */,
    const F& /* func */)
{ }

// TYsonSerializable or TYsonStruct
template <IsYsonStructOrYsonSerializable T, class F>
inline void InvokeForComposites(
    const TIntrusivePtr<T>* parameter,
    const NYPath::TYPath& path,
    const F& func)
{
    func(*parameter, path);
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
template <template<typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
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
template <template<typename...> class Map, class... T, class F, class M = typename Map<T...>::mapped_type>
inline void InvokeForComposites(const Map<T...>* parameter, const F& func)
{
    for (const auto& [key, value] : *parameter) {
        InvokeForComposites(&value, func);
    }
}

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

} // namespace NDetail

template <class T>
TYsonSerializableLite::TParameter<T>::TParameter(TString key, T& parameter)
    : Key(std::move(key))
    , Parameter(parameter)
    , MergeStrategy(EMergeStrategy::Default)
{ }

template <class T>
void TYsonSerializableLite::TParameter<T>::Load(
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    std::optional<EMergeStrategy> mergeStrategy)
{
    if (node) {
        NDetail::LoadFromNode(
            Parameter,
            node,
            path,
            mergeStrategy.value_or(MergeStrategy),
            KeepUnrecognizedRecursively);
    } else if (!DefaultValue) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            path);
    }
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SafeLoad(
    NYTree::INodePtr node,
    const NYPath::TYPath& path,
    const std::function<void()>& validate,
    std::optional<EMergeStrategy> mergeStrategy)
{
    if (node) {
        T oldValue = Parameter;
        try {
            NDetail::LoadFromNode(
                Parameter,
                node,
                path,
                mergeStrategy.value_or(MergeStrategy),
                KeepUnrecognizedRecursively);
            validate();
        } catch (const std::exception&) {
            Parameter = std::move(oldValue);
            throw;
        }
    }
}


template <class T>
void TYsonSerializableLite::TParameter<T>::Load(
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    std::optional<EMergeStrategy> mergeStrategy)
{
    if (cursor) {
        NDetail::LoadFromCursor(
            Parameter,
            cursor,
            path,
            mergeStrategy.value_or(MergeStrategy),
            KeepUnrecognizedRecursively);
    } else if (!DefaultValue) {
        THROW_ERROR_EXCEPTION("Missing required parameter %v",
            path);
    }
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SafeLoad(
    NYson::TYsonPullParserCursor* cursor,
    const NYPath::TYPath& path,
    const std::function<void()>& validate,
    std::optional<EMergeStrategy> mergeStrategy)
{
    if (cursor) {
        T oldValue = Parameter;
        try {
            NDetail::LoadFromCursor(
                Parameter,
                cursor,
                path,
                mergeStrategy.value_or(MergeStrategy),
                KeepUnrecognizedRecursively);
            validate();
        } catch (const std::exception&) {
            Parameter = std::move(oldValue);
            throw;
        }
    }
}

template <class T>
void TYsonSerializableLite::TParameter<T>::Postprocess(const NYPath::TYPath& path) const
{
    for (const auto& postprocessor : Postprocessors) {
        try {
            postprocessor(Parameter);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Postprocess failed at %v",
                path.empty() ? "root" : path)
                << ex;
        }
    }

    NYT::NYTree::NDetail::InvokeForComposites(
        &Parameter,
        path,
        [] <NDetail::IsYsonStructOrYsonSerializable TStruct> (TIntrusivePtr<TStruct> obj, const NYPath::TYPath& subpath) {
            if (obj) {
                obj->Postprocess(subpath);
            }
        });
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SetDefaults()
{
    if (DefaultValue) {
        Parameter = *DefaultValue;
    }

    NYT::NYTree::NDetail::InvokeForComposites(
        &Parameter,
        []  <NDetail::IsYsonStructOrYsonSerializable TStruct> (TIntrusivePtr<TStruct> obj) {
            if (obj) {
                obj->SetDefaults();
            }
        });
}

template <class T>
void TYsonSerializableLite::TParameter<T>::Save(NYson::IYsonConsumer* consumer) const
{
    using NYTree::Serialize;
    Serialize(Parameter, consumer);
}

template <class T>
bool TYsonSerializableLite::TParameter<T>::CanOmitValue() const
{
    if constexpr (NDetail::SupportsDontSerializeDefault<T>) {
        if (!SerializeDefault && Parameter == DefaultValue) {
            return true;
        }
    }

    return NYT::NYTree::NDetail::CanOmitValue(&Parameter, DefaultValue ? &*DefaultValue : nullptr);
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Alias(const TString& name)
{
    Aliases.push_back(name);
    return *this;
}

template <class T>
const std::vector<TString>& TYsonSerializableLite::TParameter<T>::GetAliases() const
{
    return Aliases;
}

template <class T>
const TString& TYsonSerializableLite::TParameter<T>::GetKey() const
{
    return Key;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Optional()
{
    DefaultValue = Parameter;
    return *this;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::Default(const T& defaultValue)
{
    DefaultValue = defaultValue;
    Parameter = defaultValue;
    return *this;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::DontSerializeDefault()
{
    // We should check for equality-comparability here but it is rather hard
    // to do the deep validation.
    static_assert(
        NDetail::SupportsDontSerializeDefault<T>,
        "DontSerializeDefault requires |Parameter| to be TString, TDuration, an arithmetic type or an optional of those");

    SerializeDefault = false;
    return *this;
}

template <class T>
template <class... TArgs>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::DefaultNew(TArgs&&... args)
{
    return Default(New<typename T::TUnderlying>(std::forward<TArgs>(args)...));
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::CheckThat(TPostprocessor postprocessor)
{
    Postprocessors.push_back(std::move(postprocessor));
    return *this;
}

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::MergeBy(EMergeStrategy strategy)
{
    MergeStrategy = strategy;
    return *this;
}

template <class T>
IMapNodePtr TYsonSerializableLite::TParameter<T>::GetUnrecognizedRecursively() const
{
    return NDetail::TGetUnrecognizedRecursively<T>::Do(Parameter);
}

template <class T>
void TYsonSerializableLite::TParameter<T>::SetKeepUnrecognizedRecursively()
{
    KeepUnrecognizedRecursively = true;
}

////////////////////////////////////////////////////////////////////////////////
// Standard postprocessors

#define DEFINE_POSTPROCESSOR(method, condition, error) \
    template <class T> \
    TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::TParameter<T>::method \
    { \
        return CheckThat([=] (const T& parameter) { \
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

template <class T>
TYsonSerializableLite::TParameter<T>& TYsonSerializableLite::RegisterParameter(
    TString parameterName,
    T& value)
{
    auto parameter = New<TParameter<T>>(parameterName, value);
    if (UnrecognizedStrategy == EUnrecognizedStrategy::KeepRecursive) {
        parameter->SetKeepUnrecognizedRecursively();
    }
    YT_VERIFY(Parameters.emplace(std::move(parameterName), parameter).second);
    return *parameter;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TIntrusivePtr<T> CloneYsonSerializable(const TIntrusivePtr<T>& obj)
{
    static_assert(
        std::is_convertible_v<T*, TYsonSerializable*>,
        "'obj' must be convertible to TYsonSerializable");

    return NYTree::ConvertTo<TIntrusivePtr<T>>(NYson::ConvertToYsonString(*obj));
}

template <class T>
std::vector<TIntrusivePtr<T>> CloneYsonSerializables(const std::vector<TIntrusivePtr<T>>& objs)
{
    std::vector<TIntrusivePtr<T>> clonedObjs;
    clonedObjs.reserve(objs.size());
    for (const auto& obj : objs) {
        clonedObjs.push_back(CloneYsonSerializable(obj));
    }
    return clonedObjs;
}

template <class T>
THashMap<TString, TIntrusivePtr<T>> CloneYsonSerializables(const THashMap<TString, TIntrusivePtr<T>>& objs)
{
    THashMap<TString, TIntrusivePtr<T>> clonedObjs;
    clonedObjs.reserve(objs.size());
    for (const auto& [key, obj] : objs) {
        clonedObjs.emplace(key, CloneYsonSerializable(obj));
    }
    return clonedObjs;
}

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    const TIntrusivePtr<T>& obj,
    const NYTree::INodePtr& patch)
{
    static_assert(
        std::is_convertible_v<T*, TYsonSerializable*>,
        "'obj' must be convertible to TYsonSerializable");

    using NYTree::INodePtr;
    using NYTree::ConvertTo;

    if (patch) {
        return ConvertTo<TIntrusivePtr<T>>(PatchNode(ConvertTo<INodePtr>(obj), patch));
    } else {
        return CloneYsonSerializable(obj);
    }
}

template <class T>
TIntrusivePtr<T> UpdateYsonSerializable(
    const TIntrusivePtr<T>& obj,
    const NYson::TYsonString& patch)
{
    if (!patch) {
        return obj;
    }

    return UpdateYsonSerializable(obj, ConvertToNode(patch));
}

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const NYson::TYsonString& newConfigYson)
{
    return ReconfigureYsonSerializable(config, ConvertToNode(newConfigYson));
}

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const TIntrusivePtr<T>& newConfig)
{
    return ReconfigureYsonSerializable(config, ConvertToNode(newConfig));
}

template <class T>
bool ReconfigureYsonSerializable(
    const TIntrusivePtr<T>& config,
    const NYTree::INodePtr& newConfigNode)
{
    auto configNode = NYTree::ConvertToNode(config);

    auto newConfig = NYTree::ConvertTo<TIntrusivePtr<T>>(newConfigNode);
    auto newCanonicalConfigNode = NYTree::ConvertToNode(newConfig);

    if (NYTree::AreNodesEqual(configNode, newCanonicalConfigNode)) {
        return false;
    }

    config->Load(newConfigNode);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
