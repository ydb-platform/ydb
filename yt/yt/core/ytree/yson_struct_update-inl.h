#ifndef YSON_STRUCT_UPDATE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_update.h"
// For the sake of sane code completion.
#include "yson_struct_update.h"
#endif

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <CYsonStructDerived TStruct>
const IYsonStructMeta* GetYsonStructMeta() {
    return New<TStruct>()->GetMeta();
}

struct TConfiguredFieldDirectory
    : public TRefCounted
{
    template <class TStruct>
    static TConfiguredFieldDirectoryPtr Create()
    {
        static auto* meta = GetYsonStructMeta<TStruct>();

        auto directory = New<TConfiguredFieldDirectory>();
        directory->Meta = meta;
        return directory;
    }

    THashMap<IYsonStructParameterPtr, IFieldConfiguratorPtr> ParameterToFieldConfigurator;
    const IYsonStructMeta* Meta;
};

DEFINE_REFCOUNTED_TYPE(TConfiguredFieldDirectory);

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TUnwrapYsonStructIntrusivePtr
{ };

template <class T>
struct TUnwrapYsonStructIntrusivePtr<TIntrusivePtr<T>>
{
    using TStruct = T;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TFieldConfigurator<TValue>& TFieldConfigurator<TValue>::Validator(TCallback<void(const TValue&, const TValue&)> validator)
{
    VerifyEmptyValidator();
    Validator_ = validator;
    return *this;
}

template <class TValue>
TFieldConfigurator<TValue>& TFieldConfigurator<TValue>::Validator(TCallback<void(const TValue&)> validator)
{
    VerifyEmptyValidator();
    Validator_ = BIND_NO_PROPAGATE([validator = std::move(validator)] (const TValue& /*oldValue*/, const TValue& newValue) {
        validator(std::move(newValue));
    });
    return *this;
}

template <class TValue>
TFieldConfigurator<TValue>& TFieldConfigurator<TValue>::Updater(TCallback<void(const TValue&, const TValue&)> updater)
{
    VerifyEmptyUpdater();
    Updater_ = updater;
    return *this;
}

template <class TValue>
TFieldConfigurator<TValue>& TFieldConfigurator<TValue>::Updater(TCallback<void(const TValue&)> updater)
{
    VerifyEmptyUpdater();
    Updater_ = BIND_NO_PROPAGATE([updater = std::move(updater)] (const TValue& /*oldValue*/, const TValue& newValue) {
        updater(std::move(newValue));
    });
    return *this;
}

// NB(coteeq): Little note on the weirdness of the signature:
//
// #TValue could be either YsonStruct or not. In case it is YsonStruct, I want
// to provide #NestedUpdater, that will statically check that TValue is
// actually an YsonStruct.
// On the other hand, if TValue is not an YsonStruct, I do not want this method
// to be callable (or to even exist).
// Thirdly, the signature of this method requires me to specify
// TConfigurator<TValue> that will complain if TValue is not CYsonStructDerived.
//
// So I made this method templated with <CYsonStructDerived TOtherValue>. This
// way, TConfigurator<TOtherValue> always makes sense, I just need to static_assert
// that TOtherValue == TValue. Last thing is that nested YsonStruct is always
// written as TIntrusivePtr<TMyStruct>, so I need to unwrap that to TMyStruct.
template <class TValue>
template <CYsonStructDerived TUnwrappedValue>
TFieldConfigurator<TValue>& TFieldConfigurator<TValue>::NestedUpdater(
    TCallback<TSealedConfigurator<TUnwrappedValue>()> configureCallback)
{
    static_assert(
        std::is_same_v<
            TUnwrappedValue,
            typename NDetail::TUnwrapYsonStructIntrusivePtr<TValue>::TStruct>);

    VerifyEmptyUpdater();
    auto configurator = configureCallback();
    Updater_ = BIND_NO_PROPAGATE([configurator = std::move(configurator)] (const TValue& oldValue, const TValue& newValue) {
        configurator.Update(oldValue, newValue);
    });
    return *this;
}

template <class TValue>
void TFieldConfigurator<TValue>::DoValidate(
    IYsonStructParameterPtr parameter,
    TYsonStructBase* oldStruct,
    TYsonStructBase* newStruct) const
{
    if (!Validator_) {
        return;
    }

    auto typedParameter = DynamicPointerCast<TYsonStructParameter<TValue>>(parameter);
    YT_VERIFY(typedParameter);
    Validator_(
        typedParameter->GetValue(oldStruct),
        typedParameter->GetValue(newStruct));
}

template <class TValue>
void TFieldConfigurator<TValue>::DoUpdate(
    IYsonStructParameterPtr parameter,
    TYsonStructBase* oldStruct,
    TYsonStructBase* newStruct) const
{
    if (!Updater_) {
        return;
    }

    auto typedParameter = DynamicPointerCast<TYsonStructParameter<TValue>>(parameter);
    YT_VERIFY(typedParameter);
    Updater_(
        typedParameter->GetValue(oldStruct),
        typedParameter->GetValue(newStruct));
}

template <class TValue>
void TFieldConfigurator<TValue>::VerifyEmptyValidator() const
{
    YT_VERIFY(!Validator_);
}

template <class TValue>
void TFieldConfigurator<TValue>::VerifyEmptyUpdater() const
{
    YT_VERIFY(!Updater_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TMap>
TMapFieldConfigurator<TMap>& TMapFieldConfigurator<TMap>::OnAdded(TCallback<TConfigurator<TStruct>(const TKey&, const TStructPtr&)> onAdded)
{
    OnAdded_ = std::move(onAdded);
    return *this;
}

template <class TMap>
TMapFieldConfigurator<TMap>& TMapFieldConfigurator<TMap>::OnRemoved(TCallback<void(const TKey&, const TStructPtr&)> onRemoved)
{
    OnRemoved_ = std::move(onRemoved);
    return *this;
}

template <class TMap>
TMapFieldConfigurator<TMap>& TMapFieldConfigurator<TMap>::ValidateOnAdded(TCallback<void(const TKey&, const TStructPtr&)> validateOnAdded)
{
    ValidateOnAdded_ = std::move(validateOnAdded);
    return *this;
}

template <class TMap>
TMapFieldConfigurator<TMap>& TMapFieldConfigurator<TMap>::ValidateOnRemoved(TCallback<void(const TKey&, const TStructPtr&)> validateOnRemoved)
{
    ValidateOnRemoved_ = std::move(validateOnRemoved);
    return *this;
}

template <class TMap>
TMapFieldConfigurator<TMap>& TMapFieldConfigurator<TMap>::ConfigureChild(const TKey& key, TConfigurator<TStruct> configurator)
{
    EmplaceOrCrash(Configurators_, key, std::move(configurator));
    return *this;
}

template <class TMap>
TMapFieldConfigurator<TMap>::TMapFieldConfigurator()
{
    OnAdded_ = BIND_NO_PROPAGATE([] (const TKey& key, const TStructPtr& /*newValue*/)
        -> TConfigurator<TStruct> {
        THROW_ERROR_EXCEPTION("Cannot create a new element in the map")
            << TErrorAttribute("created_key", key);
    });
    OnRemoved_ = BIND_NO_PROPAGATE([] (const TKey& key, const TStructPtr& /*oldValue*/) {
        THROW_ERROR_EXCEPTION("Cannot remove elements from the map")
            << TErrorAttribute("removed_key", key);
    });
    ValidateOnAdded_ = BIND_NO_PROPAGATE([] (const TKey& key, const TStructPtr& /*newValue*/) {
        THROW_ERROR_EXCEPTION("Cannot create a new element in the map")
            << TErrorAttribute("created_key", key);
    });
    ValidateOnRemoved_ = BIND_NO_PROPAGATE([] (const TKey& key, const TStructPtr& /*oldValue*/) {
        THROW_ERROR_EXCEPTION("Cannot remove elements from the map")
            << TErrorAttribute("removed_key", key);
    });

    TFieldConfigurator<TMap>::Updater(
        BIND_NO_PROPAGATE(ThrowOnDestroyed(&TMapFieldConfigurator::UpdateImpl), MakeWeak(this)));
    TFieldConfigurator<TMap>::Validator(
        BIND_NO_PROPAGATE(ThrowOnDestroyed(&TMapFieldConfigurator::ValidateImpl), MakeWeak(this)));
}

template <class TMap>
void TMapFieldConfigurator<TMap>::ValidateImpl(
    const THashMap<TKey, TStructPtr>& oldMap,
    const THashMap<TKey, TStructPtr>& newMap)
{
    THashSet<TKey> commonKeys;

    for (const auto& [key, oldValue] : oldMap) {
        if (!newMap.contains(key)) {
            ValidateOnRemoved_(key, oldValue);
        } else {
            EmplaceOrCrash(commonKeys, key);
        }
    }
    for (const auto& [key, newValue] : newMap) {
        if (!oldMap.contains(key)) {
            ValidateOnAdded_(key, newValue);
        } else {
            YT_VERIFY(commonKeys.contains(key));
        }
    }
    for (const auto& [key, configurator] : Configurators_) {
        if (commonKeys.contains(key)) {
            configurator.Validate(GetOrCrash(oldMap, key), GetOrCrash(newMap, key));
            EraseOrCrash(commonKeys, key);
        }
    }

    // Check that all keys were configured.
    YT_VERIFY(commonKeys.empty());
}

template <class TMap>
void TMapFieldConfigurator<TMap>::UpdateImpl(
    const THashMap<TKey, TStructPtr>& oldMap,
    const THashMap<TKey, TStructPtr>& newMap)
{
    THashSet<TKey> commonKeys;

    for (const auto& [key, oldValue] : oldMap) {
        if (!newMap.contains(key)) {
            OnRemoved_(key, oldValue);
            EraseOrCrash(Configurators_, key);
        } else {
            EmplaceOrCrash(commonKeys, key);
        }
    }
    for (const auto& [key, newValue] : newMap) {
        if (!oldMap.contains(key)) {
            EmplaceOrCrash(
                Configurators_,
                key,
                OnAdded_(key, newValue));
        } else {
            YT_VERIFY(commonKeys.contains(key));
        }
    }
    for (const auto& [key, configurator] : Configurators_) {
        if (commonKeys.contains(key)) {
            configurator.Update(GetOrCrash(oldMap, key), GetOrCrash(newMap, key));
            EraseOrCrash(commonKeys, key);
        }
    }

    // Check that all keys were configured.
    YT_VERIFY(commonKeys.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
TConfigurator<TStruct>::TConfigurator(NDetail::TConfiguredFieldDirectoryPtr configuredFields)
    : ConfiguredFields_(configuredFields)
{
    // NB: Initialize TConfiguredFieldDirectory with
    // the youngest child in the hierarchy.
    if (!configuredFields) {
        ConfiguredFields_ = NDetail::TConfiguredFieldDirectory::Create<TStruct>();
    }
}

template <CYsonStructDerived TStruct>
template <class TValue>
NDetail::TFieldConfigurator<TValue>& TConfigurator<TStruct>::Field(const std::string& name, TYsonStructField<TStruct, TValue> field)
{
    IYsonStructParameterPtr parameter;

    try {
        parameter = ConfiguredFields_->Meta->GetParameter(name);
    } catch (const std::exception& ex) {
        YT_ABORT();
    }
    YT_VERIFY(parameter->HoldsField(CreateTypeErasedYsonStructField(field)));

    auto fieldConfigurator = New<NDetail::TFieldConfigurator<TValue>>();
    ConfiguredFields_->ParameterToFieldConfigurator.emplace(parameter, fieldConfigurator);
    return *fieldConfigurator;
}

template <CYsonStructDerived TStruct>
template <class TValue>
NDetail::TMapFieldConfigurator<TValue>& TConfigurator<TStruct>::MapField(const TString& name, TYsonStructField<TStruct, TValue> field)
{
    IYsonStructParameterPtr parameter;

    try {
        parameter = ConfiguredFields_->Meta->GetParameter(name);
    } catch (const std::exception& ex) {
        YT_ABORT();
    }
    YT_VERIFY(parameter->HoldsField(CreateTypeErasedYsonStructField(field)));

    auto fieldConfigurator = New<NDetail::TMapFieldConfigurator<TValue>>();
    ConfiguredFields_->ParameterToFieldConfigurator.emplace(std::move(parameter), fieldConfigurator);
    return *fieldConfigurator;
}

template <CYsonStructDerived TStruct>
template <class TAncestor>
TConfigurator<TStruct>::operator TConfigurator<TAncestor>() const
{
    static_assert(std::derived_from<TStruct, TAncestor> && std::derived_from<TAncestor, TYsonStructBase>);
    return TConfigurator<TAncestor>(ConfiguredFields_);
}

template <CYsonStructDerived TStruct>
TSealedConfigurator<TStruct> TConfigurator<TStruct>::Seal() &&
{
    return std::move(*this);
}

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
TSealedConfigurator<TStruct>::TSealedConfigurator(TConfigurator<TStruct> configurator)
    : ConfiguredFields_(std::move(configurator.ConfiguredFields_))
{ }

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Validate(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct) const
{
    Do(std::move(oldStruct), std::move(newStruct), &NDetail::IFieldConfigurator::DoValidate);
}

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Update(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct) const
{
    Do(std::move(oldStruct), std::move(newStruct), &NDetail::IFieldConfigurator::DoUpdate);
}

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Do(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct,
    TFieldConfiguratorMethod fieldMethod) const
{
    const auto* meta = oldStruct->GetMeta();
    YT_VERIFY(meta == newStruct->GetMeta());
    const auto& parameterToFieldConfigurator = ConfiguredFields_->ParameterToFieldConfigurator;
    YT_VERIFY(ConfiguredFields_->Meta == meta);
    for (const auto& [name, parameter] : meta->GetParameterMap()) {
        if (parameter->CompareParameter(oldStruct.Get(), newStruct.Get())) {
            continue;
        }

        auto fieldDescIter = parameterToFieldConfigurator.find(parameter);
        if (fieldDescIter == parameterToFieldConfigurator.end()) {
            THROW_ERROR_EXCEPTION("Field %Qv is not marked as updatable, but was changed", name);
        } else {
            (*(fieldDescIter->second).*fieldMethod)(parameter, oldStruct.Get(), newStruct.Get());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
