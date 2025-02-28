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

struct TRegisteredFieldDirectory
    : public TRefCounted
{
    template <class TStruct>
    static TRegisteredFieldDirectoryPtr Create()
    {
        static auto* meta = GetYsonStructMeta<TStruct>();

        auto directory = New<TRegisteredFieldDirectory>();
        directory->Meta = meta;
        return directory;
    }

    THashMap<IYsonStructParameterPtr, IFieldRegistrarPtr> ParameterToFieldRegistrar;
    const IYsonStructMeta* Meta;
};

DEFINE_REFCOUNTED_TYPE(TRegisteredFieldDirectory);

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
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Validator(TCallback<void(const TValue&, const TValue&)> validator)
{
    VerifyEmptyValidator();
    Validator_ = validator;
    return *this;
}

template <class TValue>
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Validator(TCallback<void(const TValue&)> validator)
{
    VerifyEmptyValidator();
    Validator_ = BIND_NO_PROPAGATE([validator = std::move(validator)] (const TValue& /*oldValue*/, const TValue& newValue) {
        validator(std::move(newValue));
    });
    return *this;
}

template <class TValue>
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Updater(TCallback<void(const TValue&, const TValue&)> updater)
{
    VerifyEmptyUpdater();
    Updater_ = updater;
    return *this;
}

template <class TValue>
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Updater(TCallback<void(const TValue&)> updater)
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
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::NestedUpdater(
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
void TFieldRegistrar<TValue>::DoValidate(
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
void TFieldRegistrar<TValue>::DoUpdate(
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
void TFieldRegistrar<TValue>::VerifyEmptyValidator() const
{
    YT_VERIFY(!Validator_);
}

template <class TValue>
void TFieldRegistrar<TValue>::VerifyEmptyUpdater() const
{
    YT_VERIFY(!Updater_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
TConfigurator<TStruct>::TConfigurator(NDetail::TRegisteredFieldDirectoryPtr registeredFields)
    : RegisteredFields_(registeredFields)
{
    // NB: Initialize TRegisteredFieldDirectory with
    // the youngest child in the hierarchy.
    if (!registeredFields) {
        RegisteredFields_ = NDetail::TRegisteredFieldDirectory::Create<TStruct>();
    }
}

template <CYsonStructDerived TStruct>
template <class TValue>
NDetail::TFieldRegistrar<TValue>& TConfigurator<TStruct>::Field(const std::string& name, TYsonStructField<TStruct, TValue> field)
{
    IYsonStructParameterPtr parameter;

    try {
        parameter = RegisteredFields_->Meta->GetParameter(name);
    } catch (const std::exception& ex) {
        YT_ABORT();
    }
    YT_VERIFY(parameter->HoldsField(CreateTypeErasedYsonStructField(field)));

    auto fieldRegistrar = New<NDetail::TFieldRegistrar<TValue>>();
    RegisteredFields_->ParameterToFieldRegistrar.emplace(parameter, fieldRegistrar);
    return *fieldRegistrar;
}

template <CYsonStructDerived TStruct>
template <class TAncestor>
TConfigurator<TStruct>::operator TConfigurator<TAncestor>() const
{
    static_assert(std::derived_from<TStruct, TAncestor> && std::derived_from<TAncestor, TYsonStructBase>);
    return TConfigurator<TAncestor>(RegisteredFields_);
}

template <CYsonStructDerived TStruct>
TSealedConfigurator<TStruct> TConfigurator<TStruct>::Seal() &&
{
    return std::move(*this);
}

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
TSealedConfigurator<TStruct>::TSealedConfigurator(TConfigurator<TStruct> configurator)
    : RegisteredFields_(std::move(configurator.RegisteredFields_))
{ }

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Validate(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct) const
{
    Do(oldStruct, newStruct, &NDetail::IFieldRegistrar::DoValidate);
}

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Update(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct) const
{
    Do(oldStruct, newStruct, &NDetail::IFieldRegistrar::DoUpdate);
}

template <CYsonStructDerived TStruct>
void TSealedConfigurator<TStruct>::Do(
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct,
    TFieldRegistrarMethod fieldMethod) const
{
    const auto* meta = oldStruct->GetMeta();
    YT_VERIFY(meta == newStruct->GetMeta());
    const auto& parameterToFieldRegistrar = RegisteredFields_->ParameterToFieldRegistrar;
    YT_VERIFY(RegisteredFields_->Meta == meta);
    for (const auto& [name, parameter] : meta->GetParameterMap()) {
        if (parameter->CompareParameter(oldStruct.Get(), newStruct.Get())) {
            continue;
        }

        auto fieldDescIter = parameterToFieldRegistrar.find(parameter);
        if (fieldDescIter == parameterToFieldRegistrar.end()) {
            THROW_ERROR_EXCEPTION("Field %Qv is not marked as updatable, but was changed", name);
        } else {
            (*(fieldDescIter->second).*fieldMethod)(parameter, oldStruct.Get(), newStruct.Get());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
