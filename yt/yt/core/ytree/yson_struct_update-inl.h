#ifndef YSON_STRUCT_UPDATE_INL_H_
#error "Direct inclusion of this file is not allowed, include yson_struct_update.h"
// For the sake of sane code completion.
#include "yson_struct_update.h"
#endif

namespace NYT::NYTree::NYsonStructUpdate {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TRegisteredFieldDirectory
    : public TRefCounted
{
    template <class TStruct>
    static TRegisteredFieldDirectoryPtr Create()
    {
        static auto meta = GetMeta<TStruct>();

        auto directory = New<TRegisteredFieldDirectory>();
        directory->Meta = meta;
        return directory;
    }

    THashMap<IYsonStructParameterPtr, IFieldRegistrarPtr> ParameterToFieldRegistrar;
    const IYsonStructMeta* Meta;

private:
    template <class TStruct>
    static const IYsonStructMeta* GetMeta() {
        return New<TStruct>()->GetMeta();
    }
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
TFieldRegistrar<TValue>& TConfigurator<TStruct>::Field(const TString& name, TYsonStructField<TStruct, TValue> field)
{
    IYsonStructParameterPtr parameter;

    try {
        parameter = RegisteredFields_->Meta->GetParameter(name);
    } catch (const std::exception& ex) {
        YT_ABORT();
    }
    YT_VERIFY(parameter->HoldsField(CreateTypeErasedYsonStructField(field)));

    auto fieldRegistrar = New<TFieldRegistrar<TValue>>();
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

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Updater(TCallback<void(const TValue&, const TValue&)> updater)
{
    VerifyEmpty();
    Updater_ = updater;
    return *this;
}

template <class TValue>
TFieldRegistrar<TValue>& TFieldRegistrar<TValue>::Updater(TCallback<void(const TValue&)> updater)
{
    VerifyEmpty();
    Updater_ = BIND([updater = std::move(updater)] (const TValue& /*old*/, const TValue& new_) {
        updater(std::move(new_));
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
    TCallback<void(TConfigurator<TUnwrappedValue>)> registerCallback)
{
    static_assert(
        std::is_same_v<
            TUnwrappedValue,
            typename NDetail::TUnwrapYsonStructIntrusivePtr<TValue>::TStruct>);

    VerifyEmpty();
    TConfigurator<TUnwrappedValue> registrar;
    registerCallback(registrar);
    Updater_ = BIND([registrar = std::move(registrar)] (const TValue& old, const TValue& new_) {
        Update(registrar, old, new_);
    });
    return *this;
}

template <class TValue>
void TFieldRegistrar<TValue>::DoUpdate(
    IYsonStructParameterPtr parameter,
    TYsonStructBase* old,
    TYsonStructBase* new_) const
{
    if (!Updater_) {
        return;
    }

    auto typedParameter = DynamicPointerCast<TYsonStructParameter<TValue>>(parameter);
    YT_VERIFY(typedParameter);
    Updater_(
        typedParameter->GetValue(old),
        typedParameter->GetValue(new_));
}

template <class TValue>
void TFieldRegistrar<TValue>::VerifyEmpty() const
{
    YT_VERIFY(!Updater_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
void Update(
    const TConfigurator<TStruct>& registrar,
    TIntrusivePtr<TStruct> old,
    TIntrusivePtr<TStruct> new_)
{
    const auto* meta = old->GetMeta();
    const auto& parameterToFieldRegistrar = registrar.RegisteredFields_->ParameterToFieldRegistrar;
    for (const auto& [name, parameter] : meta->GetParameterMap()) {
        if (parameter->CompareParameter(old.Get(), new_.Get())) {
            continue;
        }

        if (auto fieldDescIter = parameterToFieldRegistrar.find(parameter);
            fieldDescIter != parameterToFieldRegistrar.end()
        ) {
            fieldDescIter->second->DoUpdate(parameter, old.Get(), new_.Get());
        } else {
            THROW_ERROR_EXCEPTION("Field %Qv is not marked as updatable, but was changed", name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NYsonStructUpdate
