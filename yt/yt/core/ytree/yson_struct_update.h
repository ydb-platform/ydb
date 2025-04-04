#pragma once

#include "yson_struct.h"
#include "yson_struct_detail.h"

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
class TConfigurator;

template <CYsonStructDerived TStruct>
class TSealedConfigurator;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TConfiguredFieldDirectory);

////////////////////////////////////////////////////////////////////////////////

struct IFieldConfigurator
    : public TRefCounted
{
    virtual void DoValidate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const = 0;

    virtual void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IFieldConfigurator);
DEFINE_REFCOUNTED_TYPE(IFieldConfigurator);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TFieldConfigurator
    : public IFieldConfigurator
{
public:
    // Registers validator that accepts old and new values as arguments.
    TFieldConfigurator& Validator(TCallback<void(const TValue&, const TValue&)> validator);
    TFieldConfigurator& Validator(TCallback<void(TValue, TValue)> validator);

    // Registers validator that accepts only new value as an argument.
    TFieldConfigurator& Validator(TCallback<void(const TValue&)> validator);
    TFieldConfigurator& Validator(TCallback<void(TValue)> validator);

    // Registers updater that accepts old and new values as arguments.
    TFieldConfigurator& Updater(TCallback<void(const TValue&, const TValue&)> updater);
    TFieldConfigurator& Updater(TCallback<void(TValue, TValue)> updater);

    // Registers updater that accepts only new value as an argument.
    TFieldConfigurator& Updater(TCallback<void(const TValue&)> updater);
    TFieldConfigurator& Updater(TCallback<void(TValue)> updater);

    // Registers nested YsonStruct to be updated recursively.
    template <CYsonStructDerived TUnwrappedValue>
    TFieldConfigurator& NestedUpdater(
        TCallback<TSealedConfigurator<TUnwrappedValue>()> configureCallback);

    void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const override;

    void DoValidate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const override;

private:
    void VerifyEmptyUpdater() const;
    void VerifyEmptyValidator() const;

    TCallback<void(const TValue&, const TValue&)> Updater_;
    TCallback<void(const TValue&, const TValue&)> Validator_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CString = std::same_as<T, TString> || std::same_as<T, std::string>;

template <class T>
struct TUnwrapMapOfYsonStructs
{ };

template <CYsonStructDerived T, CString TStringKey>
struct TUnwrapMapOfYsonStructs<THashMap<TStringKey, TIntrusivePtr<T>>>
{
    using TKey = TStringKey;
    using TStruct = T;
};

// todo(coteeq): This field registrar is not oldStruct-agnostic:
// It expects a serialized sequence of YsonStructs, which will be used to
// (in case of successes) update via the configurator like this:
// 1. Update(s1, s2);
// 2. Update(s2, s3);
// 3. Update(s3, s4);
//
// This is neither enforced by TSealedConfigurator's API, nor it is obvious
// to the reader.
template <class TMap>
class TMapFieldConfigurator
    // xxx(coteeq): Well, I honestly wanted to make this base protected to hide
    // the |Updater|/|Validator| methods and friends, but it seems that
    // the whole non-public inheritance thing is deranged, so here we are :(
    : public TFieldConfigurator<TMap>
{
    using TKey = typename TUnwrapMapOfYsonStructs<TMap>::TKey;
    using TStruct = typename TUnwrapMapOfYsonStructs<TMap>::TStruct;
    using TStructPtr = TIntrusivePtr<TStruct>;

public:
    TMapFieldConfigurator();

    //! (key, newStruct) -> newConfigurator
    TMapFieldConfigurator& OnAdded(TCallback<TConfigurator<TStruct>(const TKey&, const TStructPtr&)> onAdded);
    //! (key, oldStruct) -> void
    TMapFieldConfigurator& OnRemoved(TCallback<void(const TKey&, const TStructPtr&)> onRemoved);

    //! (key, newStruct) -> void
    TMapFieldConfigurator& ValidateOnAdded(TCallback<void(const TKey&, const TStructPtr&)> onAdded);
    //! (key, oldStruct) -> void
    TMapFieldConfigurator& ValidateOnRemoved(TCallback<void(const TKey&, const TStructPtr&)> onRemoved);

    TMapFieldConfigurator& ConfigureChild(const TKey& key, TConfigurator<TStruct> configurator);

private:
    void ValidateImpl(
        const THashMap<TKey, TStructPtr>& oldMap,
        const THashMap<TKey, TStructPtr>& newMap);
    void UpdateImpl(
        const THashMap<TKey, TStructPtr>& oldMap,
        const THashMap<TKey, TStructPtr>& newMap);

    THashMap<TKey, TSealedConfigurator<TStruct>> Configurators_;

    TCallback<TConfigurator<TStruct>(const TKey&, const TIntrusivePtr<TStruct>&)> OnAdded_;
    TCallback<void(const TKey&, const TIntrusivePtr<TStruct>&)> OnRemoved_;

    TCallback<void(const TKey&, const TIntrusivePtr<TStruct>&)> ValidateOnAdded_;
    TCallback<void(const TKey&, const TIntrusivePtr<TStruct>&)> ValidateOnRemoved_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

//! Use TConfigurator to configure additional (dynamic) info about fields of
//! a TYsonStruct. It is intended to be used like a TRegistrar:
//!
//!     TConfigurator<TMyStruct> configurator;
//!     configurator.Field("my_field", &TMyStruct::MyField)
//!         .Updater(...);
//!
//! When you finish configuring, call |Seal|
//! (or just implicitly cast to TSealedConfigurator) to access your info:
//!
//!     auto sealed = std::move(configurator).Seal();
//!
//! Currently, only methods related to dynamic update of YsonStruct are
//! supported (see unittests/yson_struct_update_ut.cpp).
//!
//! Note that when you update/validate YsonStruct (via sealed configurator), you
//! pass YsonStruct ptrs there. So the new YsonStruct is valid in the sense of
//! YsonStruct's built-in validity checks (i.e. LessThan/NonEmpty/Postprocessor)
//! by construction - you do not need to even think about it.
template <CYsonStructDerived TStruct>
class TConfigurator
{
public:
    explicit TConfigurator(NDetail::TConfiguredFieldDirectoryPtr configuredFields = {});

    template <class TValue>
    NDetail::TFieldConfigurator<TValue>& Field(const std::string& name, TYsonStructField<TStruct, TValue> field);

    template <class TValue>
    NDetail::TMapFieldConfigurator<TValue>& MapField(const TString& name, TYsonStructField<TStruct, TValue> field);

    // Converts to a configurator of a base class
    template <class TAncestor>
    operator TConfigurator<TAncestor>() const;

    TSealedConfigurator<TStruct> Seal() &&;

private:
    NDetail::TConfiguredFieldDirectoryPtr ConfiguredFields_;

    template <CYsonStructDerived TStructForSealed>
    friend class TSealedConfigurator;
};

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
class TSealedConfigurator
    : public TMoveOnly
{
public:
    TSealedConfigurator(TConfigurator<TStruct> configurator);

    void Validate(
        TIntrusivePtr<TStruct> oldStruct,
        TIntrusivePtr<TStruct> newStruct) const;

    void Update(
        TIntrusivePtr<TStruct> oldStruct,
        TIntrusivePtr<TStruct> newStruct) const;

private:
    using TFieldConfiguratorMethod = void(NDetail::IFieldConfigurator::*)(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const;

    void Do(
        TIntrusivePtr<TStruct> oldStruct,
        TIntrusivePtr<TStruct> newStruct,
        TFieldConfiguratorMethod fieldMethod) const;

    NDetail::TConfiguredFieldDirectoryPtr ConfiguredFields_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YSON_STRUCT_UPDATE_INL_H_
#include "yson_struct_update-inl.h"
#undef YSON_STRUCT_UPDATE_INL_H_
