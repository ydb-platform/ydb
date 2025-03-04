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

    // Registers validator that accepts only new value as an argument.
    TFieldConfigurator& Validator(TCallback<void(const TValue&)> validator);

    // Registers updater that accepts old and new values as arguments.
    TFieldConfigurator& Updater(TCallback<void(const TValue&, const TValue&)> updater);

    // Registers updater that accepts only new value as an argument.
    TFieldConfigurator& Updater(TCallback<void(const TValue&)> updater);

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
template <CYsonStructDerived TStruct>
class TConfigurator
{
public:
    explicit TConfigurator(NDetail::TConfiguredFieldDirectoryPtr state = {});

    template <class TValue>
    NDetail::TFieldConfigurator<TValue>& Field(const std::string& name, TYsonStructField<TStruct, TValue> field);

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
