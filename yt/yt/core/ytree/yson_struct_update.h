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

DECLARE_REFCOUNTED_STRUCT(TRegisteredFieldDirectory);

////////////////////////////////////////////////////////////////////////////////

struct IFieldRegistrar
    : public TRefCounted
{
    virtual void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IFieldRegistrar);
DEFINE_REFCOUNTED_TYPE(IFieldRegistrar);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TFieldRegistrar
    : public IFieldRegistrar
{
public:
    // Registers updater that accepts old and new values as arguments.
    TFieldRegistrar& Updater(TCallback<void(const TValue&, const TValue&)> updater);

    // Registers updater that accepts only new value as an argument.
    TFieldRegistrar& Updater(TCallback<void(const TValue&)> updater);

    // Registers nested YsonStruct to be updated recursively.
    template <CYsonStructDerived TUnwrappedValue>
    TFieldRegistrar& NestedUpdater(
        TCallback<TSealedConfigurator<TUnwrappedValue>()> configureCallback);

    void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* oldStruct,
        TYsonStructBase* newStruct) const override;

private:
    void VerifyEmpty() const;

    TCallback<void(const TValue&, const TValue&)> Updater_;
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
    explicit TConfigurator(NDetail::TRegisteredFieldDirectoryPtr state = {});

    template <class TValue>
    NDetail::TFieldRegistrar<TValue>& Field(const std::string& name, TYsonStructField<TStruct, TValue> field);

    // Converts to a registrar of a base class
    template <class TAncestor>
    operator TConfigurator<TAncestor>() const;

    TSealedConfigurator<TStruct> Seal() &&;

private:
    NDetail::TRegisteredFieldDirectoryPtr RegisteredFields_;

    template <CYsonStructDerived TStructForSealed>
    friend class TSealedConfigurator;
};

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
class TSealedConfigurator
{
public:
    TSealedConfigurator(TConfigurator<TStruct> configurator);

    void Update(
        TIntrusivePtr<TStruct> oldStruct,
        TIntrusivePtr<TStruct> newStruct) const;

private:
    NDetail::TRegisteredFieldDirectoryPtr RegisteredFields_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define YSON_STRUCT_UPDATE_INL_H_
#include "yson_struct_update-inl.h"
#undef YSON_STRUCT_UPDATE_INL_H_
