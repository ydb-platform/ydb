#pragma once

#include "yson_struct.h"
#include "yson_struct_detail.h"

namespace NYT::NYTree::NYsonStructUpdate {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

DECLARE_REFCOUNTED_STRUCT(TRegisteredFieldDirectory);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

struct IFieldRegistrar
    : public TRefCounted
{
    virtual void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* old,
        TYsonStructBase* new_) const = 0;
};

DECLARE_REFCOUNTED_STRUCT(IFieldRegistrar);
DEFINE_REFCOUNTED_TYPE(IFieldRegistrar);

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
struct TFieldRegistrar;

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
struct TConfigurator
{
    explicit TConfigurator(NDetail::TRegisteredFieldDirectoryPtr state = {});

    template <class TValue>
    TFieldRegistrar<TValue>& Field(const TString& name, TYsonStructField<TStruct, TValue> field);

    // Converts to a registrar of a base class
    template <class TAncestor>
    operator TConfigurator<TAncestor>() const;

private:
    NDetail::TRegisteredFieldDirectoryPtr RegisteredFields_;

    template <class TUpdateStruct>
    friend void Update(
        const TConfigurator<TUpdateStruct>& registrar,
        TIntrusivePtr<TUpdateStruct> old,
        TIntrusivePtr<TUpdateStruct> new_);
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
struct TFieldRegistrar
    : public IFieldRegistrar
{
    // Registers updater that accepts old and new values as arguments.
    TFieldRegistrar& Updater(TCallback<void(const TValue&, const TValue&)> updater);

    // Registers updater that accepts only new value as an argument.
    TFieldRegistrar& Updater(TCallback<void(const TValue&)> updater);

    // Registers nested YsonStruct to be updated recursively.
    template <CYsonStructDerived TUnwrappedValue>
    TFieldRegistrar& NestedUpdater(
        TCallback<void(TConfigurator<TUnwrappedValue>)> registerCb);

    void DoUpdate(
        IYsonStructParameterPtr parameter,
        TYsonStructBase* old,
        TYsonStructBase* new_) const override;

private:
    void VerifyEmpty() const;

    TCallback<void(const TValue&, const TValue&)> Updater_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
void Update(
    const TConfigurator<TStruct>& registrar,
    TIntrusivePtr<TStruct> old,
    TIntrusivePtr<TStruct> new_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NYsonStructUpdate

#define YSON_STRUCT_UPDATE_INL_H_
#include "yson_struct_update-inl.h"
#undef YSON_STRUCT_UPDATE_INL_H_
