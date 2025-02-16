#pragma once

#include "yson_struct.h"
#include "yson_struct_detail.h"

namespace NYT::NYTree::NYsonStructUpdate {

////////////////////////////////////////////////////////////////////////////////

template <CYsonStructDerived TStruct>
class TConfigurator;

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
        TCallback<void(TConfigurator<TUnwrappedValue>)> registerCb);

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

private:
    NDetail::TRegisteredFieldDirectoryPtr RegisteredFields_;

    template <class TUpdateStruct>
    friend void Update(
        const TConfigurator<TUpdateStruct>& registrar,
        TIntrusivePtr<TUpdateStruct> old,
        TIntrusivePtr<TUpdateStruct> new_);
};

////////////////////////////////////////////////////////////////////////////////

template <class TStruct>
void Update(
    const TConfigurator<TStruct>& registrar,
    TIntrusivePtr<TStruct> oldStruct,
    TIntrusivePtr<TStruct> newStruct);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree::NYsonStructUpdate

#define YSON_STRUCT_UPDATE_INL_H_
#include "yson_struct_update-inl.h"
#undef YSON_STRUCT_UPDATE_INL_H_
