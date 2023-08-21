#pragma once

#include "validator.h"

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/string/cast.h>
#include <util/system/types.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

#include <functional>

namespace NYamlConfig::NValidator {

enum class EBuilderType {
    Generic, Map, Array, Int64, String, Bool
};

class BuilderException : public yexception {};

class TGenericBuilder;
class TMapBuilder;
class TArrayBuilder;
class TInt64Builder;
class TStringBuilder;
class TBoolBuilder;

namespace NDetail {

class TBuilder;

TSimpleSharedPtr<TValidator> CreateValidatorPtr(const TSimpleSharedPtr<TBuilder>& builder);

class TBuilder {
    friend TSimpleSharedPtr<TValidator> NDetail::CreateValidatorPtr(const TSimpleSharedPtr<TBuilder>& builder);

public:
    TBuilder(EBuilderType builderType);

    TBuilder(const TBuilder& builder) = default;
    TBuilder(TBuilder&& builder) = default;

    TBuilder& operator=(const TBuilder& builder);
    TBuilder& operator=(TBuilder&& builder);

    virtual ~TBuilder() = default;

protected:
    const EBuilderType BuilderType_;
    bool Required_ = true;
    TString Description_;
};

template <typename ThisType>
class TCommonBuilderOps : public TBuilder {
public:
    TCommonBuilderOps<ThisType>(EBuilderType builderType);
    TCommonBuilderOps<ThisType>(const TBuilder& builder);

    ThisType& Optional();
    ThisType& Required();
    ThisType& Description(const TString& description);

    ThisType& Configure(std::function<void(ThisType&)> configurator = [](auto&){});

private:
    ThisType& AsDerived();
};

} // namespace NDetail

class TGenericBuilder : public NDetail::TCommonBuilderOps<TGenericBuilder> {
public:
    TGenericBuilder();

    TGenericBuilder(const TGenericBuilder& builder);
    TGenericBuilder(TGenericBuilder&& builder);

    TGenericBuilder& operator=(const TGenericBuilder& builder);
    TGenericBuilder& operator=(TGenericBuilder&& builder);

    TGenericBuilder(std::function<void(TGenericBuilder&)> configurator);

    template <typename Builder>
    TGenericBuilder& CanBe(Builder builder);
    TGenericBuilder& CanBeMap(std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeArray(std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeInt64(std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeString(std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeBool(std::function<void(TBoolBuilder&)> configurator = [](auto&){});

    TGenericValidator CreateValidator();

private:
    struct NodeTypeAndBuilder {
        EBuilderType Type;
        TSimpleSharedPtr<TBuilder> Builder;

        NodeTypeAndBuilder();
        NodeTypeAndBuilder(EBuilderType type, TSimpleSharedPtr<TBuilder> builder);
    };

    TVector<TSimpleSharedPtr<TBuilder>> PossibleBuilderPtrs_;
};

class TMapBuilder : public NDetail::TCommonBuilderOps<TMapBuilder> {
public:
    TMapBuilder();

    TMapBuilder(const TMapBuilder& builder);
    TMapBuilder(TMapBuilder&& builder);

    TMapBuilder& operator=(const TMapBuilder& builder);
    TMapBuilder& operator=(TMapBuilder&& builder);

    TMapBuilder(std::function<void(TMapBuilder&)> configurator);

    template <typename Builder>
    TMapBuilder& Field(const TString& field, Builder builder);

    TMapBuilder& GenericField(const TString& field, std::function<void(TGenericBuilder&)> configurator = [](auto&){});
    TMapBuilder& Map(const TString& field, std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TMapBuilder& Array(const TString& field, std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TMapBuilder& Int64(const TString& field, std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TMapBuilder& String(const TString& field, std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TMapBuilder& Bool(const TString& field, std::function<void(TBoolBuilder&)> configurator = [](auto&){});

    TMapBuilder& Opaque();
    TMapBuilder& NotOpaque();

    bool HasField(const TString& field);

    TGenericBuilder& GenericFieldAt(const TString& field);
    TMapBuilder& MapAt(const TString& field);
    TArrayBuilder& ArrayAt(const TString& field);
    TInt64Builder& Int64At(const TString& field);
    TStringBuilder& StringAt(const TString& field);
    TBoolBuilder& BoolAt(const TString& field);

    TMapValidator CreateValidator();

private:
    THashMap<TString, TSimpleSharedPtr<TBuilder>> Children_;
    bool Opaque_ = false;

    void ThrowIfAlreadyHasField(const TString& field);
};

class TArrayBuilder : public NDetail::TCommonBuilderOps<TArrayBuilder> {
public:
    TArrayBuilder();

    TArrayBuilder(const TArrayBuilder& builder);
    TArrayBuilder(TArrayBuilder&& builder);

    TArrayBuilder& operator=(const TArrayBuilder& builder);
    TArrayBuilder& operator=(TArrayBuilder&& builder);

    TArrayBuilder(std::function<void(TArrayBuilder&)> configurator);

    TArrayBuilder& Unique();

    template <typename Builder>
    TArrayBuilder& Item(Builder builder);
    TArrayBuilder& MapItem(std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TArrayBuilder& ArrayItem(std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TArrayBuilder& Int64Item(std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TArrayBuilder& StringItem(std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TArrayBuilder& BoolItem(std::function<void(TBoolBuilder&)> configurator = [](auto&){});

    TBuilder& GetItem();

    TArrayValidator CreateValidator();

private:
    TSimpleSharedPtr<TBuilder> ItemPtr_;
    bool Unique_ = false;
};

class TInt64Builder : public NDetail::TCommonBuilderOps<TInt64Builder> {
public:
    TInt64Builder();

    TInt64Builder(const TInt64Builder& builder);
    TInt64Builder(TInt64Builder&& builder);

    TInt64Builder& operator=(const TInt64Builder& builder);
    TInt64Builder& operator=(TInt64Builder&& builder);

    TInt64Builder(std::function<void(TInt64Builder&)> configurator);

    TInt64Builder& Min(i64 min);
    TInt64Builder& Max(i64 max);
    TInt64Builder& Range(i64 min, i64 max);

    TInt64Validator CreateValidator();

private:
    i64 Min_ = ::Min<i64>();
    i64 Max_ = ::Max<i64>();
};

class TStringBuilder : public NDetail::TCommonBuilderOps<TStringBuilder> {
public:
    TStringBuilder();

    TStringBuilder(const TStringBuilder& builder);
    TStringBuilder(TStringBuilder&& builder);

    TStringBuilder& operator=(const TStringBuilder& builder);
    TStringBuilder& operator=(TStringBuilder&& builder);

    TStringBuilder(std::function<void(TStringBuilder&)> configurator);

    TStringValidator CreateValidator();
};

class TBoolBuilder : public NDetail::TCommonBuilderOps<TBoolBuilder> {
public:
    TBoolBuilder();

    TBoolBuilder(const TBoolBuilder& builder);
    TBoolBuilder(TBoolBuilder&& builder);

    TBoolBuilder& operator=(const TBoolBuilder& builder);
    TBoolBuilder& operator=(TBoolBuilder&& builder);

    TBoolBuilder(std::function<void(TBoolBuilder&)> configurator);

    TBoolValidator CreateValidator();
};

template <typename ThisType>
NDetail::TCommonBuilderOps<ThisType>::TCommonBuilderOps(EBuilderType builderType)
    : TBuilder(builderType) {}

template <typename ThisType>
NDetail::TCommonBuilderOps<ThisType>::TCommonBuilderOps(const TBuilder& builder)
    : TBuilder(builder) {}

template <typename ThisType>
ThisType& NDetail::TCommonBuilderOps<ThisType>::AsDerived() {
    return static_cast<ThisType&>(*this);
}

template <typename ThisType>
ThisType& NDetail::TCommonBuilderOps<ThisType>::Optional() {
    Required_ = false;
    return AsDerived();
}

template <typename ThisType>
ThisType& NDetail::TCommonBuilderOps<ThisType>::Required() {
    Required_ = true;
    return AsDerived();
}

template <typename ThisType>
ThisType& NDetail::TCommonBuilderOps<ThisType>::Description(const TString& description) {
    Description_ = description;
    return AsDerived();
}

template <typename ThisType>
ThisType& NDetail::TCommonBuilderOps<ThisType>::Configure(std::function<void(ThisType&)> configurator) {
    configurator(AsDerived());
    return AsDerived();
}

template <typename Builder>
TGenericBuilder& TGenericBuilder::CanBe(Builder builder) {
    PossibleBuilderPtrs_.emplace_back(new Builder(std::move(builder)));
    return *this;
}

template <typename Builder>
TMapBuilder& TMapBuilder::Field(const TString& field, Builder builder) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new Builder(std::move(builder)));
    return *this;
}

template <typename Builder>
TArrayBuilder& TArrayBuilder::Item(Builder builder) {
    ItemPtr_ = MakeSimpleShared<Builder>(std::move(builder));
    return *this;
}

} // namespace NYamlConfig::NValidator
