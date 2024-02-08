#pragma once

#include "fwd.h"

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/string/cast.h>
#include <util/system/types.h>
#include <util/generic/yexception.h>
#include <util/generic/ylimits.h>

#include <functional>

namespace NKikimr::NYamlConfig::NValidator {

class TBuilderException : public yexception {};

namespace NDetail {

class TBuilder {
    friend TSimpleSharedPtr<TValidator> NDetail::CreateValidatorPtr(const TSimpleSharedPtr<TBuilder>& builder);

public:
    TBuilder(ENodeType nodeType);

    TBuilder(const TBuilder& builder) = default;
    TBuilder(TBuilder&& builder) = default;

    TBuilder& operator=(const TBuilder& builder);
    TBuilder& operator=(TBuilder&& builder);

    virtual ~TBuilder() = default;

protected:
    const ENodeType NodeType_;
    bool Required_ = true;
    TString Description_;
};

template <typename TThis, typename TCheckContext>
class TCommonBuilderOps : public TBuilder {
public:
    TCommonBuilderOps<TThis, TCheckContext>(ENodeType nodeType);
    TCommonBuilderOps<TThis, TCheckContext>(const TCommonBuilderOps<TThis, TCheckContext>& builder);

    TThis& Optional();
    TThis& Required();
    TThis& Description(const TString& description);

    TThis& Configure(std::function<void(TThis&)> configurator = [](auto&){});

    TThis& AddCheck(TString name, std::function<void(TCheckContext&)> checker);

protected:
    THashMap<TString, std::function<void(TCheckContext&)>> Checkers_;

private:
    TThis& AsDerived();
};

} // namespace NDetail

class TGenericBuilder : public NDetail::TCommonBuilderOps<TGenericBuilder, TGenericCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TGenericBuilder, TGenericCheckContext>;

public:
    TGenericBuilder();

    TGenericBuilder(const TGenericBuilder& builder);
    TGenericBuilder(TGenericBuilder&& builder);

    TGenericBuilder& operator=(const TGenericBuilder& builder);
    TGenericBuilder& operator=(TGenericBuilder&& builder);

    TGenericBuilder(std::function<void(TGenericBuilder&)> configurator);

    template <typename TBuilder>
    TGenericBuilder& CanBe(TBuilder builder);
    TGenericBuilder& CanBeMap(std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeArray(std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeInt64(std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeInt64(i64 min, i64 max);
    TGenericBuilder& CanBeString(std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeBool(std::function<void(TBoolBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeEnum(std::function<void(TEnumBuilder&)> configurator = [](auto&){});
    TGenericBuilder& CanBeEnum(THashSet<TString> items);

    TGenericValidator CreateValidator();

private:
    struct TTypedBuilder {
        ENodeType Type;
        TSimpleSharedPtr<TBuilder> Builder;

        TTypedBuilder();
        TTypedBuilder(ENodeType type, TSimpleSharedPtr<TBuilder> builder);
    };

    TVector<TSimpleSharedPtr<TBuilder>> PossibleBuilderPtrs_;
};

class TMapBuilder : public NDetail::TCommonBuilderOps<TMapBuilder, TMapCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TMapBuilder, TMapCheckContext>;

public:
    TMapBuilder();

    TMapBuilder(const TMapBuilder& builder);
    TMapBuilder(TMapBuilder&& builder);

    TMapBuilder& operator=(const TMapBuilder& builder);
    TMapBuilder& operator=(TMapBuilder&& builder);

    TMapBuilder(std::function<void(TMapBuilder&)> configurator);

    template <typename TBuilder>
    TMapBuilder& Field(const TString& field, TBuilder builder);

    TMapBuilder& GenericField(const TString& field, std::function<void(TGenericBuilder&)> configurator = [](auto&){});
    TMapBuilder& Map(const TString& field, std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TMapBuilder& Array(const TString& field, std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TMapBuilder& Int64(const TString& field, std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TMapBuilder& Int64(const TString& field, i64 min, i64 max);
    TMapBuilder& String(const TString& field, std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TMapBuilder& Bool(const TString& field, std::function<void(TBoolBuilder&)> configurator = [](auto&){});
    TMapBuilder& Enum(const TString& field, std::function<void(TEnumBuilder&)> configurator = [](auto&){});
    TMapBuilder& Enum(const TString& field, THashSet<TString> items);

    TMapBuilder& Opaque();
    TMapBuilder& NotOpaque();

    bool HasField(const TString& field);

    TGenericBuilder& GenericFieldAt(const TString& field);
    TMapBuilder& MapAt(const TString& field);
    TArrayBuilder& ArrayAt(const TString& field);
    TInt64Builder& Int64At(const TString& field);
    TStringBuilder& StringAt(const TString& field);
    TBoolBuilder& BoolAt(const TString& field);
    TEnumBuilder& EnumAt(const TString& field);

    TMapValidator CreateValidator();

private:
    THashMap<TString, TSimpleSharedPtr<TBuilder>> Children_;
    bool Opaque_ = true;

    void ThrowIfAlreadyHasField(const TString& field);
};

class TArrayBuilder : public NDetail::TCommonBuilderOps<TArrayBuilder, TArrayCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TArrayBuilder, TArrayCheckContext>;

public:
    TArrayBuilder();

    TArrayBuilder(const TArrayBuilder& builder);
    TArrayBuilder(TArrayBuilder&& builder);

    TArrayBuilder& operator=(const TArrayBuilder& builder);
    TArrayBuilder& operator=(TArrayBuilder&& builder);

    TArrayBuilder(std::function<void(TArrayBuilder&)> configurator);

    TArrayBuilder& Unique();

    template <typename TBuilder>
    TArrayBuilder& Item(TBuilder builder);
    TArrayBuilder& MapItem(std::function<void(TMapBuilder&)> configurator = [](auto&){});
    TArrayBuilder& ArrayItem(std::function<void(TArrayBuilder&)> configurator = [](auto&){});
    TArrayBuilder& Int64Item(std::function<void(TInt64Builder&)> configurator = [](auto&){});
    TArrayBuilder& Int64Item(i64 min, i64 max);
    TArrayBuilder& StringItem(std::function<void(TStringBuilder&)> configurator = [](auto&){});
    TArrayBuilder& BoolItem(std::function<void(TBoolBuilder&)> configurator = [](auto&){});
    TArrayBuilder& EnumItem(std::function<void(TEnumBuilder&)> configurator = [](auto&){});
    TArrayBuilder& EnumItem(THashSet<TString> items);

    TBuilder& GetItem();

    TArrayValidator CreateValidator();

private:
    TSimpleSharedPtr<TBuilder> ItemPtr_;
    bool Unique_ = false;
};

class TInt64Builder : public NDetail::TCommonBuilderOps<TInt64Builder, TInt64CheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TInt64Builder, TInt64CheckContext>;

public:
    TInt64Builder();
    TInt64Builder(i64 min, i64 max);

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

class TStringBuilder : public NDetail::TCommonBuilderOps<TStringBuilder, TStringCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TStringBuilder, TStringCheckContext>;

public:
    TStringBuilder();

    TStringBuilder(const TStringBuilder& builder);
    TStringBuilder(TStringBuilder&& builder);

    TStringBuilder& operator=(const TStringBuilder& builder);
    TStringBuilder& operator=(TStringBuilder&& builder);

    TStringBuilder(std::function<void(TStringBuilder&)> configurator);

    TStringValidator CreateValidator();
};

class TBoolBuilder : public NDetail::TCommonBuilderOps<TBoolBuilder, TBoolCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TBoolBuilder, TBoolCheckContext>;

public:
    TBoolBuilder();

    TBoolBuilder(const TBoolBuilder& builder);
    TBoolBuilder(TBoolBuilder&& builder);

    TBoolBuilder& operator=(const TBoolBuilder& builder);
    TBoolBuilder& operator=(TBoolBuilder&& builder);

    TBoolBuilder(std::function<void(TBoolBuilder&)> configurator);

    TBoolValidator CreateValidator();
};

class TEnumBuilder : public NDetail::TCommonBuilderOps<TEnumBuilder, TEnumCheckContext> {
    using TBase = NDetail::TCommonBuilderOps<TEnumBuilder, TEnumCheckContext>;

public:
    TEnumBuilder(THashSet<TString> Items_);

    TEnumBuilder(const TEnumBuilder& builder);
    TEnumBuilder(TEnumBuilder&& builder);

    TEnumBuilder& operator=(const TEnumBuilder& builder);
    TEnumBuilder& operator=(TEnumBuilder&& builder);

    TEnumBuilder(std::function<void(TEnumBuilder&)> configurator);

    TEnumBuilder& SetItems(THashSet<TString> items);

    TEnumValidator CreateValidator();

private:
    THashSet<TString> Items_;
};

template <typename TThis, typename TCheckContext>
NDetail::TCommonBuilderOps<TThis, TCheckContext>::TCommonBuilderOps(ENodeType nodeType)
    : TBuilder(nodeType) {}

template <typename TThis, typename TCheckContext>
NDetail::TCommonBuilderOps<TThis, TCheckContext>::TCommonBuilderOps(const TCommonBuilderOps<TThis, TCheckContext>& builder)
    : TBuilder(builder), Checkers_(builder.Checkers_) {}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::AsDerived() {
    return static_cast<TThis&>(*this);
}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::Optional() {
    Required_ = false;
    return AsDerived();
}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::Required() {
    Required_ = true;
    return AsDerived();
}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::Description(const TString& description) {
    Description_ = description;
    return AsDerived();
}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::Configure(std::function<void(TThis&)> configurator) {
    configurator(AsDerived());
    return AsDerived();
}

template <typename TThis, typename TCheckContext>
TThis& NDetail::TCommonBuilderOps<TThis, TCheckContext>::AddCheck(TString name, std::function<void(TCheckContext&)> checker) {
    if (Checkers_.contains(name)) {
        ythrow TBuilderException() << "Already has check named \"" << name << "\"";
    }
    Checkers_[name] = checker;
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

} // namespace NKikimr::NYamlConfig::NValidator
