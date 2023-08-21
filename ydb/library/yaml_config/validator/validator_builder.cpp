#include "validator_builder.h"

#include "validator.h"

#include <util/system/types.h>

#include <utility>

namespace NYamlConfig::NValidator {

namespace NDetail {

TSimpleSharedPtr<TValidator> CreateValidatorPtr(const TSimpleSharedPtr<TBuilder>& builder) {
    switch (builder->BuilderType_) {
        case EBuilderType::Generic: {
            TGenericValidator v = static_cast<TGenericBuilder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TGenericValidator(std::move(v)));
        }
        case EBuilderType::Map: {
            TMapValidator v = static_cast<TMapBuilder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TMapValidator(std::move(v)));
        }
        case EBuilderType::Array: {
            TArrayValidator v = static_cast<TArrayBuilder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TArrayValidator(std::move(v)));
        }
        case EBuilderType::Int64: {
            TInt64Validator v = static_cast<TInt64Builder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TInt64Validator(std::move(v)));
        }
        case EBuilderType::String: {
            TStringValidator v = static_cast<TStringBuilder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TStringValidator(std::move(v)));
        }
        case EBuilderType::Bool: {
            TBoolValidator v = static_cast<TBoolBuilder*>(builder.Get())->CreateValidator();
            v.Required_ = builder->Required_;
            return TSimpleSharedPtr<TValidator>(new TBoolValidator(std::move(v)));
        }
    }
}

TBuilder::TBuilder(EBuilderType builderType)
    : BuilderType_(builderType) {}

TBuilder& TBuilder::operator=(const TBuilder& builder) {
    Y_ASSERT(BuilderType_ == builder.BuilderType_);
    Required_ = builder.Required_;
    Description_ = builder.Description_;
    return *this;
}

TBuilder& TBuilder::operator=(TBuilder&& builder) {
    Y_ASSERT(BuilderType_ == builder.BuilderType_);
    Required_ = builder.Required_;
    Description_ = std::move(builder.Description_);
    return *this;
}

} // namespace NDetail

TGenericBuilder::NodeTypeAndBuilder::NodeTypeAndBuilder() {}

TGenericBuilder::NodeTypeAndBuilder::NodeTypeAndBuilder(EBuilderType type, TSimpleSharedPtr<TBuilder> builder)
    : Type(type)
    , Builder(std::move(builder)) {}


// TGenericBuilder
TGenericBuilder::TGenericBuilder()
    : TCommonBuilderOps<TGenericBuilder>(EBuilderType::Generic) {};

TGenericBuilder::TGenericBuilder(const TGenericBuilder& builder)
    : TCommonBuilderOps<TGenericBuilder>(builder)
    , PossibleBuilderPtrs_(builder.PossibleBuilderPtrs_) {}

TGenericBuilder::TGenericBuilder(TGenericBuilder&& builder)
    : TCommonBuilderOps<TGenericBuilder>(std::move(builder))
    , PossibleBuilderPtrs_(std::move(builder.PossibleBuilderPtrs_)) {}

TGenericBuilder& TGenericBuilder::operator=(const TGenericBuilder& builder) {
    TBuilder::operator=(builder);
    PossibleBuilderPtrs_ = builder.PossibleBuilderPtrs_;
    return *this;
}

TGenericBuilder& TGenericBuilder::operator=(TGenericBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    PossibleBuilderPtrs_ = std::move(builder.PossibleBuilderPtrs_);
    return *this;
}

TGenericBuilder::TGenericBuilder(std::function<void(TGenericBuilder&)> configurator)
    :TGenericBuilder() {
    configurator(*this);
}

TGenericBuilder& TGenericBuilder::CanBeMap(std::function<void(TMapBuilder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TMapBuilder(configurator));
    return *this;
}

TGenericBuilder& TGenericBuilder::CanBeArray(std::function<void(TArrayBuilder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TArrayBuilder(configurator));
    return *this;
}

TGenericBuilder& TGenericBuilder::CanBeInt64(std::function<void(TInt64Builder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TInt64Builder(configurator));
    return *this;
}

TGenericBuilder& TGenericBuilder::CanBeString(std::function<void(TStringBuilder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TStringBuilder(configurator));
    return *this;
}

TGenericBuilder& TGenericBuilder::CanBeBool(std::function<void(TBoolBuilder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TBoolBuilder(configurator));
    return *this;
}

TGenericValidator TGenericBuilder::CreateValidator() {
    TGenericValidator result;
    for (const auto& builderPtr : PossibleBuilderPtrs_) {
        result.AddValidator(NDetail::CreateValidatorPtr(builderPtr));
    }

    result.Required_ = Required_;

    return result;
}

TMapBuilder::TMapBuilder()
    : TCommonBuilderOps<TMapBuilder>(EBuilderType::Map) {}

TMapBuilder::TMapBuilder(const TMapBuilder& builder)
    : TCommonBuilderOps<TMapBuilder>(builder)
    , Children_(builder.Children_)
    , Opaque_(builder.Opaque_) {}

TMapBuilder::TMapBuilder(TMapBuilder&& builder)
    : TCommonBuilderOps<TMapBuilder>(std::move(builder))
    , Children_(std::move(builder.Children_))
    , Opaque_(builder.Opaque_) {}

TMapBuilder& TMapBuilder::operator=(const TMapBuilder& builder) {
    TBuilder::operator=(builder);
    Children_ = builder.Children_;
    Opaque_ = builder.Opaque_;
    return *this;
}

TMapBuilder& TMapBuilder::operator=(TMapBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    Children_ = std::move(builder.Children_);
    Opaque_ = builder.Opaque_;
    return *this;
}

TMapBuilder::TMapBuilder(std::function<void(TMapBuilder&)> configurator)
    :TMapBuilder() {
    configurator(*this);
}

void TMapBuilder::ThrowIfAlreadyHasField(const TString& field) {
    if (Children_.contains(field)) {
        ythrow BuilderException() << "Node already has field \"" << field << "\"";
    }
}

TMapBuilder& TMapBuilder::GenericField(const TString& field, std::function<void(TGenericBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TGenericBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Map(const TString& field, std::function<void(TMapBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TMapBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Array(const TString& field, std::function<void(TArrayBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TArrayBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Int64(const TString& field, std::function<void(TInt64Builder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TInt64Builder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::String(const TString& field, std::function<void(TStringBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TStringBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Bool(const TString& field, std::function<void(TBoolBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TBoolBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Opaque() {
    Opaque_ = true;
    return *this;
}

TMapBuilder& TMapBuilder::NotOpaque() {
    Opaque_ = false;
    return *this;
}

bool TMapBuilder::HasField(const TString& field) {
    return Children_.contains(field);
}

TGenericBuilder& TMapBuilder::GenericFieldAt(const TString& field) {
    return static_cast<TGenericBuilder&>(*Children_.at(field).Get());
}

TMapBuilder& TMapBuilder::MapAt(const TString& field) {
    return static_cast<TMapBuilder&>(*Children_.at(field).Get());
}

TArrayBuilder& TMapBuilder::ArrayAt(const TString& field) {
    return static_cast<TArrayBuilder&>(*Children_.at(field).Get());
}

TInt64Builder& TMapBuilder::Int64At(const TString& field) {
    return static_cast<TInt64Builder&>(*Children_.at(field).Get());
}

TStringBuilder& TMapBuilder::StringAt(const TString& field) {
    return static_cast<TStringBuilder&>(*Children_.at(field).Get());
}

TBoolBuilder& TMapBuilder::BoolAt(const TString& field) {
    return static_cast<TBoolBuilder&>(*Children_.at(field).Get());
}

TMapValidator TMapBuilder::CreateValidator() {
    THashMap<TString, TSimpleSharedPtr<TValidator>> children;
    for (const auto& [name, builderPtr] : Children_) {
        children[name] = NDetail::CreateValidatorPtr(builderPtr);
    }

    return TMapValidator(std::move(children), Opaque_);
}


// TArrayBuilder
TArrayBuilder::TArrayBuilder()
    : TCommonBuilderOps<TArrayBuilder>(EBuilderType::Array) {}

TArrayBuilder::TArrayBuilder(const TArrayBuilder& builder)
    : TCommonBuilderOps<TArrayBuilder>(builder)
    , ItemPtr_(builder.ItemPtr_)
    , Unique_(builder.Unique_) {}

TArrayBuilder::TArrayBuilder(TArrayBuilder&& builder)
    : TCommonBuilderOps<TArrayBuilder>(std::move(builder))
    , ItemPtr_(std::move(builder.ItemPtr_))
    , Unique_(builder.Unique_) {}

TArrayBuilder& TArrayBuilder::operator=(const TArrayBuilder& builder) {
    TBuilder::operator=(builder);
    ItemPtr_ = builder.ItemPtr_;
    Unique_ = builder.Unique_;
    return *this;
}

TArrayBuilder& TArrayBuilder::operator=(TArrayBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    ItemPtr_ = std::move(builder.ItemPtr_);
    Unique_ = builder.Unique_;
    return *this;
}

TArrayBuilder::TArrayBuilder(std::function<void(TArrayBuilder&)> configurator)
    :TArrayBuilder() {
    configurator(*this);
}

TArrayBuilder& TArrayBuilder::Unique() {
    Unique_ = true;
    return *this;
}

TArrayBuilder& TArrayBuilder::MapItem(std::function<void(TMapBuilder&)> configurator) {
    Item(TMapBuilder(configurator));
    return *this;
}

TArrayBuilder& TArrayBuilder::ArrayItem(std::function<void(TArrayBuilder&)> configurator) {
    Item(TArrayBuilder(configurator));
    return *this;
}

TArrayBuilder& TArrayBuilder::Int64Item(std::function<void(TInt64Builder&)> configurator) {
    Item(TInt64Builder(configurator));
    return *this;
}

TArrayBuilder& TArrayBuilder::StringItem(std::function<void(TStringBuilder&)> configurator) {
    Item(TStringBuilder(configurator));
    return *this;
}

TArrayBuilder& TArrayBuilder::BoolItem(std::function<void(TBoolBuilder&)> configurator) {
    Item(TBoolBuilder(configurator));
    return *this;
}

NDetail::TBuilder& TArrayBuilder::GetItem() {
    if (ItemPtr_.Get() == nullptr) {
        ythrow BuilderException() << "There is no item builder yet";
    }
    return *ItemPtr_.Get();
}

TArrayValidator TArrayBuilder::CreateValidator() {
    return TArrayValidator(NDetail::CreateValidatorPtr(ItemPtr_), Unique_);
}


// TInt64Builder
TInt64Builder::TInt64Builder()
    : TCommonBuilderOps<TInt64Builder>(EBuilderType::Int64) {}

TInt64Builder::TInt64Builder(const TInt64Builder& builder)
    : TCommonBuilderOps<TInt64Builder>(builder)
    , Min_(builder.Min_)
    , Max_(builder.Max_) {}

TInt64Builder::TInt64Builder(TInt64Builder&& builder)
    : TCommonBuilderOps<TInt64Builder>(std::move(builder))
    , Min_(builder.Min_)
    , Max_(builder.Max_) {}

TInt64Builder& TInt64Builder::operator=(const TInt64Builder& builder) {
    TBuilder::operator=(builder);
    Min_ = builder.Min_;
    Max_ = builder.Max_;
    return *this;
}

TInt64Builder& TInt64Builder::operator=(TInt64Builder&& builder) {
    TBuilder::operator=(std::move(builder));
    Min_ = builder.Min_;
    Max_ = builder.Max_;
    return *this;
}

TInt64Builder::TInt64Builder(std::function<void(TInt64Builder&)> configurator)
    :TInt64Builder() {
    configurator(*this);
}

TInt64Builder& TInt64Builder::Min(i64 min) {
    Min_ = min;
    return *this;
}

TInt64Builder& TInt64Builder::Max(i64 max) {
    Max_ = max;
    return *this;
}

TInt64Builder& TInt64Builder::Range(i64 min, i64 max) {
    Max_ = max;
    Min_ = min;
    return *this;
}

TInt64Validator TInt64Builder::CreateValidator() {
    return TInt64Validator(Min_, Max_);
}


// TStringBuilder
TStringBuilder::TStringBuilder()
    : TCommonBuilderOps<TStringBuilder>(EBuilderType::Int64) {}

TStringBuilder::TStringBuilder(const TStringBuilder& builder)
    : TCommonBuilderOps<TStringBuilder>(builder) {}

TStringBuilder::TStringBuilder(TStringBuilder&& builder)
    : TCommonBuilderOps<TStringBuilder>(std::move(builder)) {}

TStringBuilder& TStringBuilder::operator=(const TStringBuilder& builder) {
    TBuilder::operator=(builder);
    return *this;
}

TStringBuilder& TStringBuilder::operator=(TStringBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    return *this;
}

TStringBuilder::TStringBuilder(std::function<void(TStringBuilder&)> configurator)
    :TStringBuilder() {
    configurator(*this);
}

TStringValidator TStringBuilder::CreateValidator() {
    return TStringValidator();
}


// TBoolBuilder
TBoolBuilder::TBoolBuilder()
    : TCommonBuilderOps<TBoolBuilder>(EBuilderType::Bool) {}

TBoolBuilder::TBoolBuilder(const TBoolBuilder& builder)
    : TCommonBuilderOps<TBoolBuilder>(builder) {}

TBoolBuilder::TBoolBuilder(TBoolBuilder&& builder)
    : TCommonBuilderOps<TBoolBuilder>(std::move(builder)) {}

TBoolBuilder& TBoolBuilder::operator=(const TBoolBuilder& builder) {
    TBuilder::operator=(builder);
    return *this;
}

TBoolBuilder& TBoolBuilder::operator=(TBoolBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    return *this;
}

TBoolBuilder::TBoolBuilder(std::function<void(TBoolBuilder&)> configurator)
    :TBoolBuilder() {
    configurator(*this);
}

TBoolValidator TBoolBuilder::CreateValidator() {
    return TBoolValidator();
}

} // namespace NYamlConfig::NValidator
