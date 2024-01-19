#include "validator_builder.h"

#include "validator.h"

#include <util/system/types.h>

#include <utility>

namespace NKikimr::NYamlConfig::NValidator {

namespace NDetail {

TSimpleSharedPtr<TValidator> CreateValidatorPtr(const TSimpleSharedPtr<TBuilder>& builder) {
    switch (builder->NodeType_) {
        case ENodeType::Generic: {
            return MakeSimpleShared<TGenericValidator>(static_cast<TGenericBuilder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::Map: {
            return MakeSimpleShared<TMapValidator>(static_cast<TMapBuilder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::Array: {
            return MakeSimpleShared<TArrayValidator>(static_cast<TArrayBuilder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::Int64: {
            return MakeSimpleShared<TInt64Validator>(static_cast<TInt64Builder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::String: {
            return MakeSimpleShared<TStringValidator>(static_cast<TStringBuilder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::Bool: {
            return MakeSimpleShared<TBoolValidator>(static_cast<TBoolBuilder*>(builder.Get())->CreateValidator());
        }
        case ENodeType::Enum: {
            return MakeSimpleShared<TEnumValidator>(static_cast<TEnumBuilder*>(builder.Get())->CreateValidator());
        }
    }
}

TBuilder::TBuilder(ENodeType nodeType)
    : NodeType_(nodeType) {}

TBuilder& TBuilder::operator=(const TBuilder& builder) {
    Y_ASSERT(NodeType_ == builder.NodeType_);
    Required_ = builder.Required_;
    Description_ = builder.Description_;
    return *this;
}

TBuilder& TBuilder::operator=(TBuilder&& builder) {
    Y_ASSERT(NodeType_ == builder.NodeType_);
    Required_ = builder.Required_;
    Description_ = std::move(builder.Description_);
    return *this;
}

} // namespace NDetail

TGenericBuilder::TTypedBuilder::TTypedBuilder() {}

TGenericBuilder::TTypedBuilder::TTypedBuilder(ENodeType type, TSimpleSharedPtr<TBuilder> builder)
    : Type(type)
    , Builder(std::move(builder)) {}


// TGenericBuilder
TGenericBuilder::TGenericBuilder()
    : TBase(ENodeType::Generic) {};

TGenericBuilder::TGenericBuilder(const TGenericBuilder& builder)
    : TBase(builder)
    , PossibleBuilderPtrs_(builder.PossibleBuilderPtrs_) {}

TGenericBuilder::TGenericBuilder(TGenericBuilder&& builder)
    : TBase(std::move(builder))
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

TGenericBuilder& TGenericBuilder::CanBeInt64(i64 min, i64 max) {
    PossibleBuilderPtrs_.emplace_back(new TInt64Builder(min, max));
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

TGenericBuilder& TGenericBuilder::CanBeEnum(std::function<void(TEnumBuilder&)> configurator) {
    PossibleBuilderPtrs_.emplace_back(new TEnumBuilder(configurator));
    return *this;
}

TGenericBuilder& TGenericBuilder::CanBeEnum(THashSet<TString> items) {
    PossibleBuilderPtrs_.emplace_back(new TEnumBuilder(std::move(items)));
    return *this;
}

TGenericValidator TGenericBuilder::CreateValidator() {
    TGenericValidator result;
    for (const auto& builderPtr : PossibleBuilderPtrs_) {
        result.AddValidator(NDetail::CreateValidatorPtr(builderPtr));
    }

    result.Checkers_ = Checkers_;
    result.Required_ = Required_;

    return result;
}

TMapBuilder::TMapBuilder()
    : TBase(ENodeType::Map) {}

TMapBuilder::TMapBuilder(const TMapBuilder& builder)
    : TBase(builder)
    , Children_(builder.Children_)
    , Opaque_(builder.Opaque_) {}

TMapBuilder::TMapBuilder(TMapBuilder&& builder)
    : TBase(std::move(builder))
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
        ythrow TBuilderException() << "Node already has field \"" << field << "\"";
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

TMapBuilder& TMapBuilder::Int64(const TString& field, i64 min, i64 max) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TInt64Builder(min, max));
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

TMapBuilder& TMapBuilder::Enum(const TString& field, std::function<void(TEnumBuilder&)> configurator) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TEnumBuilder(configurator));
    return *this;
}

TMapBuilder& TMapBuilder::Enum(const TString& field, THashSet<TString> items) {
    ThrowIfAlreadyHasField(field);
    Children_[field] = TSimpleSharedPtr<TBuilder>(new TEnumBuilder(std::move(items)));
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

TEnumBuilder& TMapBuilder::EnumAt(const TString& field) {
    return static_cast<TEnumBuilder&>(*Children_.at(field).Get());
}

TMapValidator TMapBuilder::CreateValidator() {
    THashMap<TString, TSimpleSharedPtr<TValidator>> children;
    for (const auto& [name, builderPtr] : Children_) {
        children[name] = NDetail::CreateValidatorPtr(builderPtr);
    }

    auto result = TMapValidator(std::move(children), Opaque_);

    result.Checkers_ = Checkers_;
    result.Required_ = Required_;

    return result;
}


// TArrayBuilder
TArrayBuilder::TArrayBuilder()
    : TBase(ENodeType::Array) {}

TArrayBuilder::TArrayBuilder(const TArrayBuilder& builder)
    : TBase(builder)
    , ItemPtr_(builder.ItemPtr_)
    , Unique_(builder.Unique_) {}

TArrayBuilder::TArrayBuilder(TArrayBuilder&& builder)
    : TBase(std::move(builder))
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

TArrayBuilder& TArrayBuilder::Int64Item(i64 min, i64 max) {
    Item(TInt64Builder(min, max));
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

TArrayBuilder& TArrayBuilder::EnumItem(std::function<void(TEnumBuilder&)> configurator) {
    Item(TEnumBuilder(configurator));
    return *this;
}

TArrayBuilder& TArrayBuilder::EnumItem(THashSet<TString> items) {
    Item(TEnumBuilder(items));
    return *this;
}

NDetail::TBuilder& TArrayBuilder::GetItem() {
    if (ItemPtr_.Get() == nullptr) {
        ythrow TBuilderException() << "There is no item builder yet";
    }
    return *ItemPtr_.Get();
}

TArrayValidator TArrayBuilder::CreateValidator() {
    auto result = TArrayValidator(NDetail::CreateValidatorPtr(ItemPtr_), Unique_);
    result.Checkers_ = Checkers_;
    result.Required_ = Required_;
    return result;
}


// TInt64Builder
TInt64Builder::TInt64Builder()
    : TBase(ENodeType::Int64) {}

TInt64Builder::TInt64Builder(i64 min, i64 max)
    : TBase(ENodeType::Int64), Min_(min), Max_(max) {}

TInt64Builder::TInt64Builder(const TInt64Builder& builder)
    : TBase(builder)
    , Min_(builder.Min_)
    , Max_(builder.Max_) {}

TInt64Builder::TInt64Builder(TInt64Builder&& builder)
    : TBase(std::move(builder))
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
    auto result = TInt64Validator(Min_, Max_);
    result.Checkers_ = Checkers_;
    result.Required_ = Required_;
    return result;
}


// TStringBuilder
TStringBuilder::TStringBuilder()
    : TBase(ENodeType::String) {}

TStringBuilder::TStringBuilder(const TStringBuilder& builder)
    : TBase(builder) {}

TStringBuilder::TStringBuilder(TStringBuilder&& builder)
    : TBase(std::move(builder)) {}

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
    auto result = TStringValidator();
    result.Checkers_ = Checkers_;
    result.Required_ = Required_;
    return result;
}


// TBoolBuilder
TBoolBuilder::TBoolBuilder()
    : TBase(ENodeType::Bool) {}

TBoolBuilder::TBoolBuilder(const TBoolBuilder& builder)
    : TBase(builder) {}

TBoolBuilder::TBoolBuilder(TBoolBuilder&& builder)
    : TBase(std::move(builder)) {}

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
    auto result = TBoolValidator();
    result.Checkers_ = Checkers_;
    result.Required_ = Required_;
    return result;
}


// TEnumBuilder
TEnumBuilder::TEnumBuilder(THashSet<TString> items)
    : TBase(ENodeType::Enum) {
        for (TString item : items) {
            item.to_lower();
            Items_.insert(item);
        }
    }

TEnumBuilder::TEnumBuilder(const TEnumBuilder& builder)
    : TBase(builder), Items_(builder.Items_) {}

TEnumBuilder::TEnumBuilder(TEnumBuilder&& builder)
    : TBase(std::move(builder)), Items_(std::move(builder.Items_)) {
        for (TString item : builder.Items_) {
            item.to_lower();
            Items_.insert(item);
        }
    }

TEnumBuilder& TEnumBuilder::operator=(const TEnumBuilder& builder) {
    TBuilder::operator=(builder);
    Items_ = builder.Items_;
    return *this;
}

TEnumBuilder& TEnumBuilder::operator=(TEnumBuilder&& builder) {
    TBuilder::operator=(std::move(builder));
    Items_ = std::move(builder.Items_);
    return *this;
}

TEnumBuilder::TEnumBuilder(std::function<void(TEnumBuilder&)> configurator)
    : TBase(ENodeType::Enum) {
    configurator(*this);
}

TEnumBuilder& TEnumBuilder::SetItems(THashSet<TString> items) {
    Items_.clear();
    for (TString item : items) {
        item.to_lower();
        Items_.insert(item);
    }
    return *this;
}

TEnumValidator TEnumBuilder::CreateValidator() {
    auto result = TEnumValidator();
    result.Checkers_ = Checkers_;
    result.Required_ = Required_;
    result.Items_ = Items_;
    return result;
}

} // namespace NKikimr::NYamlConfig::NValidator
