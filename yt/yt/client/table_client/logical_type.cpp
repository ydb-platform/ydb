#include "logical_type.h"
#include "schema.h"
#include "yt/yt/client/table_client/row_base.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node.h>

#include <util/charset/utf8.h>

namespace NYT::NTableClient {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TWalkContext
{
    std::vector<TComplexTypeFieldDescriptor> Stack;
};

////////////////////////////////////////////////////////////////////////////////

static void WalkImpl(
    TWalkContext* walkContext,
    const TComplexTypeFieldDescriptor& descriptor,
    const std::function<void(const TWalkContext&, const TComplexTypeFieldDescriptor&)>& onElement)
{
    onElement(*walkContext, descriptor);
    walkContext->Stack.push_back(descriptor);
    auto g = Finally([&] {
        walkContext->Stack.pop_back();
    });
    const auto metatype = descriptor.GetType()->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
        case ELogicalMetatype::Decimal:
            return;
        case ELogicalMetatype::Optional:
            WalkImpl(walkContext, descriptor.OptionalElement(), onElement);
            return;
        case ELogicalMetatype::List:
            WalkImpl(walkContext, descriptor.ListElement(), onElement);
            return;
        case ELogicalMetatype::Struct:
            for (size_t i = 0; i < descriptor.GetType()->AsStructTypeRef().GetFields().size(); ++i) {
                WalkImpl(walkContext, descriptor.StructField(i), onElement);
            }
            return;
        case ELogicalMetatype::Tuple:
            for (size_t i = 0; i < descriptor.GetType()->AsTupleTypeRef().GetElements().size(); ++i) {
                WalkImpl(walkContext, descriptor.TupleElement(i), onElement);
            }
            return;
        case ELogicalMetatype::VariantStruct: {
            for (size_t i = 0; i < descriptor.GetType()->AsVariantStructTypeRef().GetFields().size(); ++i) {
                WalkImpl(walkContext, descriptor.VariantStructField(i), onElement);
            }
            return;
        }
        case ELogicalMetatype::VariantTuple:
            for (size_t i = 0; i < descriptor.GetType()->AsVariantTupleTypeRef().GetElements().size(); ++i) {
                WalkImpl(walkContext, descriptor.VariantTupleElement(i), onElement);
            }
            return;
        case ELogicalMetatype::Dict:
            WalkImpl(walkContext, descriptor.DictKey(), onElement);
            WalkImpl(walkContext, descriptor.DictValue(), onElement);
            return;
        case ELogicalMetatype::Tagged:
            WalkImpl(walkContext, descriptor.TaggedElement(), onElement);
            return;
    }
    YT_ABORT();
}

static void Walk(
    const TComplexTypeFieldDescriptor& descriptor,
    const std::function<void(const TWalkContext&, const TComplexTypeFieldDescriptor&)>& onElement)
{
    TWalkContext walkContext;
    WalkImpl(&walkContext, descriptor, onElement);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename F>
const T& VerifiedCast(const F& from)
{
    const T* to = dynamic_cast<const T*>(&from);
    YT_VERIFY(to != nullptr);
    return *to;
}

TLogicalType::TLogicalType(ELogicalMetatype type)
    : Metatype_(type)
{ }

const TSimpleLogicalType& TLogicalType::AsSimpleTypeRef() const
{
    return VerifiedCast<TSimpleLogicalType>(*this);
}

const TDecimalLogicalType& TLogicalType::AsDecimalTypeRef() const
{
    return VerifiedCast<TDecimalLogicalType>(*this);
}

const TOptionalLogicalType& TLogicalType::AsOptionalTypeRef() const
{
    return VerifiedCast<TOptionalLogicalType>(*this);
}

const TListLogicalType& TLogicalType::AsListTypeRef() const
{
    return VerifiedCast<TListLogicalType>(*this);
}

const TStructLogicalType& TLogicalType::AsStructTypeRef() const
{
    return VerifiedCast<TStructLogicalType>(*this);
}

const TTupleLogicalType& TLogicalType::AsTupleTypeRef() const
{
    return VerifiedCast<TTupleLogicalType>(*this);
}

const TVariantTupleLogicalType& TLogicalType::AsVariantTupleTypeRef() const
{
    return VerifiedCast<TVariantTupleLogicalType>(*this);
}

const TVariantStructLogicalType& TLogicalType::AsVariantStructTypeRef() const
{
    return VerifiedCast<TVariantStructLogicalType>(*this);
}

const TDictLogicalType& TLogicalType::AsDictTypeRef() const
{
    return VerifiedCast<TDictLogicalType>(*this);
}

const TTaggedLogicalType& TLogicalType::AsTaggedTypeRef() const
{
    return VerifiedCast<TTaggedLogicalType>(*this);
}

const TLogicalTypePtr& TLogicalType::GetElement() const
{
    switch (Metatype_) {
        case ELogicalMetatype::Optional:
            return AsOptionalTypeRef().GetElement();
        case ELogicalMetatype::List:
            return AsListTypeRef().GetElement();
        case ELogicalMetatype::Tagged:
            return AsTaggedTypeRef().GetElement();
        default:
            YT_ABORT();
    }
}

const std::vector<TLogicalTypePtr>& TLogicalType::GetElements() const
{
    switch (Metatype_) {
        case ELogicalMetatype::Tuple:
            return AsTupleTypeRef().GetElements();
        case ELogicalMetatype::VariantTuple:
            return AsVariantTupleTypeRef().GetElements();
        default:
            YT_ABORT();
    }
}

const std::vector<TStructField>& TLogicalType::GetFields() const
{
    switch (Metatype_) {
        case ELogicalMetatype::Struct:
            return AsStructTypeRef().GetFields();
        case ELogicalMetatype::VariantStruct:
            return AsVariantStructTypeRef().GetFields();
        default:
            YT_ABORT();
    }
}

static bool operator == (const TStructField& lhs, const TStructField& rhs)
{
    return (lhs.Name == rhs.Name) && (*lhs.Type == *rhs.Type);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLogicalType& logicalType)
{
    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CamelCaseToUnderscoreCase(ToString(logicalType.AsSimpleTypeRef().GetElement()));
        case ELogicalMetatype::Decimal:
            return Format("decimal(%v,%v)",
                logicalType.AsDecimalTypeRef().GetPrecision(),
                logicalType.AsDecimalTypeRef().GetScale());
        case ELogicalMetatype::Optional:
            return Format("optional<%v>", *logicalType.AsOptionalTypeRef().GetElement());
        case ELogicalMetatype::List:
            return Format("list<%v>", *logicalType.AsListTypeRef().GetElement());
        case ELogicalMetatype::Struct: {
            TStringStream out;
            out << "struct<";
            bool first = true;
            for (const auto& structItem : logicalType.AsStructTypeRef().GetFields()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << structItem.Name << '=' << ToString(*structItem.Type);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::Tuple: {
            TStringStream out;
            out << "tuple<";
            bool first = true;
            for (const auto& element : logicalType.AsTupleTypeRef().GetElements()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << ToString(*element);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::VariantTuple: {
            TStringStream out;
            out << "variant<";
            bool first = true;
            for (const auto& element : logicalType.AsVariantTupleTypeRef().GetElements()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << ToString(*element);
            }
            out << '>';
            return out.Str();
        }
        case ELogicalMetatype::VariantStruct: {
            TStringStream out;
            out << "named_variant<";
            bool first = true;
            for (const auto& field : logicalType.AsVariantStructTypeRef().GetFields()) {
                if (first) {
                    first = false;
                } else {
                    out << ';';
                }
                out << field.Name << '=' << ToString(*field.Type);
            }
            out << '>';
            return out.Str();

        }
        case ELogicalMetatype::Dict: {
            const auto& dictType = logicalType.AsDictTypeRef();
            TStringStream out;
            out << "dict<" << ToString(*dictType.GetKey()) << ';' << ToString(*dictType.GetValue()) << '>';
            return out.Str();
        }
        case ELogicalMetatype::Tagged: {
            const auto& taggedType = logicalType.AsTaggedTypeRef();
            TStringStream out;
            out << "tagged<\"" << ToString(taggedType.GetTag()) << "\";" << ToString(*taggedType.GetElement()) << '>';
            return out.Str();
        }
    }
    YT_ABORT();
}

void FormatValue(TStringBuilderBase* builder, const TLogicalType& logicalType, TStringBuf spec)
{
    // TODO(arkady-e1ppa): Optimize and express ToString using this.
    FormatValue(builder, ToString(logicalType), spec);
}

void PrintTo(ELogicalMetatype metatype, std::ostream* os)
{
    *os << ToString(metatype);
}

void PrintTo(const TLogicalType& type, std::ostream* os)
{
    *os << ToString(type);
}

void PrintTo(const TLogicalTypePtr& type, std::ostream* os)
{
    if (type) {
        PrintTo(*type, os);
    } else {
        (*os) << "<nullptr>";
    }
}

////////////////////////////////////////////////////////////////////////////////

TDecimalLogicalType::TDecimalLogicalType(int precision, int scale)
    : TLogicalType(ELogicalMetatype::Decimal)
    , Precision_(precision)
    , Scale_(scale)
{ }

size_t TDecimalLogicalType::GetMemoryUsage() const
{
    return sizeof(*this);
}

int TDecimalLogicalType::GetTypeComplexity() const
{
    return 1;
}

void TDecimalLogicalType::ValidateNode(const TWalkContext& /*context*/) const
{
    if (Precision_ < MinPrecision || Precision_ > MaxPrecision) {
        THROW_ERROR_EXCEPTION("Decimal precision %Qv is not in range [%v, %v]",
            Precision_,
            MinPrecision,
            MaxPrecision);
    }
    if (Scale_ < 0) {
        THROW_ERROR_EXCEPTION("Decimal scale %Qv is negative",
            Scale_);
    }

    if (Scale_ > Precision_) {
        THROW_ERROR_EXCEPTION("Decimal scale %Qv exceeds precision %Qv",
            Scale_,
            Precision_);
    }
}

bool TDecimalLogicalType::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TOptionalLogicalType::TOptionalLogicalType(TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::Optional)
    , Element_(std::move(element))
    , ElementIsNullable_(Element_->IsNullable())
{ }

std::optional<ESimpleLogicalValueType> TOptionalLogicalType::Simplify() const
{
    if (!IsElementNullable() && GetElement()->GetMetatype() == ELogicalMetatype::Simple) {
        return GetElement()->AsSimpleTypeRef().GetElement();
    } else {
        return std::nullopt;
    }
}

size_t TOptionalLogicalType::GetMemoryUsage() const
{
    if (Element_->GetMetatype() == ELogicalMetatype::Simple) {
        // All optionals of simple logical types are singletons and therefore we assume they use no space.
        return 0;
    } else {
        return sizeof(*this) + Element_->GetMemoryUsage();
    }
}

int TOptionalLogicalType::GetTypeComplexity() const
{
    if (Element_->GetMetatype() == ELogicalMetatype::Simple) {
        return 1;
    } else {
        return 1 + Element_->GetTypeComplexity();
    }
}

void TOptionalLogicalType::ValidateNode(const TWalkContext&) const
{ }

bool TOptionalLogicalType::IsNullable() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleLogicalType::TSimpleLogicalType(ESimpleLogicalValueType element)
    : TLogicalType(ELogicalMetatype::Simple)
    , Element_(element)
{ }

size_t TSimpleLogicalType::GetMemoryUsage() const
{
    // All simple logical types are singletons and therefore we assume they use no space.
    return 0;
}

int TSimpleLogicalType::GetTypeComplexity() const
{
    return 1;
}

void TSimpleLogicalType::ValidateNode(const TWalkContext& context) const
{
    if (Element_ == ESimpleLogicalValueType::Any) {
        if (context.Stack.empty() || context.Stack.back().GetType()->GetMetatype() != ELogicalMetatype::Optional) {
            THROW_ERROR_EXCEPTION("Type %Qv is disallowed outside of optional",
                ESimpleLogicalValueType::Any);
        }
    }
}

bool TSimpleLogicalType::IsNullable() const
{
    return GetPhysicalType(Element_) == EValueType::Null;
}

////////////////////////////////////////////////////////////////////////////////

TListLogicalType::TListLogicalType(TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::List)
    , Element_(std::move(element))
{ }

size_t TListLogicalType::GetMemoryUsage() const
{
    return sizeof(*this) + Element_->GetMemoryUsage();
}

int TListLogicalType::GetTypeComplexity() const
{
    return 1 + Element_->GetTypeComplexity();
}

void TListLogicalType::ValidateNode(const TWalkContext& /*context*/) const
{ }

bool TListLogicalType::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TComplexTypeFieldDescriptor::TComplexTypeFieldDescriptor(TLogicalTypePtr type)
    : Type_(std::move(type))
{ }

TComplexTypeFieldDescriptor::TComplexTypeFieldDescriptor(const NYT::NTableClient::TColumnSchema& column)
    : TComplexTypeFieldDescriptor(column.Name(), column.LogicalType())
{ }

TComplexTypeFieldDescriptor::TComplexTypeFieldDescriptor(TString columnName, TLogicalTypePtr type)
    : Descriptor_(std::move(columnName))
    , Type_(std::move(type))
{ }

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::OptionalElement() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<optional-element>", Type_->AsOptionalTypeRef().GetElement());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::ListElement() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<list-element>", Type_->AsListTypeRef().GetElement());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::Field(size_t i) const
{
    switch (Type_->GetMetatype()) {
        case ELogicalMetatype::Struct:
            return StructField(i);
        case ELogicalMetatype::VariantStruct:
            return VariantStructField(i);
        default:
            YT_ABORT();
    }
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::StructField(size_t i) const
{
    const auto& fields = Type_->AsStructTypeRef().GetFields();
    YT_VERIFY(i < fields.size());
    const auto& field = fields[i];
    return TComplexTypeFieldDescriptor(Descriptor_ + "." + field.Name, field.Type);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::Element(size_t i) const
{
    switch (Type_->GetMetatype()) {
        case ELogicalMetatype::Tuple:
            return TupleElement(i);
        case ELogicalMetatype::VariantTuple:
            return VariantTupleElement(i);
        default:
            YT_ABORT();
    }
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::TupleElement(size_t i) const
{
    const auto& elements = Type_->AsTupleTypeRef().GetElements();
    YT_VERIFY(i < elements.size());
    return TComplexTypeFieldDescriptor(Descriptor_ + Format(".<tuple-element-%v>", i), elements[i]);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::VariantTupleElement(size_t i) const
{
    const auto& elements = Type_->AsVariantTupleTypeRef().GetElements();
    YT_VERIFY(i < elements.size());
    return TComplexTypeFieldDescriptor(Descriptor_ + Format(".<variant-element-%v>", i), elements[i]);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::VariantStructField(size_t i) const
{
    const auto& fields = Type_->AsVariantStructTypeRef().GetFields();
    YT_VERIFY(i < fields.size());
    const auto& field = fields[i];
    return TComplexTypeFieldDescriptor(Descriptor_ + "." + field.Name, field.Type);
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::DictKey() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<key>", Type_->AsDictTypeRef().GetKey());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::DictValue() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<value>", Type_->AsDictTypeRef().GetValue());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::TaggedElement() const
{
    return TComplexTypeFieldDescriptor(Descriptor_ + ".<tagged-element>", Type_->AsTaggedTypeRef().GetElement());
}

TComplexTypeFieldDescriptor TComplexTypeFieldDescriptor::Detag() const
{
    return TComplexTypeFieldDescriptor(Descriptor_, DetagLogicalType(Type_));
}

const TString& TComplexTypeFieldDescriptor::GetDescription() const
{
    return Descriptor_;
}

const TLogicalTypePtr& TComplexTypeFieldDescriptor::GetType() const
{
    return Type_;
}

static std::pair<std::vector<TStructField>, bool> DetagFields(const std::vector<TStructField>& fields)
{
    bool changed = false;
    std::vector<TStructField> result;
    for (const auto& field : fields) {
        result.emplace_back();
        result.back().Name = field.Name;
        result.back().Type = DetagLogicalType(field.Type);
        if (result.back().Type.Get() != field.Type.Get()) {
            changed = true;
        }
    }
    return std::pair(result, changed);
}

static std::pair<std::vector<TLogicalTypePtr>, bool> DetagElements(const std::vector<TLogicalTypePtr>& elements)
{
    std::vector<TLogicalTypePtr> result;
    bool changed = false;
    for (const auto& element : elements) {
        result.emplace_back(DetagLogicalType(element));
        if (result.back().Get() != element.Get()) {
            changed = true;
        }
    }
    return std::pair(result, changed);
}

TLogicalTypePtr DetagLogicalType(const TLogicalTypePtr& type)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple:
        case ELogicalMetatype::Decimal:
            return type;
        case ELogicalMetatype::Optional: {
            const auto& element = type->AsOptionalTypeRef().GetElement();
            auto detaggedElement = DetagLogicalType(element);
            if (element.Get() != detaggedElement.Get()) {
                return OptionalLogicalType(detaggedElement);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::List: {
            const auto& element = type->AsListTypeRef().GetElement();
            auto detaggedElement = DetagLogicalType(element);
            if (element.Get() != detaggedElement.Get()) {
                return ListLogicalType(detaggedElement);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::Struct: {
            const auto [fields, changed] = DetagFields(type->AsStructTypeRef().GetFields());
            if (changed) {
                return StructLogicalType(fields);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::Tuple: {
            const auto [elements, changed] = DetagElements(type->AsTupleTypeRef().GetElements());
            if (changed) {
                return TupleLogicalType(elements);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::VariantStruct: {
            const auto [fields, changed] = DetagFields(type->AsVariantStructTypeRef().GetFields());
            if (changed) {
                return VariantStructLogicalType(fields);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::VariantTuple: {
            const auto [elements, changed] = DetagElements(type->AsVariantTupleTypeRef().GetElements());
            if (changed) {
                return VariantTupleLogicalType(elements);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::Dict: {
            const auto& dictType = type->AsDictTypeRef();
            const auto& key = dictType.GetKey();
            const auto& value = dictType.GetValue();
            const auto detaggedKey = DetagLogicalType(key);
            const auto detaggedValue = DetagLogicalType(value);
            if (detaggedKey.Get() != key.Get() || detaggedValue.Get() != value.Get()) {
                return DictLogicalType(
                    detaggedKey,
                    detaggedValue);
            } else {
                return type;
            }
        }
        case ELogicalMetatype::Tagged:
            return DetagLogicalType(type->AsTaggedTypeRef().GetElement());
    }
    YT_ABORT();
}


////////////////////////////////////////////////////////////////////////////////

TStructLogicalTypeBase::TStructLogicalTypeBase(ELogicalMetatype metatype, std::vector<TStructField> fields)
    : TLogicalType(metatype)
    , Fields_(std::move(fields))
{ }

size_t TStructLogicalTypeBase::GetMemoryUsage() const
{
    size_t result = sizeof(*this);
    result += sizeof(TStructField) * Fields_.size();
    for (const auto& field : Fields_) {
        result += field.Type->GetMemoryUsage();
    }
    return result;
}

int TStructLogicalTypeBase::GetTypeComplexity() const
{
    int result = 1;
    for (const auto& field : Fields_) {
        result += field.Type->GetTypeComplexity();
    }
    return result;
}

void TStructLogicalTypeBase::ValidateNode(const TWalkContext& /*context*/) const
{
    THashSet<TStringBuf> usedNames;
    for (size_t i = 0; i < Fields_.size(); ++i) {
        const auto& field = Fields_[i];
        if (field.Name.empty()) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v is empty",
                i);
        }
        if (usedNames.contains(field.Name)) {
            THROW_ERROR_EXCEPTION("Struct field name %Qv is used twice",
                field.Name);
        }
        usedNames.emplace(field.Name);
        if (field.Name.size() > MaxColumnNameLength) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v exceeds limit: %v > %v",
                i,
                field.Name.size(),
                MaxColumnNameLength);
        }
        if (!IsUtf(field.Name)) {
            THROW_ERROR_EXCEPTION("Name of struct field #%v is not valid utf8",
                i);
        }
    }
}

bool TStructLogicalTypeBase::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TTupleLogicalTypeBase::TTupleLogicalTypeBase(ELogicalMetatype metatype, std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TLogicalType(metatype)
    , Elements_(std::move(elements))
{ }

size_t TTupleLogicalTypeBase::GetMemoryUsage() const
{
    size_t result = sizeof(*this);
    result += sizeof(TLogicalTypePtr) * Elements_.size();
    for (const auto& element : Elements_) {
        result += element->GetMemoryUsage();
    }
    return result;
}

int TTupleLogicalTypeBase::GetTypeComplexity() const
{
    int result = 1;
    for (const auto& element : Elements_) {
        result += element->GetTypeComplexity();
    }
    return result;
}

void TTupleLogicalTypeBase::ValidateNode(const TWalkContext& /*context*/) const
{ }

bool TTupleLogicalTypeBase::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TStructLogicalType::TStructLogicalType(std::vector<NYT::NTableClient::TStructField> fields)
    : TStructLogicalTypeBase(ELogicalMetatype::Struct, std::move(fields))
{ }

////////////////////////////////////////////////////////////////////////////////

TTupleLogicalType::TTupleLogicalType(std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TTupleLogicalTypeBase(ELogicalMetatype::Tuple, std::move(elements))
{ }

////////////////////////////////////////////////////////////////////////////////

TVariantStructLogicalType::TVariantStructLogicalType(std::vector<NYT::NTableClient::TStructField> fields)
    : TStructLogicalTypeBase(ELogicalMetatype::VariantStruct, std::move(fields))
{ }

////////////////////////////////////////////////////////////////////////////////

TVariantTupleLogicalType::TVariantTupleLogicalType(std::vector<NYT::NTableClient::TLogicalTypePtr> elements)
    : TTupleLogicalTypeBase(ELogicalMetatype::VariantTuple, std::move(elements))
{ }

////////////////////////////////////////////////////////////////////////////////

TDictLogicalType::TDictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value)
    : TLogicalType(ELogicalMetatype::Dict)
    , Key_(std::move(key))
    , Value_(std::move(value))
{ }

size_t TDictLogicalType::GetMemoryUsage() const
{
    return sizeof(*this) + Key_->GetMemoryUsage() + Value_->GetMemoryUsage();
}

int TDictLogicalType::GetTypeComplexity() const
{
    return 1 + Key_->GetTypeComplexity() + Value_->GetTypeComplexity();
}

void TDictLogicalType::ValidateNode(const TWalkContext&) const
{
    TComplexTypeFieldDescriptor descriptor("<dict-key>", GetKey());
    Walk(descriptor, [] (const TWalkContext&, const TComplexTypeFieldDescriptor& descriptor) {
        const auto& logicalType = descriptor.GetType();

        // NB. We intentionally list all metatypes and simple types here.
        // We want careful decision if type can be used as dictionary key each time new type is added.
        // Compiler will warn you (with error) if some enum is not handled.
        switch (logicalType->GetMetatype()) {
            case ELogicalMetatype::Simple:
                switch (auto simpleType = logicalType->AsSimpleTypeRef().GetElement()) {
                    case ESimpleLogicalValueType::Any:
                    case ESimpleLogicalValueType::Json:
                        THROW_ERROR_EXCEPTION("%Qv is of type %Qv that is not allowed in dict key",
                            descriptor.GetDescription(),
                            simpleType);
                    case ESimpleLogicalValueType::Null:
                    case ESimpleLogicalValueType::Void:
                    case ESimpleLogicalValueType::Boolean:
                    case ESimpleLogicalValueType::Int8:
                    case ESimpleLogicalValueType::Int16:
                    case ESimpleLogicalValueType::Int32:
                    case ESimpleLogicalValueType::Int64:
                    case ESimpleLogicalValueType::Uint8:
                    case ESimpleLogicalValueType::Uint16:
                    case ESimpleLogicalValueType::Uint32:
                    case ESimpleLogicalValueType::Uint64:
                    case ESimpleLogicalValueType::Float:
                    case ESimpleLogicalValueType::Double:
                    case ESimpleLogicalValueType::String:
                    case ESimpleLogicalValueType::Utf8:
                    case ESimpleLogicalValueType::Date:
                    case ESimpleLogicalValueType::Datetime:
                    case ESimpleLogicalValueType::Timestamp:
                    case ESimpleLogicalValueType::Interval:
                    case ESimpleLogicalValueType::Uuid:
                    case ESimpleLogicalValueType::Date32:
                    case ESimpleLogicalValueType::Datetime64:
                    case ESimpleLogicalValueType::Timestamp64:
                    case ESimpleLogicalValueType::Interval64:
                        return;
                }
                YT_ABORT();
            case ELogicalMetatype::Optional:
            case ELogicalMetatype::Struct:
            case ELogicalMetatype::VariantStruct:
            case ELogicalMetatype::Tuple:
            case ELogicalMetatype::VariantTuple:
            case ELogicalMetatype::List:
            case ELogicalMetatype::Dict:
            case ELogicalMetatype::Tagged:
            case ELogicalMetatype::Decimal:
                return;
        }
        YT_ABORT();
    });
}

bool TDictLogicalType::IsNullable() const
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

TTaggedLogicalType::TTaggedLogicalType(TString tag, NYT::NTableClient::TLogicalTypePtr element)
    : TLogicalType(ELogicalMetatype::Tagged)
    , Tag_(std::move(tag))
    , Element_(std::move(element))
{ }

size_t TTaggedLogicalType::GetMemoryUsage() const
{
    return sizeof(*this) + GetElement()->GetMemoryUsage();
}

int TTaggedLogicalType::GetTypeComplexity() const
{
    return 1 + GetElement()->GetTypeComplexity();
}

void TTaggedLogicalType::ValidateNode(const TWalkContext&) const
{
    if (Tag_.empty()) {
        THROW_ERROR_EXCEPTION("Tag is empty");
    }

    if (Tag_.size() > MaxColumnNameLength) {
        THROW_ERROR_EXCEPTION("Tag is too big");
    }

    if (!IsUtf(Tag_)) {
        THROW_ERROR_EXCEPTION("Tag is not valid utf8");
    }
}

bool TTaggedLogicalType::IsNullable() const
{
    return GetElement()->IsNullable();
}

////////////////////////////////////////////////////////////////////////////////

struct TTypeV3Info
{
    ESimpleLogicalValueType V1Type;
    EValueType WireType;
    bool Required;
    bool IsPureV1Type;
};

static TTypeV3Info GetTypeV3Info(const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple: {
            auto element = logicalType->AsSimpleTypeRef().GetElement();
            return {element, GetPhysicalType(element), !logicalType->IsNullable(), true};
        }
        case ELogicalMetatype::Decimal:
            return {ESimpleLogicalValueType::String, EValueType::String, true, false};
        case ELogicalMetatype::Optional: {
            const auto& element = logicalType->AsOptionalTypeRef().GetElement();
            if (element->IsNullable()) {
                return {ESimpleLogicalValueType::Any, EValueType::Composite, false, false};
            } else {
                auto elementInfo = GetTypeV3Info(element);
                elementInfo.Required = false;
                return elementInfo;
            }
        }
        case ELogicalMetatype::Tagged:
            return GetTypeV3Info(logicalType->AsTaggedTypeRef().GetElement());
        case ELogicalMetatype::List:
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::VariantTuple:
        case ELogicalMetatype::Dict:
            return {ESimpleLogicalValueType::Any, EValueType::Composite, true, false};
    }
    YT_ABORT();
}

std::pair<ESimpleLogicalValueType, bool> CastToV1Type(const TLogicalTypePtr& logicalType)
{
    auto info = GetTypeV3Info(logicalType);
    return {info.V1Type, info.Required};
}

bool IsV1Type(const TLogicalTypePtr& logicalType)
{
    return GetTypeV3Info(logicalType).IsPureV1Type;
}

EValueType GetWireType(const TLogicalTypePtr& logicalType)
{
    return GetTypeV3Info(logicalType).WireType;
}

bool IsV3Composite(const TLogicalTypePtr& logicalType)
{
    return GetWireType(logicalType) == EValueType::Composite;
}

TLogicalTypePtr DenullifyLogicalType(const TLogicalTypePtr& type)
{
    auto detagged = DetagLogicalType(type);
    if (detagged->GetMetatype() == ELogicalMetatype::Optional) {
        const auto& optional = detagged->AsOptionalTypeRef();
        if (!optional.IsElementNullable()) {
            return optional.GetElement();
        }
    }
    return detagged;
}

bool operator == (const std::vector<TLogicalTypePtr>& lhs, const std::vector<TLogicalTypePtr>& rhs)
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    for (size_t i = 0; i < lhs.size(); ++i) {
        if (*lhs[i] != *rhs[i]) {
            return false;
        }
    }
    return true;
}

bool operator == (const TLogicalType& lhs, const TLogicalType& rhs)
{
    if (&lhs == &rhs) {
        return true;
    }

    if (lhs.GetMetatype() != rhs.GetMetatype()) {
        return false;
    }

    switch (lhs.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return lhs.AsSimpleTypeRef().GetElement() == rhs.AsSimpleTypeRef().GetElement();
        case ELogicalMetatype::Optional:
            return *lhs.AsOptionalTypeRef().GetElement() == *rhs.AsOptionalTypeRef().GetElement();
        case ELogicalMetatype::List:
            return *lhs.AsListTypeRef().GetElement() == *rhs.AsListTypeRef().GetElement();
        case ELogicalMetatype::Struct:
            return lhs.AsStructTypeRef().GetFields() == rhs.AsStructTypeRef().GetFields();
        case ELogicalMetatype::Tuple:
            return lhs.AsTupleTypeRef().GetElements() == rhs.AsTupleTypeRef().GetElements();
        case ELogicalMetatype::VariantStruct:
            return lhs.AsVariantStructTypeRef().GetFields() == rhs.AsVariantStructTypeRef().GetFields();
        case ELogicalMetatype::VariantTuple:
            return lhs.AsVariantTupleTypeRef().GetElements() == rhs.AsVariantTupleTypeRef().GetElements();
        case ELogicalMetatype::Dict:
            return *lhs.AsDictTypeRef().GetKey() == *rhs.AsDictTypeRef().GetKey() &&
                *lhs.AsDictTypeRef().GetValue() == *rhs.AsDictTypeRef().GetValue();
        case ELogicalMetatype::Tagged:
            return lhs.AsTaggedTypeRef().GetTag() == rhs.AsTaggedTypeRef().GetTag() &&
                *lhs.AsTaggedTypeRef().GetElement() == *rhs.AsTaggedTypeRef().GetElement();
        case ELogicalMetatype::Decimal:
            return lhs.AsDecimalTypeRef().GetPrecision() == rhs.AsDecimalTypeRef().GetPrecision() &&
                lhs.AsDecimalTypeRef().GetScale() == rhs.AsDecimalTypeRef().GetScale();
    }
    YT_ABORT();
}

void ValidateLogicalType(const TComplexTypeFieldDescriptor& rootDescriptor, std::optional<int> depthLimit)
{
    Walk(rootDescriptor, [&] (const TWalkContext& context, const TComplexTypeFieldDescriptor& descriptor) {
        try {
            descriptor.GetType()->ValidateNode(context);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("%Qv has bad type",
                descriptor.GetDescription())
                << ex;
        }
        if (depthLimit && std::ssize(context.Stack) > *depthLimit) {
            THROW_ERROR_EXCEPTION("%Qv exceeds type depth limit",
                descriptor.GetDescription())
                << TErrorAttribute("limit", *depthLimit);
        }
    });
}

void ToProto(NProto::TLogicalType* protoLogicalType, const TLogicalTypePtr& logicalType)
{
    switch (logicalType->GetMetatype()) {
        case ELogicalMetatype::Simple:
            protoLogicalType->set_simple(static_cast<int>(logicalType->AsSimpleTypeRef().GetElement()));
            return;
        case ELogicalMetatype::Decimal:
            protoLogicalType->mutable_decimal()->set_precision(logicalType->AsDecimalTypeRef().GetPrecision());
            protoLogicalType->mutable_decimal()->set_scale(logicalType->AsDecimalTypeRef().GetScale());
            return;
        case ELogicalMetatype::Optional:
            ToProto(protoLogicalType->mutable_optional(), logicalType->AsOptionalTypeRef().GetElement());
            return;
        case ELogicalMetatype::List:
            ToProto(protoLogicalType->mutable_list(), logicalType->AsListTypeRef().GetElement());
            return;
        case ELogicalMetatype::Struct: {
            auto protoStruct = protoLogicalType->mutable_struct_();
            for (const auto& structField : logicalType->AsStructTypeRef().GetFields()) {
                auto protoStructField = protoStruct->add_fields();
                protoStructField->set_name(structField.Name);
                ToProto(protoStructField->mutable_type(), structField.Type);
            }
            return;
        }
        case ELogicalMetatype::Tuple: {
            auto protoTuple = protoLogicalType->mutable_tuple();
            for (const auto& element : logicalType->AsTupleTypeRef().GetElements()) {
                auto protoElement = protoTuple->add_elements();
                ToProto(protoElement, element);
            }
            return;
        }
        case ELogicalMetatype::VariantStruct: {
            auto protoVariantStruct = protoLogicalType->mutable_variant_struct();
            for (const auto& field : logicalType->AsVariantStructTypeRef().GetFields()) {
                auto protoField = protoVariantStruct->add_fields();
                protoField->set_name(field.Name);
                ToProto(protoField->mutable_type(), field.Type);
            }
            return;
        }
        case ELogicalMetatype::VariantTuple: {
            auto protoVariantTuple = protoLogicalType->mutable_variant_tuple();
            for (const auto& element : logicalType->AsVariantTupleTypeRef().GetElements()) {
                auto protoElement = protoVariantTuple->add_elements();
                ToProto(protoElement, element);
            }
            return;
        }
        case ELogicalMetatype::Dict: {
            auto protoDict = protoLogicalType->mutable_dict();
            const auto& dictLogicalType = logicalType->AsDictTypeRef();
            ToProto(protoDict->mutable_key(), dictLogicalType.GetKey());
            ToProto(protoDict->mutable_value(), dictLogicalType.GetValue());
            return;
        }
        case ELogicalMetatype::Tagged: {
            auto protoTagged = protoLogicalType->mutable_tagged();
            const auto& taggedLogicalType = logicalType->AsTaggedTypeRef();
            protoTagged->set_tag(taggedLogicalType.GetTag());
            ToProto(protoTagged->mutable_element(), taggedLogicalType.GetElement());
            return;
        }
    }
    YT_ABORT();
}

void FromProto(TLogicalTypePtr* logicalType, const NProto::TLogicalType& protoLogicalType)
{
    switch (protoLogicalType.type_case()) {
        case NProto::TLogicalType::TypeCase::kSimple:
            *logicalType = SimpleLogicalType(CheckedEnumCast<ESimpleLogicalValueType>(protoLogicalType.simple()));
            return;
        case NProto::TLogicalType::TypeCase::kDecimal:
            *logicalType = DecimalLogicalType(protoLogicalType.decimal().precision(), protoLogicalType.decimal().scale());
            return;
        case NProto::TLogicalType::TypeCase::kOptional: {
            TLogicalTypePtr element;
            FromProto(&element, protoLogicalType.optional());
            *logicalType = OptionalLogicalType(element);
            return;
        }
        case NProto::TLogicalType::TypeCase::kList: {
            TLogicalTypePtr element;
            FromProto(&element, protoLogicalType.list());
            *logicalType = ListLogicalType(element);
            return;
        }
        case NProto::TLogicalType::TypeCase::kStruct: {
            std::vector<TStructField> fields;
            for (const auto& protoField : protoLogicalType.struct_().fields()) {
                TLogicalTypePtr fieldType;
                FromProto(&fieldType, protoField.type());
                fields.emplace_back(TStructField{protoField.name(), std::move(fieldType)});
            }
            *logicalType = StructLogicalType(std::move(fields));
            return;
        }
        case NProto::TLogicalType::TypeCase::kTuple: {
            std::vector<TLogicalTypePtr> elements;
            for (const auto& protoField : protoLogicalType.tuple().elements()) {
                elements.emplace_back();
                FromProto(&elements.back(), protoField);
            }
            *logicalType = TupleLogicalType(std::move(elements));
            return;
        }
        case NProto::TLogicalType::TypeCase::kVariantTuple: {
            std::vector<TLogicalTypePtr> elements;
            for (const auto& protoElement : protoLogicalType.variant_tuple().elements()) {
                elements.emplace_back();
                FromProto(&elements.back(), protoElement);
            }
            *logicalType = VariantTupleLogicalType(std::move(elements));
            return;
        }
        case NProto::TLogicalType::TypeCase::kVariantStruct: {
            std::vector<TStructField> fields;
            for (const auto& protoField : protoLogicalType.variant_struct().fields()) {
                TLogicalTypePtr fieldType;
                FromProto(&fieldType, protoField.type());
                fields.emplace_back(TStructField{protoField.name(), std::move(fieldType)});
            }
            *logicalType = VariantStructLogicalType(std::move(fields));
            return;
        }
        case NProto::TLogicalType::TypeCase::kDict: {
            TLogicalTypePtr keyType;
            TLogicalTypePtr valueType;
            FromProto(&keyType, protoLogicalType.dict().key());
            FromProto(&valueType, protoLogicalType.dict().value());
            *logicalType = DictLogicalType(keyType, valueType);
            return;
        }
        case NProto::TLogicalType::TypeCase::kTagged: {
            TLogicalTypePtr element;
            FromProto(&element, protoLogicalType.tagged().element());
            *logicalType = TaggedLogicalType(protoLogicalType.tagged().tag(), std::move(element));
            return;
        }
        case NProto::TLogicalType::TypeCase::TYPE_NOT_SET:
            THROW_ERROR_EXCEPTION("Cannot parse unknown logical type from proto");
    }
    YT_ABORT();
}

void Serialize(const TStructField& structElement, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer).BeginMap()
        .Item("name").Value(structElement.Name)
        .Item("type").Value(structElement.Type)
    .EndMap();
}

void Deserialize(TStructField& structElement, NYTree::INodePtr node)
{
    const auto& mapNode = node->AsMap();
    structElement.Name = NYTree::ConvertTo<TString>(mapNode->GetChildOrThrow("name"));
    structElement.Type = NYTree::ConvertTo<TLogicalTypePtr>(mapNode->GetChildOrThrow("type"));
}

// TODO(levysotsky): Get rid of this variable when we are sure type_v2 is dead for good.
static constexpr bool UseTypeV3ForSerialization = true;

void Serialize(const TLogicalTypePtr& logicalType, NYson::IYsonConsumer* consumer)
{
    if (UseTypeV3ForSerialization) {
        Serialize(TTypeV3LogicalTypeWrapper{logicalType}, consumer);
        return;
    }

    const auto metatype = logicalType->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
            NYTree::BuildYsonFluently(consumer)
                .Value(logicalType->AsSimpleTypeRef().GetElement());
            return;
        case ELogicalMetatype::Decimal:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("precision").Value(logicalType->AsDecimalTypeRef().GetPrecision())
                    .Item("scale").Value(logicalType->AsDecimalTypeRef().GetScale())
                .EndMap();
            return;
        case ELogicalMetatype::Optional:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("element").Value(logicalType->AsOptionalTypeRef().GetElement())
                .EndMap();
            return;
        case ELogicalMetatype::List:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("element").Value(logicalType->AsListTypeRef().GetElement())
                .EndMap();
            return;
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::VariantStruct: {
            const auto& fields =
                metatype == ELogicalMetatype::Struct ?
                logicalType->AsStructTypeRef().GetFields() :
                logicalType->AsVariantStructTypeRef().GetFields();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("fields").Value(fields)
                .EndMap();
            return;
        }
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple: {
            const auto& elements =
                metatype == ELogicalMetatype::Tuple ?
                logicalType->AsTupleTypeRef().GetElements() :
                logicalType->AsVariantTupleTypeRef().GetElements();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("elements").Value(elements)
                .EndMap();
            return;
        }
        case ELogicalMetatype::Dict: {
            const auto& key = logicalType->AsDictTypeRef().GetKey();
            const auto& value = logicalType->AsDictTypeRef().GetValue();
            NYTree::BuildYsonFluently(consumer)
            .BeginMap()
                .Item("metatype").Value(metatype)
                .Item("key").Value(key)
                .Item("value").Value(value)
            .EndMap();
            return;
        }
        case ELogicalMetatype::Tagged: {
            const auto& element = logicalType->AsTaggedTypeRef().GetElement();
            const auto& tag = logicalType->AsTaggedTypeRef().GetTag();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("metatype").Value(metatype)
                    .Item("tag").Value(tag)
                    .Item("element").Value(element)
                .EndMap();
            return;
        }
    }
    YT_ABORT();
}

void Deserialize(TLogicalTypePtr& logicalType, NYTree::INodePtr node)
{
    if (UseTypeV3ForSerialization) {
        TTypeV3LogicalTypeWrapper wrapper;
        Deserialize(wrapper, node);
        logicalType = wrapper.LogicalType;
        return;
    }

    if (node->GetType() == NYTree::ENodeType::String) {
        auto simpleLogicalType = NYTree::ConvertTo<ESimpleLogicalValueType>(node);
        logicalType = SimpleLogicalType(simpleLogicalType);
        return;
    }
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing logical type: expected %Qlv or %Qlv, actual %Qlv",
            NYTree::ENodeType::Map,
            NYTree::ENodeType::String,
            node->GetType());
    }
    auto mapNode = node->AsMap();

    ELogicalMetatype metatype;
    {
        auto metatypeNode = mapNode->GetChildOrThrow("metatype");
        metatype = NYTree::ConvertTo<ELogicalMetatype>(metatypeNode);
    }
    switch (metatype) {
        case ELogicalMetatype::Simple: {
            THROW_ERROR_EXCEPTION("Error parsing logical type: cannot parse simple type from %Qv",
                NYTree::ENodeType::Map);
        }
        case ELogicalMetatype::Decimal: {
            auto precision = mapNode->GetChildValueOrThrow<i64>("precision");
            auto scale = mapNode->GetChildValueOrThrow<i64>("scale");
            logicalType = DecimalLogicalType(precision, scale);
            return;
        }
        case ELogicalMetatype::Optional: {
            auto elementNode = mapNode->GetChildOrThrow("element");
            auto element = NYTree::ConvertTo<TLogicalTypePtr>(elementNode);
            logicalType = OptionalLogicalType(std::move(element));
            return;
        }
        case ELogicalMetatype::List: {
            auto elementNode = mapNode->GetChildOrThrow("element");
            auto element = NYTree::ConvertTo<TLogicalTypePtr>(elementNode);
            logicalType = ListLogicalType(std::move(element));
            return;
        }
        case ELogicalMetatype::Struct: {
            auto fieldsNode = mapNode->GetChildOrThrow("fields");
            auto fields = NYTree::ConvertTo<std::vector<TStructField>>(fieldsNode);
            logicalType = StructLogicalType(std::move(fields));
            return;
        }
        case ELogicalMetatype::Tuple: {
            auto elementsNode = mapNode->GetChildOrThrow("elements");
            auto elements = NYTree::ConvertTo<std::vector<TLogicalTypePtr>>(elementsNode);
            logicalType = TupleLogicalType(std::move(elements));
            return;
        }
        case ELogicalMetatype::VariantStruct: {
            auto fieldsNode = mapNode->GetChildOrThrow("fields");
            auto fields = NYTree::ConvertTo<std::vector<TStructField>>(fieldsNode);
            logicalType = VariantStructLogicalType(std::move(fields));
            return;
        }
        case ELogicalMetatype::VariantTuple: {
            auto elementsNode = mapNode->GetChildOrThrow("elements");
            auto elements = NYTree::ConvertTo<std::vector<TLogicalTypePtr>>(elementsNode);
            logicalType = VariantTupleLogicalType(std::move(elements));
            return;
        }
        case ELogicalMetatype::Dict: {
            auto keyNode = mapNode->GetChildOrThrow("key");
            auto key = NYTree::ConvertTo<TLogicalTypePtr>(keyNode);
            auto valueNode = mapNode->GetChildOrThrow("value");
            auto value = NYTree::ConvertTo<TLogicalTypePtr>(valueNode);
            logicalType = DictLogicalType(std::move(key), std::move(value));
            return;
        }
        case ELogicalMetatype::Tagged: {
            auto tagNode = mapNode->GetChildOrThrow("tag");
            auto tag = NYTree::ConvertTo<TString>(tagNode);
            auto elementNode = mapNode->GetChildOrThrow("element");
            auto element = NYTree::ConvertTo<TLogicalTypePtr>(elementNode);
            logicalType = TaggedLogicalType(std::move(tag), std::move(element));
            return;
        }
    }
    YT_ABORT();
}

bool IsComparable(const TLogicalTypePtr& type)
{
    switch (type->GetMetatype()) {
        case ELogicalMetatype::Simple:
            switch (type->AsSimpleTypeRef().GetElement()) {
                case ESimpleLogicalValueType::Int64:
                case ESimpleLogicalValueType::Int32:
                case ESimpleLogicalValueType::Int16:
                case ESimpleLogicalValueType::Int8:

                case ESimpleLogicalValueType::Uint64:
                case ESimpleLogicalValueType::Uint32:
                case ESimpleLogicalValueType::Uint16:
                case ESimpleLogicalValueType::Uint8:

                case ESimpleLogicalValueType::Boolean:

                case ESimpleLogicalValueType::Double:
                case ESimpleLogicalValueType::Float:

                case ESimpleLogicalValueType::String:
                case ESimpleLogicalValueType::Utf8:

                case ESimpleLogicalValueType::Date:
                case ESimpleLogicalValueType::Datetime:
                case ESimpleLogicalValueType::Timestamp:
                case ESimpleLogicalValueType::Interval:

                case ESimpleLogicalValueType::Null:
                case ESimpleLogicalValueType::Void:

                case ESimpleLogicalValueType::Uuid:

                case ESimpleLogicalValueType::Date32:
                case ESimpleLogicalValueType::Datetime64:
                case ESimpleLogicalValueType::Timestamp64:
                case ESimpleLogicalValueType::Interval64:
                    return true;

                case ESimpleLogicalValueType::Any:
                case ESimpleLogicalValueType::Json:
                    return false;
            }
            Y_ABORT();
        case ELogicalMetatype::Decimal:
            return true;
        case ELogicalMetatype::Optional:
        case ELogicalMetatype::List:
        case ELogicalMetatype::Tagged:
            return IsComparable(type->GetElement());

        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple: {
            for (const auto& element : type->GetElements()) {
                if (!IsComparable(element)) {
                    return false;
                }
            }
            return true;
        }

        case ELogicalMetatype::Struct:
        case ELogicalMetatype::VariantStruct:
        case ELogicalMetatype::Dict:
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TV3Variant
{ };

using TV3TypeName = std::variant<ESimpleLogicalValueType, ELogicalMetatype, TV3Variant>;

static const std::pair<ESimpleLogicalValueType, TString> V3SimpleLogicalValueTypeEncoding[] =
{
    {ESimpleLogicalValueType::Null,         "null"},
    {ESimpleLogicalValueType::Int64,        "int64"},
    {ESimpleLogicalValueType::Uint64,       "uint64"},
    {ESimpleLogicalValueType::Double,       "double"},
    {ESimpleLogicalValueType::Float,        "float"},
    {ESimpleLogicalValueType::Boolean,      "bool"},  // NB. diff
    {ESimpleLogicalValueType::String,       "string"},
    {ESimpleLogicalValueType::Any,          "yson"}, // NB. diff
    {ESimpleLogicalValueType::Json,         "json"},
    {ESimpleLogicalValueType::Int8,         "int8"},
    {ESimpleLogicalValueType::Uint8,       "uint8"},

    {ESimpleLogicalValueType::Int16,       "int16"},
    {ESimpleLogicalValueType::Uint16,      "uint16"},

    {ESimpleLogicalValueType::Int32,       "int32"},
    {ESimpleLogicalValueType::Uint32,      "uint32"},

    {ESimpleLogicalValueType::Utf8,        "utf8"},

    {ESimpleLogicalValueType::Date,        "date"},
    {ESimpleLogicalValueType::Datetime,    "datetime"},
    {ESimpleLogicalValueType::Timestamp,   "timestamp"},
    {ESimpleLogicalValueType::Interval,    "interval"},

    {ESimpleLogicalValueType::Void,        "void"},

    {ESimpleLogicalValueType::Uuid,        "uuid"},

    {ESimpleLogicalValueType::Date32,      "date32"},
    {ESimpleLogicalValueType::Datetime64,  "datetime64"},
    {ESimpleLogicalValueType::Timestamp64, "timestamp64"},
    {ESimpleLogicalValueType::Interval64,  "interval64"},
};
static_assert(std::size(V3SimpleLogicalValueTypeEncoding) == TEnumTraits<ESimpleLogicalValueType>::GetDomainSize());

std::pair<ELogicalMetatype, TString> V3LogicalMetatypeEncoding[] =
{
    // NB. following metatypes are not included:
    //   - ELogicalMetatype::Simple
    //   - ELogicalMetatype::VariantStruct
    //   - ELogicalMetatype::VariantTuple
    {ELogicalMetatype::Optional, "optional"},
    {ELogicalMetatype::List, "list"},
    {ELogicalMetatype::Struct, "struct"},
    {ELogicalMetatype::Tuple, "tuple"},
    {ELogicalMetatype::Dict, "dict"},
    {ELogicalMetatype::Tagged, "tagged"},
    {ELogicalMetatype::Decimal, "decimal"},
};

// NB ELogicalMetatype::{Simple,VariantStruct,VariantTuple} are not encoded therefore we have `-3` in static_assert below.
static_assert(std::size(V3LogicalMetatypeEncoding) == TEnumTraits<ELogicalMetatype>::GetDomainSize() - 3);

TV3TypeName FromTypeV3(TStringBuf stringBuf)
{
    static const auto map = [] {
        THashMap<TStringBuf, TV3TypeName> res;
        for (const auto& [value, string] : V3SimpleLogicalValueTypeEncoding) {
            res[string] = value;
        }
        for (const auto& [value, string] : V3LogicalMetatypeEncoding) {
            res[string] = value;
        }
        res["variant"] = TV3Variant{};
        return res;
    }();

    auto it = map.find(stringBuf);
    if (it == map.end()) {
        THROW_ERROR_EXCEPTION("%Qv is not valid type_v3 simple type",
            stringBuf);
    }
    return it->second;
}

TStringBuf ToTypeV3(ESimpleLogicalValueType value)
{
    for (const auto& [type, string] : V3SimpleLogicalValueTypeEncoding) {
        if (type == value) {
            return string;
        }
    }
    YT_ABORT();
}

TStringBuf ToTypeV3(ELogicalMetatype value)
{
    YT_VERIFY(value != ELogicalMetatype::Simple);
    for (const auto& [type, string] : V3LogicalMetatypeEncoding) {
        if (type == value) {
            return string;
        }
    }
    YT_VERIFY(value == ELogicalMetatype::VariantStruct || value == ELogicalMetatype::VariantTuple);
    return "variant";
}

struct TTypeV3MemberWrapper
{
    TStructField Member;
};

void Serialize(const TTypeV3MemberWrapper& wrapper, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer).BeginMap()
        .Item("type").Value(TTypeV3LogicalTypeWrapper{wrapper.Member.Type})
        .Item("name").Value(wrapper.Member.Name)
    .EndMap();
}

void Deserialize(TTypeV3MemberWrapper& wrapper, NYTree::INodePtr node)
{
    const auto& mapNode = node->AsMap();
    auto wrappedType = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(mapNode->GetChildOrThrow("type"));
    wrapper.Member.Type = wrappedType.LogicalType;
    wrapper.Member.Name = NYTree::ConvertTo<TString>(mapNode->GetChildOrThrow("name"));
}

struct TTypeV3ElementWrapper
{
    TLogicalTypePtr Element;
};

void Serialize(const TTypeV3ElementWrapper& wrapper, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer).BeginMap()
        .Item("type").Value(TTypeV3LogicalTypeWrapper{wrapper.Element})
    .EndMap();
}

void Deserialize(TTypeV3ElementWrapper& wrapper, NYTree::INodePtr node)
{
    const auto& mapNode = node->AsMap();
    auto wrappedElement = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(mapNode->GetChildOrThrow("type"));
    wrapper.Element = wrappedElement.LogicalType;
}

void Serialize(const TTypeV3LogicalTypeWrapper& wrapper, NYson::IYsonConsumer* consumer)
{
    using TWrapper = TTypeV3LogicalTypeWrapper;

    const auto metatype = wrapper.LogicalType->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
            NYTree::BuildYsonFluently(consumer)
                .Value(ToTypeV3(wrapper.LogicalType->AsSimpleTypeRef().GetElement()));
            return;
        case ELogicalMetatype::Decimal:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("precision").Value(wrapper.LogicalType->AsDecimalTypeRef().GetPrecision())
                    .Item("scale").Value(wrapper.LogicalType->AsDecimalTypeRef().GetScale())
                .EndMap();
            return;
        case ELogicalMetatype::Optional:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("item").Value(TWrapper{wrapper.LogicalType->AsOptionalTypeRef().GetElement()})
                .EndMap();
            return;
        case ELogicalMetatype::List:
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("item").Value(TWrapper{wrapper.LogicalType->AsListTypeRef().GetElement()})
                .EndMap();
            return;
        case ELogicalMetatype::Struct:
        case ELogicalMetatype::VariantStruct: {
            const auto& fields = wrapper.LogicalType->GetFields();

            std::vector<TTypeV3MemberWrapper> wrappedMembers;
            wrappedMembers.reserve(fields.size());
            for (const auto& f : fields) {
                wrappedMembers.emplace_back(TTypeV3MemberWrapper{f});
            }

            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("members").Value(wrappedMembers)
                .EndMap();
            return;
        }
        case ELogicalMetatype::Tuple:
        case ELogicalMetatype::VariantTuple: {
            const auto& elements = wrapper.LogicalType->GetElements();

            std::vector<TTypeV3ElementWrapper> wrappedElements;
            wrappedElements.reserve(elements.size());
            for (const auto& e : elements) {
                wrappedElements.emplace_back(TTypeV3ElementWrapper{e});
            }

            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("elements").Value(wrappedElements)
                .EndMap();
            return;
        }
        case ELogicalMetatype::Dict: {
            const auto& key = wrapper.LogicalType->AsDictTypeRef().GetKey();
            const auto& value = wrapper.LogicalType->AsDictTypeRef().GetValue();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("key").Value(TWrapper{key})
                    .Item("value").Value(TWrapper{value})
                .EndMap();
            return;
        }
        case ELogicalMetatype::Tagged: {
            const auto& element = wrapper.LogicalType->AsTaggedTypeRef().GetElement();
            const auto& tag = wrapper.LogicalType->AsTaggedTypeRef().GetTag();
            NYTree::BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("type_name").Value(ToTypeV3(metatype))
                    .Item("tag").Value(tag)
                    .Item("item").Value(TWrapper{element})
                .EndMap();
            return;
        }
    }
    YT_ABORT();
}

void Deserialize(TTypeV3LogicalTypeWrapper& wrapper, NYTree::INodePtr node)
{
    if (node->GetType() == NYTree::ENodeType::String) {
        auto typeNameString = node->AsString()->GetValue();
        auto typeName = FromTypeV3(typeNameString);
        std::visit([&] (const auto& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, ESimpleLogicalValueType>) {
                wrapper.LogicalType = SimpleLogicalType(arg);
            } else {
                static_assert(std::is_same_v<T, ELogicalMetatype> || std::is_same_v<T, TV3Variant>);
                THROW_ERROR_EXCEPTION("Type %Qv must be represented by map, not a string",
                    typeNameString);
            }
        }, typeName);
        return;
    }
    if (node->GetType() != NYTree::ENodeType::Map) {
        THROW_ERROR_EXCEPTION("Error parsing logical type: expected %Qlv or %Qlv, actual %Qlv",
            NYTree::ENodeType::Map,
            NYTree::ENodeType::String,
            node->GetType());
    }

    auto mapNode = node->AsMap();
    auto typeNameString = mapNode->GetChildValueOrThrow<TString>("type_name");
    auto typeName = FromTypeV3(typeNameString);
    std::visit([&] (const auto& typeName) {
        using T = std::decay_t<decltype(typeName)>;
        if constexpr (std::is_same_v<T, ESimpleLogicalValueType>) {
            wrapper.LogicalType = SimpleLogicalType(typeName);
        } else {
            ELogicalMetatype type;
            if constexpr (std::is_same_v<T, ELogicalMetatype>) {
                type = typeName;
            } else {
                const bool hasMembers = static_cast<bool>(mapNode->FindChild("members"));
                const bool hasElements =  static_cast<bool>(mapNode->FindChild("elements"));
                if (hasMembers && hasElements) {
                    THROW_ERROR_EXCEPTION("\"variant\" cannot have both children \"elements\" and \"members\"");
                } else if (hasMembers) {
                    type = ELogicalMetatype::VariantStruct;
                } else if (hasElements) {
                    type = ELogicalMetatype::VariantTuple;
                } else {
                    THROW_ERROR_EXCEPTION("\"variant\" must have \"elements\" or \"members\" child");
                }
            }
            switch (type) {
                case ELogicalMetatype::Simple:
                    // NB. FromTypeV3 never returns this value.
                    YT_ABORT();
                case ELogicalMetatype::Decimal: {
                    auto precision = mapNode->GetChildValueOrThrow<int>("precision");
                    auto scale = mapNode->GetChildValueOrThrow<int>("scale");
                    wrapper.LogicalType = DecimalLogicalType(precision, scale);
                    return;
                }
                case ELogicalMetatype::Optional: {
                    auto itemNode = mapNode->GetChildOrThrow("item");
                    auto item = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(itemNode);
                    wrapper.LogicalType = OptionalLogicalType(std::move(item.LogicalType));
                    return;
                }
                case ELogicalMetatype::List: {
                    auto itemNode = mapNode->GetChildOrThrow("item");
                    auto wrappedItem = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(itemNode);
                    wrapper.LogicalType = ListLogicalType(std::move(wrappedItem.LogicalType));
                    return;
                }
                case ELogicalMetatype::Struct:
                case ELogicalMetatype::VariantStruct: {
                    auto membersNode = mapNode->GetChildOrThrow("members");
                    auto wrappedMembers = NYTree::ConvertTo<std::vector<TTypeV3MemberWrapper>>(membersNode);

                    std::vector<TStructField> members;
                    members.reserve(wrappedMembers.size());
                    for (auto& w : wrappedMembers) {
                        members.emplace_back(w.Member);
                    }

                    if (type == ELogicalMetatype::Struct) {
                        wrapper.LogicalType = StructLogicalType(std::move(members));
                    } else {
                        wrapper.LogicalType = VariantStructLogicalType(std::move(members));
                    }
                    return;
                }
                case ELogicalMetatype::Tuple:
                case ELogicalMetatype::VariantTuple: {
                    auto elementsNode = mapNode->GetChildOrThrow("elements");
                    auto elementsV3 = NYTree::ConvertTo<std::vector<TTypeV3ElementWrapper>>(elementsNode);

                    std::vector<TLogicalTypePtr> elements;
                    elements.reserve(elementsV3.size());
                    for (auto& e : elementsV3) {
                        elements.emplace_back(std::move(e.Element));
                    }
                    if (type == ELogicalMetatype::Tuple) {
                        wrapper.LogicalType = TupleLogicalType(std::move(elements));
                    } else {
                        wrapper.LogicalType = VariantTupleLogicalType(std::move(elements));
                    }
                    return;
                }
                case ELogicalMetatype::Dict: {
                    auto keyNode = mapNode->GetChildOrThrow("key");
                    auto key = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(keyNode);
                    auto valueNode = mapNode->GetChildOrThrow("value");
                    auto value = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(valueNode);
                    wrapper.LogicalType = DictLogicalType(std::move(key.LogicalType), std::move(value.LogicalType));
                    return;
                }
                case ELogicalMetatype::Tagged: {
                    auto tagNode = mapNode->GetChildOrThrow("tag");
                    auto tag = NYTree::ConvertTo<TString>(tagNode);
                    auto elementNode = mapNode->GetChildOrThrow("item");
                    auto element = NYTree::ConvertTo<TTypeV3LogicalTypeWrapper>(elementNode);
                    wrapper.LogicalType = TaggedLogicalType(std::move(tag), std::move(element.LogicalType));
                    return;
                }
            }
            YT_ABORT();
        }
    }, typeName);
}

void DeserializeV3Impl(TLogicalTypePtr& type, TYsonPullParserCursor* cursor, int depth)
{
    // Check depth early to avoid stack overflow.
    if (depth > MaxSchemaDepth) {
        THROW_ERROR_EXCEPTION("Logical type exceeds depth limit during parsing")
            << TErrorAttribute("limit", MaxSchemaDepth);
    }

    if ((*cursor)->GetType() == EYsonItemType::StringValue) {
        auto typeNameString = (*cursor)->UncheckedAsString();
        auto typeName = FromTypeV3(typeNameString);
        std::visit([&] (const auto& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, ESimpleLogicalValueType>) {
                type = SimpleLogicalType(arg);
            } else {
                static_assert(std::is_same_v<T, ELogicalMetatype> || std::is_same_v<T, TV3Variant>);
                THROW_ERROR_EXCEPTION("Type %Qv must be represented by map, not a string",
                    typeNameString);
            }
        }, typeName);
        cursor->Next();
        return;
    }
    if ((*cursor)->GetType() != EYsonItemType::BeginMap) {
        THROW_ERROR_EXCEPTION("Error parsing logical type: expected %Qlv or %Qlv, actual %Qlv",
            EYsonItemType::BeginMap,
            EYsonItemType::BeginList,
            (*cursor)->GetType());
    }

    std::optional<TV3TypeName> typeName;
    std::optional<std::vector<TStructField>> members;
    std::optional<std::vector<TLogicalTypePtr>> elements;
    TLogicalTypePtr item, keyType, valueType;
    std::optional<i64> precision, scale;
    std::optional<TString> tag;

    cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
        EnsureYsonToken(TStringBuf("logical type attribute key"), *cursor, EYsonItemType::StringValue);
        auto key = (*cursor)->UncheckedAsString();
        if (key == TStringBuf("type_name")) {
            cursor->Next();
            EnsureYsonToken(TStringBuf("logical type name"), *cursor, EYsonItemType::StringValue);
            typeName = FromTypeV3((*cursor)->UncheckedAsString());
            cursor->Next();
        } else if (key == TStringBuf("item")) {
            cursor->Next();
            DeserializeV3Impl(item, cursor, depth + 1);
        } else if (key == TStringBuf("members")) {
            cursor->Next();
            members.emplace();
            cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
                std::optional<TString> name;
                TLogicalTypePtr type;
                cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                    EnsureYsonToken(TStringBuf("logical type member attribute key"), *cursor, EYsonItemType::StringValue);
                    auto key = (*cursor)->UncheckedAsString();
                    if (key == TStringBuf("name")) {
                        cursor->Next();
                        name = ExtractTo<TString>(cursor);
                    } else if (key == TStringBuf("type")) {
                        cursor->Next();
                        DeserializeV3Impl(type, cursor, depth + 1);
                    } else {
                        cursor->Next();
                        cursor->SkipComplexValue();
                    }
                });
                if (!name) {
                    THROW_ERROR_EXCEPTION("Name is required");
                }
                if (!type) {
                    THROW_ERROR_EXCEPTION("Type is required");
                }
                members->push_back({std::move(*name), std::move(type)});
            });
        } else if (key == TStringBuf("elements")) {
            cursor->Next();
            elements.emplace();
            cursor->ParseList([&] (TYsonPullParserCursor* cursor) {
                TLogicalTypePtr type;
                cursor->ParseMap([&] (TYsonPullParserCursor* cursor) {
                    EnsureYsonToken(TStringBuf("logical type member attribute key"), *cursor, EYsonItemType::StringValue);
                    auto key = (*cursor)->UncheckedAsString();
                    if (key == TStringBuf("type")) {
                        cursor->Next();
                        DeserializeV3Impl(type, cursor, depth + 1);
                    } else {
                        cursor->Next();
                        cursor->SkipComplexValue();
                    }
                });
                if (!type) {
                    THROW_ERROR_EXCEPTION("Type is required");
                }
                elements->push_back(std::move(type));
            });
        } else if (key == TStringBuf("precision")) {
            cursor->Next();
            precision = ExtractTo<i64>(cursor);
        } else if (key == TStringBuf("scale")) {
            cursor->Next();
            scale = ExtractTo<i64>(cursor);
        } else if (key == TStringBuf("key")) {
            cursor->Next();
            DeserializeV3Impl(keyType, cursor, depth + 1);
        } else if (key == TStringBuf("value")) {
            cursor->Next();
            DeserializeV3Impl(valueType, cursor, depth + 1);
        } else if (key == TStringBuf("tag")) {
            cursor->Next();
            tag = ExtractTo<TString>(cursor);
        } else {
            cursor->Next();
            cursor->SkipComplexValue();
        }
    });

    if (!typeName) {
        THROW_ERROR_EXCEPTION("\"type_name\" is required");
    }

    type = std::visit([&] (const auto& typeName) {
        using T = std::decay_t<decltype(typeName)>;
        if constexpr (std::is_same_v<T, ESimpleLogicalValueType>) {
            return SimpleLogicalType(typeName);
        } else {
            ELogicalMetatype type;
            if constexpr (std::is_same_v<T, ELogicalMetatype>) {
                type = typeName;
            } else {
                if (members && elements) {
                    THROW_ERROR_EXCEPTION("\"variant\" cannot have both children \"elements\" and \"members\"");
                } else if (members) {
                    type = ELogicalMetatype::VariantStruct;
                } else if (elements) {
                    type = ELogicalMetatype::VariantTuple;
                } else {
                    THROW_ERROR_EXCEPTION("\"variant\" must have \"elements\" or \"members\" child");
                }
            }
            auto ensureIsPresent = [&] (const char* fieldName, const auto& value) {
                if (!value) {
                    THROW_ERROR_EXCEPTION("Field %Qv is required for logical type %Qlv",
                        fieldName,
                        type);
                }
            };
            switch (type) {
                case ELogicalMetatype::Simple:
                    // NB. FromTypeV3 never returns this value.
                    YT_ABORT();
                case ELogicalMetatype::Decimal:
                    ensureIsPresent("precision", precision);
                    ensureIsPresent("scale", scale);
                    return DecimalLogicalType(*precision, *scale);
                case ELogicalMetatype::Optional:
                    ensureIsPresent("item", item);
                    return OptionalLogicalType(std::move(item));
                case ELogicalMetatype::List:
                    ensureIsPresent("item", item);
                    return ListLogicalType(std::move(item));
                case ELogicalMetatype::Struct:
                    ensureIsPresent("members", members);
                    return StructLogicalType(std::move(*members));
                case ELogicalMetatype::VariantStruct:
                    ensureIsPresent("members", members);
                    return VariantStructLogicalType(std::move(*members));
                case ELogicalMetatype::Tuple:
                    ensureIsPresent("elements", elements);
                    return TupleLogicalType(std::move(*elements));
                case ELogicalMetatype::VariantTuple:
                    ensureIsPresent("elements", elements);
                    return VariantTupleLogicalType(std::move(*elements));
                case ELogicalMetatype::Dict:
                    ensureIsPresent("key", keyType);
                    ensureIsPresent("value", valueType);
                    return DictLogicalType(std::move(keyType), std::move(valueType));
                case ELogicalMetatype::Tagged:
                    ensureIsPresent("tag", tag);
                    ensureIsPresent("item", item);
                    return TaggedLogicalType(std::move(*tag), std::move(item));
            }
            YT_ABORT();
        }
    }, *typeName);
}

void Deserialize(TTypeV3LogicalTypeWrapper& wrapper, TYsonPullParserCursor* cursor)
{
    DeserializeV3(wrapper.LogicalType, cursor);
}

void DeserializeV3(TLogicalTypePtr& type, TYsonPullParserCursor* cursor)
{
    DeserializeV3Impl(type, cursor, /*depth*/ 0);
}

void Deserialize(TLogicalTypePtr& type, NYson::TYsonPullParserCursor* cursor)
{
    YT_VERIFY(UseTypeV3ForSerialization);
    DeserializeV3(type, cursor);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeStore
{
public:
    TSimpleTypeStore()
    {
        for (auto simpleLogicalType : TEnumTraits<ESimpleLogicalValueType>::GetDomainValues()) {
            auto logicalType = New<TSimpleLogicalType>(simpleLogicalType);
            SimpleTypeMap_[simpleLogicalType] = logicalType;
            OptionalTypeMap_[simpleLogicalType] = New<TOptionalLogicalType>(logicalType);
        }
    }

    const TLogicalTypePtr& GetSimpleType(ESimpleLogicalValueType type)
    {
        return GetOrCrash(SimpleTypeMap_, type);
    }

    const TLogicalTypePtr& GetOptionalType(ESimpleLogicalValueType type)
    {
        return GetOrCrash(OptionalTypeMap_, type);
    }

private:
    THashMap<ESimpleLogicalValueType, TLogicalTypePtr> SimpleTypeMap_;
    THashMap<ESimpleLogicalValueType, TLogicalTypePtr> OptionalTypeMap_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr OptionalLogicalType(TLogicalTypePtr element)
{
    if (element->GetMetatype() == ELogicalMetatype::Simple) {
        auto simpleLogicalType = element->AsSimpleTypeRef().GetElement();
        if (element.Get() == Singleton<TSimpleTypeStore>()->GetSimpleType(simpleLogicalType).Get()) {
            return Singleton<TSimpleTypeStore>()->GetOptionalType(simpleLogicalType);
        }
    }
    return New<TOptionalLogicalType>(std::move(element));
}

TLogicalTypePtr MakeOptionalIfNot(TLogicalTypePtr element)
{
    if (element->GetMetatype() == ELogicalMetatype::Optional) {
        return element;
    }
    return OptionalLogicalType(std::move(element));
}

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element)
{
    return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
}

TLogicalTypePtr DecimalLogicalType(int precision, int scale)
{
    return New<TDecimalLogicalType>(precision, scale);
}

TLogicalTypePtr MakeLogicalType(ESimpleLogicalValueType element, bool required)
{
    if (element == ESimpleLogicalValueType::Null || element == ESimpleLogicalValueType::Void) {
        if (required) {
            THROW_ERROR_EXCEPTION("Null type cannot be required");
        }
        return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
    } else if (required) {
        return Singleton<TSimpleTypeStore>()->GetSimpleType(element);
    } else {
        return Singleton<TSimpleTypeStore>()->GetOptionalType(element);
    }
}

TLogicalTypePtr ListLogicalType(TLogicalTypePtr element)
{
    return New<TListLogicalType>(std::move(element));
}

TLogicalTypePtr StructLogicalType(std::vector<TStructField> fields)
{
    return New<TStructLogicalType>(std::move(fields));
}

TLogicalTypePtr TupleLogicalType(std::vector<TLogicalTypePtr> elements)
{
    return New<TTupleLogicalType>(std::move(elements));
}

TLogicalTypePtr VariantTupleLogicalType(std::vector<TLogicalTypePtr> elements)
{
    return New<TVariantTupleLogicalType>(std::move(elements));
}

TLogicalTypePtr VariantStructLogicalType(std::vector<TStructField> fields)
{
    return New<TVariantStructLogicalType>(std::move(fields));
}

TLogicalTypePtr DictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value)
{
    return New<TDictLogicalType>(std::move(key), std::move(value));
}

TLogicalTypePtr TaggedLogicalType(TString tag, TLogicalTypePtr element)
{
    return New<TTaggedLogicalType>(std::move(tag), std::move(element));
}

TLogicalTypePtr NullLogicalType()
{
    return Singleton<TSimpleTypeStore>()->GetSimpleType(ESimpleLogicalValueType::Null);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

static inline size_t GetHash(
    const THash<NYT::NTableClient::TLogicalType>& hasher,
    const std::vector<NYT::NTableClient::TLogicalTypePtr>& elements)
{
    size_t result = 0;
    for (const auto& element : elements) {
        result = CombineHashes(result, hasher(*element));
    }
    return result;
}

static inline size_t GetHash(
    const THash<NYT::NTableClient::TLogicalType>& hasher,
    const std::vector<NYT::NTableClient::TStructField>& fields)
{
    size_t result = 0;
    for (const auto& field : fields) {
        result = CombineHashes(result, THash<TString>{}(field.Name));
        result = CombineHashes(result, hasher(*field.Type));
    }
    return result;
}

size_t THash<NYT::NTableClient::TLogicalType>::operator()(const NYT::NTableClient::TLogicalType& logicalType) const
{
    using namespace NYT::NTableClient;
    const auto typeHash = static_cast<size_t>(logicalType.GetMetatype());
    switch (logicalType.GetMetatype()) {
        case ELogicalMetatype::Simple:
            return CombineHashes(static_cast<size_t>(logicalType.AsSimpleTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::Decimal:
            return CombineHashes(
                CombineHashes(
                    static_cast<size_t>(logicalType.AsDecimalTypeRef().GetPrecision()),
                    static_cast<size_t>(logicalType.AsDecimalTypeRef().GetScale())),
                typeHash);
        case ELogicalMetatype::Optional:
            return CombineHashes((*this)(*logicalType.AsOptionalTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::List:
            return CombineHashes((*this)(*logicalType.AsListTypeRef().GetElement()), typeHash);
        case ELogicalMetatype::Struct:
            return CombineHashes(GetHash(*this, logicalType.AsStructTypeRef().GetFields()), typeHash);
        case ELogicalMetatype::Tuple:
            return CombineHashes(GetHash(*this, logicalType.AsTupleTypeRef().GetElements()), typeHash);
        case ELogicalMetatype::VariantStruct:
            return CombineHashes(GetHash(*this, logicalType.AsVariantStructTypeRef().GetFields()), typeHash);
        case ELogicalMetatype::VariantTuple:
            return CombineHashes(GetHash(*this, logicalType.AsVariantTupleTypeRef().GetElements()), typeHash);
        case ELogicalMetatype::Dict:
            return CombineHashes(
                CombineHashes(
                    (*this)(*logicalType.AsDictTypeRef().GetKey()),
                    (*this)(*logicalType.AsDictTypeRef().GetValue())),
                typeHash);
        case ELogicalMetatype::Tagged:
            return CombineHashes(
                CombineHashes(
                    THash<TString>()(logicalType.AsTaggedTypeRef().GetTag()),
                    (*this)(*logicalType.AsTaggedTypeRef().GetElement())),
                typeHash);
    }
    YT_ABORT();
}
