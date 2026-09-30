#include "skiff_schema.h"

#include "skiff.h"

#include <util/generic/hash.h>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TSkiffSchema& lhs, const TSkiffSchema& rhs)
{
    if (lhs.GetWireType() != rhs.GetWireType() || lhs.GetName() != rhs.GetName() || lhs.GetSize() != rhs.GetSize()) {
        return false;
    }
    const auto& lhsChildren = lhs.GetChildren();
    const auto& rhsChildren = rhs.GetChildren();
    return std::equal(
        std::begin(lhsChildren),
        std::end(lhsChildren),
        std::begin(rhsChildren),
        std::end(rhsChildren),
        TSkiffSchemaPtrEqual());
}

bool operator!=(const TSkiffSchema& lhs, const TSkiffSchema& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

void PrintShortDebugString(const std::shared_ptr<const TSkiffSchema>& schema, IOutputStream* out)
{
    (*out) << ToString(schema->GetWireType());
    if (schema->GetWireType() == EWireType::StringFixed) {
        (*out) << '(' << schema->GetSize() << ')';
    }
    if (!IsSimpleType(schema->GetWireType())) {
        auto children = schema->GetChildren();
        if (!children.empty()) {
            (*out) << '<';
            for (const auto& child : children) {
                PrintShortDebugString(child, out);
                (*out) << ';';
            }
            (*out) << '>';
        }
    }
}

TString GetShortDebugString(const std::shared_ptr<const TSkiffSchema>& schema)
{
    TStringStream out;
    PrintShortDebugString(schema, &out);
    return out.Str();
}

std::shared_ptr<TSimpleTypeSchema> CreateSimpleTypeSchema(EWireType type)
{
    if (!IsSimpleType(type)) {
        ythrow TSkiffException() << "WireType must be Simple, got \"" << ToString(type) << "\"";
    }
    return std::make_shared<TSimpleTypeSchema>(type);
}

std::shared_ptr<TStringFixedSchema> CreateStringFixedSchema(i64 size)
{
    if (size < 0 || size > MaxStringLength) {
        ythrow TSkiffException() << "\"" << ToString(EWireType::StringFixed) << "\" size " << size << " is out of range [0, " << MaxStringLength << "]";
    }
    return std::make_shared<TStringFixedSchema>(size);
}

static void VerifyNonemptyChildren(const TSkiffSchemaList& children, EWireType wireType)
{
    if (children.empty()) {
        ythrow TSkiffException() << "\"" << ToString(wireType) << "\" must have at least one child";
    }
}

std::shared_ptr<TTupleSchema> CreateTupleSchema(TSkiffSchemaList children)
{
    return std::make_shared<TTupleSchema>(std::move(children));
}

std::shared_ptr<TVariant8Schema> CreateVariant8Schema(TSkiffSchemaList children)
{
    VerifyNonemptyChildren(children, EWireType::Variant8);
    return std::make_shared<TVariant8Schema>(std::move(children));
}

std::shared_ptr<TVariant16Schema> CreateVariant16Schema(TSkiffSchemaList children)
{
    VerifyNonemptyChildren(children, EWireType::Variant16);
    return std::make_shared<TVariant16Schema>(std::move(children));
}

std::shared_ptr<TVariantVarSchema> CreateVariantVarSchema(TSkiffSchemaList children)
{
    VerifyNonemptyChildren(children, EWireType::VariantVar);
    return std::make_shared<TVariantVarSchema>(std::move(children));
}

std::shared_ptr<TRepeatedVariant8Schema> CreateRepeatedVariant8Schema(TSkiffSchemaList children)
{
    VerifyNonemptyChildren(children, EWireType::RepeatedVariant8);
    return std::make_shared<TRepeatedVariant8Schema>(std::move(children));
}

std::shared_ptr<TRepeatedVariant16Schema> CreateRepeatedVariant16Schema(TSkiffSchemaList children)
{
    VerifyNonemptyChildren(children, EWireType::RepeatedVariant16);
    return std::make_shared<TRepeatedVariant16Schema>(std::move(children));
}

std::shared_ptr<TRepeatedBlockVarSchema> CreateRepeatedBlockVarSchema(TSkiffSchemaList children)
{
    if (std::ssize(children) != 1) {
        ythrow TSkiffException() << "\"" << ToString(EWireType::RepeatedBlockVar) << "\" must have exactly one child";
    }
    return std::make_shared<TRepeatedBlockVarSchema>(std::move(children));
}

////////////////////////////////////////////////////////////////////////////////

TSkiffSchema::TSkiffSchema(EWireType type)
    : Type_(type)
{ }

EWireType TSkiffSchema::GetWireType() const
{
    return Type_;
}

std::shared_ptr<TSkiffSchema> TSkiffSchema::SetName(TString name)
{
    Name_ = std::move(name);
    return shared_from_this();
}

const TString& TSkiffSchema::GetName() const
{
    return Name_;
}

const TSkiffSchemaList& TSkiffSchema::GetChildren() const
{
    static const TSkiffSchemaList children;
    return children;
}

i64 TSkiffSchema::GetSize() const
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleTypeSchema::TSimpleTypeSchema(EWireType type)
    : TSkiffSchema(type)
{
    Y_ABORT_UNLESS(IsSimpleType(type));
}

////////////////////////////////////////////////////////////////////////////////

TStringFixedSchema::TStringFixedSchema(i64 size)
    : TSkiffSchema(EWireType::StringFixed)
    , Size_(size)
{
    Y_ABORT_UNLESS(0 <= size && size <= MaxStringLength);
}

i64 TStringFixedSchema::GetSize() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

size_t TSkiffSchemaPtrHasher::operator()(const std::shared_ptr<TSkiffSchema>& schema) const
{
    return THash<NSkiff::TSkiffSchema>()(*schema);
}

size_t TSkiffSchemaPtrEqual::operator()(
    const std::shared_ptr<TSkiffSchema>& lhs,
    const std::shared_ptr<TSkiffSchema>& rhs) const
{
    return *lhs == *rhs;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkiff

////////////////////////////////////////////////////////////////////////////////

size_t THash<NSkiff::TSkiffSchema>::operator()(const NSkiff::TSkiffSchema &schema) const
{
    auto hash = CombineHashes(
        THash<TString>()(schema.GetName()),
        static_cast<size_t>(schema.GetWireType()));
    for (const auto& child : schema.GetChildren()) {
        hash = CombineHashes(hash, (*this)(*child));
    }
    hash = CombineHashes(hash, static_cast<size_t>(schema.GetSize()));
    return hash;
}

////////////////////////////////////////////////////////////////////////////////
