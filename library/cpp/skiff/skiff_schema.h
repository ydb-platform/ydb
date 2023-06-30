#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/string/cast.h>

#include <vector>

namespace NSkiff {

////////////////////////////////////////////////////////////////////////////////

template <EWireType WireType>
class TComplexSchema;

using TTupleSchema = TComplexSchema<EWireType::Tuple>;
using TVariant8Schema = TComplexSchema<EWireType::Variant8>;
using TVariant16Schema = TComplexSchema<EWireType::Variant16>;
using TRepeatedVariant8Schema = TComplexSchema<EWireType::RepeatedVariant8>;
using TRepeatedVariant16Schema = TComplexSchema<EWireType::RepeatedVariant16>;

using TTupleSchemaPtr = std::shared_ptr<TTupleSchema>;
using TVariant8SchemaPtr = std::shared_ptr<TVariant8Schema>;
using TVariant16SchemaPtr = std::shared_ptr<TVariant16Schema>;
using TRepeatedVariant8SchemaPtr = std::shared_ptr<TRepeatedVariant8Schema>;
using TRepeatedVariant16SchemaPtr = std::shared_ptr<TRepeatedVariant16Schema>;



////////////////////////////////////////////////////////////////////////////////

class TSkiffSchema
    : public std::enable_shared_from_this<TSkiffSchema>
{
public:
    virtual ~TSkiffSchema() = default;

    EWireType GetWireType() const;
    std::shared_ptr<TSkiffSchema> SetName(TString name);
    const TString& GetName() const;

    virtual const TSkiffSchemaList& GetChildren() const;

protected:
    explicit TSkiffSchema(EWireType type);

private:
    const EWireType Type_;
    TString Name_;
};

bool operator==(const TSkiffSchema& lhs, const TSkiffSchema& rhs);
bool operator!=(const TSkiffSchema& lhs, const TSkiffSchema& rhs);

////////////////////////////////////////////////////////////////////////////////

class TSimpleTypeSchema
    : public TSkiffSchema
{
public:
    explicit TSimpleTypeSchema(EWireType type);
};

////////////////////////////////////////////////////////////////////////////////

template <EWireType WireType>
class TComplexSchema
    : public TSkiffSchema
{
public:
    explicit TComplexSchema(TSkiffSchemaList elements);

    virtual const TSkiffSchemaList& GetChildren() const override;

private:
    const TSkiffSchemaList Elements_;
};

////////////////////////////////////////////////////////////////////////////////

bool IsSimpleType(EWireType type);
TString GetShortDebugString(const std::shared_ptr<const TSkiffSchema>& schema);
void PrintShortDebugString(const std::shared_ptr<const TSkiffSchema>& schema, IOutputStream* out);

std::shared_ptr<TSimpleTypeSchema> CreateSimpleTypeSchema(EWireType type);
std::shared_ptr<TTupleSchema> CreateTupleSchema(TSkiffSchemaList children);
std::shared_ptr<TVariant8Schema> CreateVariant8Schema(TSkiffSchemaList children);
std::shared_ptr<TVariant16Schema> CreateVariant16Schema(TSkiffSchemaList children);
std::shared_ptr<TRepeatedVariant8Schema> CreateRepeatedVariant8Schema(TSkiffSchemaList children);
std::shared_ptr<TRepeatedVariant16Schema> CreateRepeatedVariant16Schema(TSkiffSchemaList children);

////////////////////////////////////////////////////////////////////////////////

struct TSkiffSchemaPtrHasher
{
    size_t operator()(const std::shared_ptr<TSkiffSchema>& schema) const;
};

struct TSkiffSchemaPtrEqual
{
    size_t operator()(
        const std::shared_ptr<TSkiffSchema>& lhs,
        const std::shared_ptr<TSkiffSchema>& rhs) const;
};

} // namespace NSkiff

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NSkiff::TSkiffSchema>
{
    size_t operator()(const NSkiff::TSkiffSchema& schema) const;
};

////////////////////////////////////////////////////////////////////////////////

#define SKIFF_SCHEMA_H
#include "skiff_schema-inl.h"
#undef SKIFF_SCHEMA_H
