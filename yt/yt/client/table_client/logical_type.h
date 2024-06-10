#pragma once

#include "public.h"

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/yson/public.h>
#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

#include <util/generic/hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TWalkContext;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELogicalMetatype,
    (Simple)
    (Optional)
    (List)
    (Struct)
    (Tuple)

    // Variant with named elements.
    (VariantStruct)

    // Variant with unnamed elements.
    (VariantTuple)

    // Dict is effectively list of pairs. We have restrictions on the key type.
    // YT doesn't check uniqueness of the keys.
    (Dict)

    (Tagged)

    (Decimal)
);

class TLogicalType
    : public virtual TRefCounted
{
public:
    explicit TLogicalType(ELogicalMetatype type);
    Y_FORCE_INLINE ELogicalMetatype GetMetatype() const;

    const TSimpleLogicalType& AsSimpleTypeRef() const;
    Y_FORCE_INLINE const TSimpleLogicalType& UncheckedAsSimpleTypeRef() const;
    const TDecimalLogicalType& AsDecimalTypeRef() const;
    Y_FORCE_INLINE const TDecimalLogicalType& UncheckedAsDecimalTypeRef() const;
    const TOptionalLogicalType& AsOptionalTypeRef() const;
    Y_FORCE_INLINE const TOptionalLogicalType& UncheckedAsOptionalTypeRef() const;
    const TListLogicalType& AsListTypeRef() const;
    Y_FORCE_INLINE const TListLogicalType& UncheckedAsListTypeRef() const;
    const TStructLogicalType& AsStructTypeRef() const;
    Y_FORCE_INLINE const TStructLogicalType& UncheckedAsStructTypeRef() const;
    const TTupleLogicalType& AsTupleTypeRef() const;
    Y_FORCE_INLINE const TTupleLogicalType& UncheckedAsTupleTypeRef() const;
    const TVariantTupleLogicalType& AsVariantTupleTypeRef() const;
    Y_FORCE_INLINE const TVariantTupleLogicalType& UncheckedAsVariantTupleTypeRef() const;
    const TVariantStructLogicalType& AsVariantStructTypeRef() const;
    Y_FORCE_INLINE const TVariantStructLogicalType& UncheckedAsVariantStructTypeRef() const;
    const TDictLogicalType& AsDictTypeRef() const;
    Y_FORCE_INLINE const TDictLogicalType& UncheckedAsDictTypeRef() const;
    const TTaggedLogicalType& AsTaggedTypeRef() const;
    Y_FORCE_INLINE const TTaggedLogicalType& UncheckedAsTaggedTypeRef() const;

    virtual size_t GetMemoryUsage() const = 0;
    virtual int GetTypeComplexity() const = 0;

    // This function doesn't validate children of current node.
    // Users should use ValidateLogicalType function.
    virtual void ValidateNode(const TWalkContext& context) const = 0;

    // Whether or not this type can have null value.
    virtual bool IsNullable() const = 0;

    //
    // Additional helpers to decompose complex type.
    // Logical type MUST have appropriate metatype otherwise abort() will be called.

    //
    // Return underlying element for Optional,List,Tagged.
    const TLogicalTypePtr& GetElement() const;

    //
    // Return elements for Tuple,VariantTuple
    const std::vector<TLogicalTypePtr>& GetElements() const;

    //
    // Return fields for Struct,VariantStruct
    const std::vector<TStructField>& GetFields() const;

private:
    const ELogicalMetatype Metatype_;
};

DEFINE_REFCOUNTED_TYPE(TLogicalType)

TString ToString(const TLogicalType& logicalType);
void FormatValue(TStringBuilderBase* builder, const TLogicalType& logicalType, TStringBuf /*spec*/);

//! Debug printers for Gtest unittests.
void PrintTo(ELogicalMetatype type, std::ostream* os);
void PrintTo(const TLogicalType& type, std::ostream* os);
void PrintTo(const TLogicalTypePtr& type, std::ostream* os);

bool operator == (const TLogicalType& lhs, const TLogicalType& rhs);
bool operator == (const TLogicalTypePtr& lhs, const TLogicalTypePtr& rhs) = delete;

void ValidateLogicalType(const TComplexTypeFieldDescriptor& descriptor, std::optional<int> depthLimit = std::nullopt);

// Function converts new type to old typesystem.
// The first element of result is ESimpleLogicalValue type corresponding to logicalType
// (as seen in `type` field of column schema).
// The second element of result is false if logicalType is Null or it is optional<A> where A is any type otherwise it's true.
std::pair<ESimpleLogicalValueType, bool> CastToV1Type(const TLogicalTypePtr& logicalType);

EValueType GetWireType(const TLogicalTypePtr& logicalType);

// Return true if given type is pure v1 type (i.e. expressible with `type` and `required` fields in schema).
bool IsV1Type(const TLogicalTypePtr& logicalType);

// Return true if this is new type expressible with EValueType::Composite type.
bool IsV3Composite(const TLogicalTypePtr& logicalType);

EValueType GetWireType(const TLogicalTypePtr& logicalType);

// Try to remove top level optional type if that doesn't change UnversionedValue representation
// of non null types.
TLogicalTypePtr DenullifyLogicalType(const TLogicalTypePtr& logicalType);

// Returns copy of the logical type with all tagged types replaces with its elements.
TLogicalTypePtr DetagLogicalType(const TLogicalTypePtr& logicalType);

void ToProto(NProto::TLogicalType* protoLogicalType, const TLogicalTypePtr& logicalType);
void FromProto(TLogicalTypePtr* logicalType, const NProto::TLogicalType& protoLogicalType);

bool IsComparable(const TLogicalTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

// Special wrapper class that allows to serialize LogicalType in type_v3 format
// https://a.yandex-team.ru/arc/trunk/arcadia/logfeller/mvp/docs/types_serialization.md
struct TTypeV3LogicalTypeWrapper
{
    TLogicalTypePtr LogicalType;
};

void Serialize(const TTypeV3LogicalTypeWrapper& wrapper, NYson::IYsonConsumer* consumer);
void Deserialize(TTypeV3LogicalTypeWrapper& wrapper, NYTree::INodePtr node);
void Deserialize(TTypeV3LogicalTypeWrapper& wrapper, NYson::TYsonPullParserCursor* cursor);

void Serialize(const TLogicalTypePtr& type, NYson::IYsonConsumer* consumer);
void Deserialize(TLogicalTypePtr& type, NYTree::INodePtr node);

void DeserializeV3(TLogicalTypePtr& type, NYson::TYsonPullParserCursor* cursor);
void Deserialize(TLogicalTypePtr& type, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

class TDecimalLogicalType
    : public TLogicalType
{
public:
    static constexpr int MinPrecision = 1;
    static constexpr int MaxPrecision = 35;

public:
    TDecimalLogicalType(int precision, int scale);

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

    Y_FORCE_INLINE int GetPrecision() const;
    Y_FORCE_INLINE int GetScale() const;

private:
    const int Precision_;
    const int Scale_;
};

////////////////////////////////////////////////////////////////////////////////

class TOptionalLogicalType
    : public TLogicalType
{
public:
    explicit TOptionalLogicalType(TLogicalTypePtr element);

    Y_FORCE_INLINE const TLogicalTypePtr& GetElement() const;

    std::optional<ESimpleLogicalValueType> Simplify() const;

    // Cached value of GetElement()->IsNullable(), useful for performance reasons.
    Y_FORCE_INLINE bool IsElementNullable() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    const TLogicalTypePtr Element_;
    const bool ElementIsNullable_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleLogicalType
    : public TLogicalType
{
public:
    explicit TSimpleLogicalType(ESimpleLogicalValueType element);

    Y_FORCE_INLINE ESimpleLogicalValueType GetElement() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    ESimpleLogicalValueType Element_;
};

////////////////////////////////////////////////////////////////////////////////

class TListLogicalType
    : public TLogicalType
{
public:
    explicit TListLogicalType(TLogicalTypePtr element);

    Y_FORCE_INLINE const TLogicalTypePtr& GetElement() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    TLogicalTypePtr Element_;
};

////////////////////////////////////////////////////////////////////////////////

// Class builds descriptors of a complex value field.
// Such descriptors are useful for generating error messages when working with complex types.
class TComplexTypeFieldDescriptor
{
public:
    explicit TComplexTypeFieldDescriptor(TLogicalTypePtr type);
    explicit TComplexTypeFieldDescriptor(const TColumnSchema& column);
    TComplexTypeFieldDescriptor(TString columnName, TLogicalTypePtr type);

    TComplexTypeFieldDescriptor OptionalElement() const;
    TComplexTypeFieldDescriptor ListElement() const;
    TComplexTypeFieldDescriptor Element(size_t i) const;
    TComplexTypeFieldDescriptor TupleElement(size_t i) const;
    TComplexTypeFieldDescriptor VariantTupleElement(size_t i) const;
    TComplexTypeFieldDescriptor Field(size_t i) const;
    TComplexTypeFieldDescriptor StructField(size_t i) const;
    TComplexTypeFieldDescriptor VariantStructField(size_t i) const;
    TComplexTypeFieldDescriptor DictKey() const;
    TComplexTypeFieldDescriptor DictValue() const;
    TComplexTypeFieldDescriptor TaggedElement() const;

    TComplexTypeFieldDescriptor Detag() const;

    const TString& GetDescription() const;
    const TLogicalTypePtr& GetType() const;

private:
    TString Descriptor_;
    TLogicalTypePtr Type_;
};

////////////////////////////////////////////////////////////////////////////////

struct TStructField
{
    TString Name;
    TLogicalTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

// Base class for struct and named variant.
class TStructLogicalTypeBase
    : public TLogicalType
{
public:

public:
    TStructLogicalTypeBase(ELogicalMetatype metatype, std::vector<TStructField> fields);
    Y_FORCE_INLINE const std::vector<TStructField>& GetFields() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    std::vector<TStructField> Fields_;
};

////////////////////////////////////////////////////////////////////////////////

class TTupleLogicalTypeBase
    : public TLogicalType
{
public:
    explicit TTupleLogicalTypeBase(ELogicalMetatype metatype, std::vector<TLogicalTypePtr> elements);

    Y_FORCE_INLINE const std::vector<TLogicalTypePtr>& GetElements() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    std::vector<TLogicalTypePtr> Elements_;
};

////////////////////////////////////////////////////////////////////////////////

class TStructLogicalType
    : public TStructLogicalTypeBase
{
public:
    TStructLogicalType(std::vector<TStructField> fields);
};

////////////////////////////////////////////////////////////////////////////////

class TTupleLogicalType
    : public TTupleLogicalTypeBase
{
public:
    TTupleLogicalType(std::vector<TLogicalTypePtr> elements);
};

////////////////////////////////////////////////////////////////////////////////

class TVariantStructLogicalType
    : public TStructLogicalTypeBase
{
public:
    explicit TVariantStructLogicalType(std::vector<TStructField> fields);
};

////////////////////////////////////////////////////////////////////////////////

class TVariantTupleLogicalType
    : public TTupleLogicalTypeBase
{
public:
    explicit TVariantTupleLogicalType(std::vector<TLogicalTypePtr> elements);
};

////////////////////////////////////////////////////////////////////////////////

class TDictLogicalType
    : public TLogicalType
{
public:
    TDictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value);

    Y_FORCE_INLINE const TLogicalTypePtr& GetKey() const;
    Y_FORCE_INLINE const TLogicalTypePtr& GetValue() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    TLogicalTypePtr Key_;
    TLogicalTypePtr Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TTaggedLogicalType
    : public TLogicalType
{
public:
    TTaggedLogicalType(TString tag, TLogicalTypePtr element);

    Y_FORCE_INLINE const TString& GetTag() const;
    Y_FORCE_INLINE const TLogicalTypePtr& GetElement() const;

    size_t GetMemoryUsage() const override;
    int GetTypeComplexity() const override;
    void ValidateNode(const TWalkContext& context) const override;
    bool IsNullable() const override;

private:
    const TString Tag_;
    const TLogicalTypePtr Element_;
};

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr SimpleLogicalType(ESimpleLogicalValueType element);
TLogicalTypePtr DecimalLogicalType(int precision, int scale);
TLogicalTypePtr OptionalLogicalType(TLogicalTypePtr element);
TLogicalTypePtr ListLogicalType(TLogicalTypePtr element);
TLogicalTypePtr StructLogicalType(std::vector<TStructField> fields);
TLogicalTypePtr TupleLogicalType(std::vector<TLogicalTypePtr> elements);
TLogicalTypePtr VariantStructLogicalType(std::vector<TStructField> fields);
TLogicalTypePtr VariantTupleLogicalType(std::vector<TLogicalTypePtr> elements);
TLogicalTypePtr DictLogicalType(TLogicalTypePtr key, TLogicalTypePtr value);
TLogicalTypePtr TaggedLogicalType(TString tag, TLogicalTypePtr element);
TLogicalTypePtr NullLogicalType();

TLogicalTypePtr MakeOptionalIfNot(TLogicalTypePtr element);

// Creates logical type from legacy schema fields.
// IMPORTANT: Only used for compatibility reasons.
// In modern code, one should use OptionalLogicalType + SimpleLogicalType instead.
TLogicalTypePtr MakeLogicalType(ESimpleLogicalValueType type, bool required);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

template <>
struct THash<NYT::NTableClient::TLogicalType>
{
    size_t operator() (const NYT::NTableClient::TLogicalType& logicalType) const;
};

#define LOGICAL_TYPE_INL_H_
#include "logical_type-inl.h"
#undef LOGICAL_TYPE_INL_H_
