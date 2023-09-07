#pragma once

#include <yt/yt/client/formats/config.h>

#include "private.h"

#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

class TEnumerationDescription
{
public:
    explicit TEnumerationDescription(const TString& name);

    const TString& GetEnumerationName() const;

    const TString& GetValueName(i32 value) const;
    const TString* TryGetValueName(i32 value) const;

    i32 GetValue(TStringBuf name) const;
    std::optional<i32> TryGetValue(TStringBuf name) const;

    void Add(TString name, i32 value);

private:
    THashMap<TString, i32> NameToValue_;
    THashMap<i32, TString> ValueToName_;
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtobufTag
{
    ui64 WireTag = 0;
    size_t TagSize = 0;
    // Extracts field number from |WireTag|.
    ui32 GetFieldNumber() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtobufFieldDescriptionBase
    : public TProtobufTag
{
    TString Name;

    // Index of field inside struct (for fields corresponding to struct fields in schema).
    int StructFieldIndex = 0;

    bool Repeated = false;

    // Is a repeated field packed (i.e. it is encoded as `<tag> <length> <value1> ... <valueK>`)?
    bool Packed = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtobufTypeBase
    : public TRefCounted
{
    EProtobufType ProtoType = EProtobufType::Int64;

    // Is the corresponding type in schema optional?
    bool Optional = true;

    // Number of fields in struct in schema (only for |Type == StructuredMessage|).
    int StructFieldCount = 0;

    const TEnumerationDescription* EnumerationDescription = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

struct TProtobufWriterEmbeddingDescription
    : public TProtobufTag
{
    static const int InvalidIndex = -1;
    // Index of wrapping proto message
    int ParentEmbeddingIndex = InvalidIndex;
};

class TProtobufWriterFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    // Index of wrapping proto message
    int ParentEmbeddingIndex = TProtobufWriterEmbeddingDescription::InvalidIndex;

    // Whether to fail on writing unknown value to the enumeration.
    EProtobufEnumWritingMode EnumWritingMode = EProtobufEnumWritingMode::CheckValues;

    TProtobufWriterTypePtr Type;
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriterType
    : public TProtobufTypeBase
{
public:
    int AddEmbedding(
        int parentParentEmbeddingIndex,
        const TProtobufColumnConfigPtr& embeddingConfig);

    void AddChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        std::unique_ptr<TProtobufWriterFieldDescription> childType,
        std::optional<int> fieldIndex,
        std::optional<int> parentParentEmbeddingIndex = std::nullopt);

    void IgnoreChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber);

    const TProtobufWriterFieldDescription* FindAlternative(int alternativeIndex) const;

public:
    std::vector<std::unique_ptr<TProtobufWriterFieldDescription>> Children;
    std::vector<TProtobufWriterEmbeddingDescription> Embeddings;

private:
    std::vector<int> AlternativeToChildIndex_;
    static constexpr int InvalidChildIndex = -1;
};

DEFINE_REFCOUNTED_TYPE(TProtobufWriterType)

////////////////////////////////////////////////////////////////////////////////

class TProtobufParserType
    : public TProtobufTypeBase
{
private:
    struct TFieldNumberToChildIndex
    {
        std::vector<int> FieldNumberToChildIndexVector;
        THashMap<int, int> FieldNumberToChildIndexMap;
    };

public:
    int AddEmbedding(
        int parentParentEmbeddingIndex,
        const TProtobufColumnConfigPtr& embeddingConfig);

    void AddChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        std::unique_ptr<TProtobufParserFieldDescription> childType,
        std::optional<int> fieldIndex,
        std::optional<int> parentParentEmbeddingIndex = std::nullopt);

    void IgnoreChild(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber);

    // Returns std::nullopt iff the field is ignored (missing from table schema).
    // Throws an exception iff the field number is unknown.
    std::optional<int> FieldNumberToChildIndex(int fieldNumber, const TFieldNumberToChildIndex* store = nullptr) const;
    std::optional<int> FieldNumberToEmbeddedChildIndex(int fieldNumber) const;

    void SetEmbeddedChildIndex(
        int fieldNumber,
        int childIndex);
private:
    void SetChildIndex(
        const std::optional<NTableClient::TComplexTypeFieldDescriptor>& descriptor,
        int fieldNumber,
        int childIndex,
        TFieldNumberToChildIndex* store = nullptr);

public:
    // For oneof types -- corresponding oneof description.
    const TProtobufParserFieldDescription* Field = nullptr;

    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> Children;

private:
    static constexpr int InvalidChildIndex = -1;
    static constexpr int IgnoredChildIndex = -2;
    static constexpr int MaxFieldNumberVectorSize = 256;

    std::vector<int> IgnoredChildFieldNumbers_;
    std::vector<std::unique_ptr<TProtobufParserFieldDescription>> OneofDescriptions_;

    TFieldNumberToChildIndex FieldNumberToChildIndex_;
    TFieldNumberToChildIndex FieldNumberToEmbeddedChildIndex_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufParserType)


class TProtobufParserFieldDescription
    : public TProtobufFieldDescriptionBase
{
public:
    bool IsOneofAlternative() const
    {
        return AlternativeIndex.has_value();
    }

public:
    TProtobufParserTypePtr Type;

    // For oneof members -- index of alternative.
    std::optional<int> AlternativeIndex;
    // For oneof members -- containing oneof type.
    const TProtobufParserType* ContainingOneof;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TType>
class TProtobufTypeBuilder
{
public:
    static constexpr bool IsWriter = std::is_same_v<TType, TProtobufWriterType>;
    static constexpr bool AreNonOptionalMissingFieldsAllowed = IsWriter;

    using TTypePtr = NYT::TIntrusivePtr<TType>;
    using TField = std::conditional_t<IsWriter, TProtobufWriterFieldDescription, TProtobufParserFieldDescription>;
    using TFieldPtr = std::unique_ptr<TField>;

    TProtobufTypeBuilder(const THashMap<TString, TEnumerationDescription>& enumerations);

    TFieldPtr CreateField(
        int structFieldIndex,
        const TProtobufColumnConfigPtr& columnConfig,
        std::optional<NTableClient::TComplexTypeFieldDescriptor> maybeDescriptor,
        bool allowOtherColumns = false,
        bool allowEmbedded = false);

private:
    const THashMap<TString, TEnumerationDescription>& Enumerations_;

private:
    // Traverse type config, matching it with type descriptor from schema.
    //
    // Matching of the type config and type descriptor is performed by the following rules:
    //  * Field of simple type matches simple type T "naturally"
    //  * Repeated field matches List<T> iff corresponding non-repeated field matches T and T is not Optional<...>
    //    (List<Optional<Any>> is allowed as an exception)
    //  * Non-repeated field matches Optional<T> iff it matches T and T is not Optional<...>
    //  * StructuredMessage field matches Struct<Name1: Type1, ..., NameN: TypeN> iff
    //      - the field has subfields whose names are in set {Name1, ..., NameN}
    //      - the subfield with name NameK matches TypeK
    //      - if |NonOptionalMissingFieldsAllowed()| is |false|,
    //        for each name NameK missing from subfields TypeK is Optional<...>
    TTypePtr FindOrCreateType(
        const TProtobufTypeConfigPtr& typeConfig,
        std::optional<NTableClient::TComplexTypeFieldDescriptor> maybeDescriptor,
        bool optional,
        bool repeated);

    void VisitStruct(
        const TTypePtr& type,
        const TProtobufTypeConfigPtr& typeConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);

    void VisitDict(
        const TTypePtr& type,
        const TProtobufTypeConfigPtr& typeConfig,
        NTableClient::TComplexTypeFieldDescriptor descriptor);
};

////////////////////////////////////////////////////////////////////////////////

template <typename TType>
class TProtobufFormatDescriptionBase
    : public TRefCounted
{
    static constexpr bool IsWriter = std::is_same_v<TType, TProtobufWriterType>;
    using TTypePtr = NYT::TIntrusivePtr<TType>;

protected:
    void DoInit(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

private:
    THashMap<TString, TEnumerationDescription> EnumerationDescriptionMap_;

private:
    virtual void AddTable(NYT::TIntrusivePtr<TType> tableType) = 0;

    void InitFromFileDescriptorsLegacy(const TProtobufFormatConfigPtr& config);

    void InitFromFileDescriptors(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    void InitEmbeddedColumn(
        int& fieldIndex,
        const NTableClient::TTableSchemaPtr& tableSchema,
        TProtobufTypeBuilder<TType>& typeBuilder,
        TTypePtr tableType,
        TProtobufColumnConfigPtr columnConfig,
        TTypePtr parent,
        int parentParentEmbeddingIndex);

    void InitColumn(
        int& fieldIndex,
        const NTableClient::TTableSchemaPtr& tableSchema,
        TProtobufTypeBuilder<TType>& typeBuilder,
        TTypePtr tableType,
        TProtobufColumnConfigPtr columnConfig,
        TTypePtr parent,
        int parentParentEmbeddingIndex);

    void InitFromProtobufSchema(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);
};

////////////////////////////////////////////////////////////////////////////////

class TProtobufWriterFormatDescription
    : public TProtobufFormatDescriptionBase<TProtobufWriterType>
{
public:
    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    const TProtobufWriterFieldDescription* FindField(
        int tableIndex,
        int fieldIndex,
        const NTableClient::TNameTablePtr& nameTable) const;

    int GetTableCount() const;

    const TProtobufWriterFieldDescription* FindOtherColumnsField(int tableIndex) const;

private:
    void AddTable(TProtobufWriterTypePtr tableType) override;

private:
    struct TTableDescription
    {
        TProtobufWriterTypePtr Type;
        THashMap<TString, const TProtobufWriterFieldDescription*> Columns;
        std::vector<TProtobufWriterEmbeddingDescription> Embeddings;

        // Cached data.
        mutable std::vector<const TProtobufWriterFieldDescription*> FieldIndexToDescription;
        mutable const TProtobufWriterFieldDescription* OtherColumnsField = nullptr;
    };

public:
    const TTableDescription& GetTableDescription(int tableIndex) const;

private:
    std::vector<TTableDescription> Tables_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufWriterFormatDescription)

////////////////////////////////////////////////////////////////////////////////

class TProtobufParserFormatDescription
    : public TProtobufFormatDescriptionBase<TProtobufParserType>
{
public:
    void Init(
        const TProtobufFormatConfigPtr& config,
        const std::vector<NTableClient::TTableSchemaPtr>& schemas);

    const TProtobufParserTypePtr& GetTableType() const;
    std::vector<std::pair<ui16, TProtobufParserFieldDescription*>> CreateRootChildColumnIds(const NTableClient::TNameTablePtr& nameTable) const;

private:
    void AddTable(TProtobufParserTypePtr tableType) override;

private:
    TProtobufParserTypePtr TableType_;
};

DEFINE_REFCOUNTED_TYPE(TProtobufParserFormatDescription)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
