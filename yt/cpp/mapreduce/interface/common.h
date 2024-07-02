#pragma once

///
/// @file yt/cpp/mapreduce/interface/common.h
///
/// Header containing miscellaneous structs and classes used in library.

#include "fwd.h"

#include <library/cpp/type_info/type_info.h>
#include <library/cpp/yson/node/node.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/system/type_name.h>
#include <util/generic/vector.h>

#include <google/protobuf/message.h>

#include <initializer_list>
#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/// @cond Doxygen_Suppress
#define FLUENT_FIELD(type, name) \
    type name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    static_assert(true)

#define FLUENT_FIELD_ENCAPSULATED(type, name) \
private: \
    type name##_; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    const type& name() const & \
    { \
        return name##_; \
    } \
    type name() && \
    { \
        return name##_; \
    } \
    static_assert(true)

#define FLUENT_FIELD_OPTION(type, name) \
    TMaybe<type> name##_; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    static_assert(true)

#define FLUENT_FIELD_OPTION_ENCAPSULATED(type, name) \
private: \
    TMaybe<type> name##_; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf& Reset##name() & \
    { \
        name##_ = Nothing(); \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf Reset##name() && \
    { \
        name##_ = Nothing(); \
        return static_cast<TSelf&>(*this); \
    } \
    const TMaybe<type>& name() const& \
    { \
        return name##_; \
    } \
    TMaybe<type> name() && \
    { \
        return name##_; \
    } \
    static_assert(true)

#define FLUENT_FIELD_DEFAULT(type, name, defaultValue) \
    type name##_ = defaultValue; \
    TSelf& name(const type& value) \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    static_assert(true)

#define FLUENT_FIELD_DEFAULT_ENCAPSULATED(type, name, defaultValue) \
private: \
    type name##_ = defaultValue; \
public: \
    TSelf& name(const type& value) & \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    TSelf name(const type& value) && \
    { \
        name##_ = value; \
        return static_cast<TSelf&>(*this); \
    } \
    const type& name() const & \
    { \
        return name##_; \
    } \
    type name() && \
    { \
        return name##_; \
    } \
    static_assert(true)

#define FLUENT_VECTOR_FIELD(type, name) \
    TVector<type> name##s_; \
    TSelf& Add##name(const type& value) \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf& name##s(TVector<type> values) \
    { \
        name##s_ = std::move(values); \
        return static_cast<TSelf&>(*this);\
    } \
    static_assert(true)

#define FLUENT_OPTIONAL_VECTOR_FIELD_ENCAPSULATED(type, name) \
private: \
    TMaybe<TVector<type>> name##s_; \
public: \
    const TMaybe<TVector<type>>& name##s() const & { \
        return name##s_; \
    } \
    TMaybe<TVector<type>>& name##s() & { \
        return name##s_; \
    } \
    TMaybe<TVector<type>> name##s() && { \
        return std::move(name##s_); \
    } \
    TSelf& Add##name(const type& value) & \
    { \
        if (name##s_.Empty()) { \
            name##s_.ConstructInPlace(); \
        } \
        name##s_->push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf Add##name(const type& value) && \
    { \
        if (name##s_.Empty()) { \
            name##s_.ConstructInPlace(); \
        } \
        name##s_->push_back(value); \
        return static_cast<TSelf&&>(*this);\
    } \
    TSelf& name##s(TVector<type> values) & \
    { \
        name##s_ = std::move(values); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf name##s(TVector<type> values) && \
    { \
        name##s_ = std::move(values); \
        return static_cast<TSelf&&>(*this);\
    } \
    TSelf& name##s(TNothing) & \
    { \
        name##s_ = Nothing(); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf name##s(TNothing) && \
    { \
        name##s_ = Nothing(); \
        return static_cast<TSelf&&>(*this);\
    } \
    TSelf& Reset##name##s() & \
    { \
        name##s_ = Nothing(); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf Reset##name##s() && \
    { \
        name##s_ = Nothing(); \
        return static_cast<TSelf&&>(*this);\
    } \
    static_assert(true)

#define FLUENT_VECTOR_FIELD_ENCAPSULATED(type, name) \
private: \
    TVector<type> name##s_; \
public: \
    TSelf& Add##name(const type& value) & \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf Add##name(const type& value) && \
    { \
        name##s_.push_back(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf& name##s(TVector<type> value) & \
    { \
        name##s_ = std::move(value); \
        return static_cast<TSelf&>(*this);\
    } \
    TSelf name##s(TVector<type> value) && \
    { \
        name##s_ = std::move(value); \
        return static_cast<TSelf&>(*this);\
    } \
    const TVector<type>& name##s() const & \
    { \
        return name##s_; \
    } \
    TVector<type> name##s() && \
    { \
        return name##s_; \
    } \
    static_assert(true)

#define FLUENT_MAP_FIELD(keytype, valuetype, name) \
    TMap<keytype,valuetype> name##_; \
    TSelf& Add##name(const keytype& key, const valuetype& value) \
    { \
        name##_.emplace(key, value); \
        return static_cast<TSelf&>(*this);\
    } \
    static_assert(true)

/// @endcond

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Convenience class that keeps sequence of items.
///
/// Designed to be used as function parameter.
///
/// Users of such function can then pass:
///  - single item,
///  - initializer list of items,
///  - vector of items;
/// as argument to this function.
///
/// Example:
///   ```
///   void Foo(const TOneOrMany<int>& arg);
///   ...
///   Foo(1); // ok
///   Foo({1, 2, 3}); // ok
///   ```
template <class T, class TDerived>
struct TOneOrMany
{
    /// @cond Doxygen_Suppress
    using TSelf = std::conditional_t<std::is_void_v<TDerived>, TOneOrMany, TDerived>;
    /// @endcond

    /// Initialize with empty sequence.
    TOneOrMany() = default;

    // Initialize from initializer list.
    template<class U>
    TOneOrMany(std::initializer_list<U> il)
    {
        Parts_.assign(il.begin(), il.end());
    }

    /// Put arguments to sequence
    template <class U, class... TArgs>
        requires std::is_convertible_v<U, T>
    TOneOrMany(U&& arg, TArgs&&... args)
    {
        Add(arg, std::forward<TArgs>(args)...);
    }

    /// Initialize from vector.
    TOneOrMany(TVector<T> args)
        : Parts_(std::move(args))
    { }

    /// @brief Order is defined the same way as in TVector
    bool operator==(const TOneOrMany& rhs) const
    {
        // N.B. We would like to make this method to be `= default`,
        // but this breaks MSVC compiler for the cases when T doesn't
        // support comparison.
        return Parts_ == rhs.Parts_;
    }

    ///
    /// @{
    ///
    /// @brief Add all arguments to sequence
    template <class U, class... TArgs>
        requires std::is_convertible_v<U, T>
    TSelf& Add(U&& part, TArgs&&... args) &
    {
        Parts_.push_back(std::forward<U>(part));
        if constexpr (sizeof...(args) > 0) {
            [[maybe_unused]] int dummy[sizeof...(args)] = {(Parts_.push_back(std::forward<TArgs>(args)), 0) ... };
        }
        return static_cast<TSelf&>(*this);
    }

    template <class U, class... TArgs>
        requires std::is_convertible_v<U, T>
    TSelf Add(U&& part, TArgs&&... args) &&
    {
        return std::move(Add(std::forward<U>(part), std::forward<TArgs>(args)...));
    }
    /// @}

    /// Content of sequence.
    TVector<T> Parts_;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Type of the value that can occur in YT table.
///
/// @ref NYT::TTableSchema
/// https://ytsaurus.tech/docs/en/user-guide/storage/data-types
enum EValueType : int
{
    /// Int64, signed integer of 64 bits.
    VT_INT64,

    /// Uint64, unsigned integer of 64 bits.
    VT_UINT64,

    /// Double, floating point number of double precision (64 bits).
    VT_DOUBLE,
    /// Boolean, `true` or `false`.
    VT_BOOLEAN,

    /// String, arbitrary byte sequence.
    VT_STRING,

    /// Any, arbitrary yson document.
    VT_ANY,

    /// Int8, signed integer of 8 bits.
    VT_INT8,
    /// Int16, signed integer of 16 bits.
    VT_INT16,
    /// Int32, signed integer of 32 bits.
    VT_INT32,

    /// Uint8, unsigned integer of 8 bits.
    VT_UINT8,
    /// Uint16, unsigned integer of 16 bits.
    VT_UINT16,
    /// Uint32, unsigned integer of 32 bits.
    VT_UINT32,

    /// Utf8, byte sequence that is valid utf8.
    VT_UTF8,

    /// Null, absence of value (almost never used in schemas)
    VT_NULL,
    /// Void, absence of value (almost never used in schemas) the difference between null, and void is yql-specific.
    VT_VOID,

    /// Date, number of days since Unix epoch (unsigned)
    VT_DATE,
    /// Datetime, number of seconds since Unix epoch (unsigned)
    VT_DATETIME,
    /// Timestamp, number of milliseconds since Unix epoch (unsigned)
    VT_TIMESTAMP,
    /// Interval, difference between two timestamps (signed)
    VT_INTERVAL,

    /// Float, floating point number (32 bits)
    VT_FLOAT,
    /// Json, sequence of bytes that is valid json.
    VT_JSON,

    // Date32, number of days shifted from Unix epoch, which is 0 (signed)
    VT_DATE32,
    // Datetime64, number of seconds shifted from Unix epoch, which is 0 (signed)
    VT_DATETIME64,
    // Timestamp64, number of milliseconds shifted from Unix epoch, which is 0 (signed)
    VT_TIMESTAMP64,
    // Interval64, difference between two timestamps64 (signed)
    VT_INTERVAL64,
};

///
/// @brief Sort order.
///
/// @ref NYT::TTableSchema
enum ESortOrder : int
{
    /// Ascending sort order.
    SO_ASCENDING    /* "ascending" */,
    /// Descending sort order.
    SO_DESCENDING   /* "descending" */,
};

///
/// @brief Value of "optimize_for" attribute.
///
/// @ref NYT::TRichYPath
enum EOptimizeForAttr : i8
{
    /// Optimize for scan
    OF_SCAN_ATTR    /* "scan" */,

    /// Optimize for lookup
    OF_LOOKUP_ATTR  /* "lookup" */,
};

///
/// @brief Value of "erasure_codec" attribute.
///
/// @ref NYT::TRichYPath
enum EErasureCodecAttr : i8
{
    /// @cond Doxygen_Suppress
    EC_NONE_ATTR                /* "none" */,
    EC_REED_SOLOMON_6_3_ATTR    /* "reed_solomon_6_3" */,
    EC_LRC_12_2_2_ATTR          /* "lrc_12_2_2" */,
    EC_ISA_LRC_12_2_2_ATTR      /* "isa_lrc_12_2_2" */,
    /// @endcond
};

///
/// @brief Value of "schema_modification" attribute.
///
/// @ref NYT::TRichYPath
enum ESchemaModificationAttr : i8
{
    SM_NONE_ATTR                    /* "none" */,
    SM_UNVERSIONED_UPDATE           /* "unversioned_update" */,
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Table key column description.
///
/// The description includes column name and sort order.
///
/// @anchor TSortOrder_backward_compatibility
/// @note
/// Many functions that use `TSortOrder` as argument used to take `TString`
/// (the only allowed sort order was "ascending" and user didn't have to specify it).
/// @note
/// This class is designed to provide backward compatibility for such code and therefore
/// objects of this class can be constructed and assigned from TString-like objects only.
///
/// @see NYT::TSortOperationSpec
class TSortColumn
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TSortColumn;
    /// @endcond

    /// Column name
    FLUENT_FIELD_ENCAPSULATED(TString, Name);

    /// Sort order
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(ESortOrder, SortOrder, ESortOrder::SO_ASCENDING);

    ///
    /// @{
    ///
    /// @brief Construct object from name and sort order
    ///
    /// Constructors are intentionally implicit so `TSortColumn` can be compatible with old code.
    /// @ref TSortOrder_backward_compatibility
    TSortColumn(TStringBuf name = {}, ESortOrder sortOrder = ESortOrder::SO_ASCENDING);
    TSortColumn(const TString& name, ESortOrder sortOrder = ESortOrder::SO_ASCENDING);
    TSortColumn(const char* name, ESortOrder sortOrder = ESortOrder::SO_ASCENDING);
    /// @}

    /// Check that sort order is ascending, throw exception otherwise.
    const TSortColumn& EnsureAscending() const;

    /// @brief Convert sort to yson representation as YT API expects it.
    TNode ToNode() const;

    /// @brief Comparison is default and checks both name and sort order.
    bool operator == (const TSortColumn& rhs) const = default;

    ///
    /// @{
    ///
    /// @brief Assign object from column name, and set sort order to `ascending`.
    ///
    /// This is backward compatibility methods.
    ///
    /// @ref TSortOrder_backward_compatibility
    TSortColumn& operator = (TStringBuf name);
    TSortColumn& operator = (const TString& name);
    TSortColumn& operator = (const char* name);
    /// @}

    bool operator == (const TStringBuf rhsName) const;
    bool operator == (const TString& rhsName) const;
    bool operator == (const char* rhsName) const;

    // Intentionally implicit conversions.
    operator TString() const;
    operator TStringBuf() const;
    operator std::string() const;

    Y_SAVELOAD_DEFINE(Name_, SortOrder_);
};

///
/// @brief List of @ref TSortColumn
///
/// Contains a bunch of helper methods such as constructing from single object.
class TSortColumns
    : public TOneOrMany<TSortColumn, TSortColumns>
{
public:
    using TOneOrMany<TSortColumn, TSortColumns>::TOneOrMany;

    /// Construct empty list.
    TSortColumns();

    ///
    /// @{
    ///
    /// @brief Construct list of ascending sort order columns by their names.
    ///
    /// Required for backward compatibility.
    ///
    /// @ref TSortOrder_backward_compatibility
    TSortColumns(const TVector<TString>& names);
    TSortColumns(const TColumnNames& names);
    /// @}


    ///
    /// @brief Implicit conversion to column list.
    ///
    /// If all columns has ascending sort order return list of their names.
    /// Throw exception otherwise.
    ///
    /// Required for backward compatibility.
    ///
    /// @ref TSortOrder_backward_compatibility
    operator TColumnNames() const;

    /// Make sure that all columns are of ascending sort order.
    const TSortColumns& EnsureAscending() const;

    /// Get list of column names.
    TVector<TString> GetNames() const;
};

////////////////////////////////////////////////////////////////////////////////

/// Helper function to create new style type from old style one.
NTi::TTypePtr ToTypeV3(EValueType type, bool required);

///
/// @brief Single column description
///
/// Each field describing column has setter and getter.
///
/// Example reading field:
/// ```
///    ... columnSchema.Name() ...
/// ```
///
/// Example setting field:
/// ```
///    columnSchema.Name("my-column").Type(VT_INT64); // set name and type
/// ```
///
/// @ref https://ytsaurus.tech/docs/en/user-guide/storage/static-schema
class TColumnSchema
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TColumnSchema;
    /// @endcond

    ///
    /// @brief Construct empty column schemas
    ///
    /// @note
    /// Such schema cannot be used in schema as it it doesn't have name.
    TColumnSchema();

    ///
    /// @{
    ///
    /// @brief Copy and move constructors are default.
    TColumnSchema(const TColumnSchema&) = default;
    TColumnSchema& operator=(const TColumnSchema&) = default;
    /// @}


    FLUENT_FIELD_ENCAPSULATED(TString, Name);

    ///
    /// @brief Functions to work with type in old manner.
    ///
    /// @deprecated New code is recommended to work with types using @ref NTi::TTypePtr from type_info library.
    TColumnSchema& Type(EValueType type) &;
    TColumnSchema Type(EValueType type) &&;
    EValueType Type() const;

    /// @brief Set and get column type.
    /// @{
    TColumnSchema& Type(const NTi::TTypePtr& type) &;
    TColumnSchema Type(const NTi::TTypePtr& type) &&;

    TColumnSchema& TypeV3(const NTi::TTypePtr& type) &;
    TColumnSchema TypeV3(const NTi::TTypePtr& type) &&;
    NTi::TTypePtr TypeV3() const;
    /// @}

    ///
    /// @brief Raw yson representation of column type
    /// @deprecated Prefer to use `TypeV3` methods.
    FLUENT_FIELD_OPTION_ENCAPSULATED(TNode, RawTypeV3);

    /// Column sort order
    FLUENT_FIELD_OPTION_ENCAPSULATED(ESortOrder, SortOrder);

    ///
    /// @brief Lock group name
    ///
    /// @ref https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/sorted-dynamic-tables#locking-rows
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Lock);

    /// Expression defining column value
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Expression);

    /// Aggregating function name
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Aggregate);

    ///
    /// @brief Storage group name
    ///
    /// @ref https://ytsaurus.tech/docs/en/user-guide/storage/static-schema
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, Group);

    // StableName for renamed and deleted columns.
    FLUENT_FIELD_OPTION_ENCAPSULATED(TString, StableName);

    /// Deleted column
    FLUENT_FIELD_OPTION_ENCAPSULATED(bool, Deleted);

    ///
    /// @brief Column requiredness.
    ///
    /// Required columns doesn't accept NULL values.
    /// Usually if column is required it means that it has Optional<...> type
    bool Required() const;

    ///
    /// @{
    ///
    /// @brief Set type in old-style manner
    TColumnSchema& Type(EValueType type, bool required) &;
    TColumnSchema Type(EValueType type, bool required) &&;
    /// @}

private:
    friend void Deserialize(TColumnSchema& columnSchema, const TNode& node);
    NTi::TTypePtr TypeV3_;
    bool Required_ = false;
};

/// Equality check checks all fields of column schema.
bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs);

///
/// @brief Description of table schema
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/static-schema
class TTableSchema
{
public:
    /// @cond Doxygen_Suppress
    using TSelf = TTableSchema;
    /// @endcond

    /// Column schema
    FLUENT_VECTOR_FIELD_ENCAPSULATED(TColumnSchema, Column);

    ///
    /// @brief Strictness of the schema
    ///
    /// Strict schemas are not allowed to have columns not described in schema.
    /// Nonstrict schemas are allowed to have such columns, all such missing columns are assumed to have
    /// type any (or optional<yson> in type_v3 terminology).
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(bool, Strict, true);

    ///
    /// @brief Whether keys are unique
    ///
    /// This flag can be set only for schemas that have sorted columns.
    /// If flag is set table cannot have multiple rows with same key.
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(bool, UniqueKeys, false);

    /// Get modifiable column list
    TVector<TColumnSchema>& MutableColumns();

    /// Check if schema has any described column
    [[nodiscard]] bool Empty() const;

    /// Add column
    TTableSchema& AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &;
    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, const NTi::TTypePtr&, ESortOrder)&;
    TTableSchema AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &&;

    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, const NTi::TTypePtr&, ESortOrder)&;
    TTableSchema& AddColumn(const TString& name, const NTi::TTypePtr& type) &;
    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, const NTi::TTypePtr&, ESortOrder)&;
    TTableSchema AddColumn(const TString& name, const NTi::TTypePtr& type) &&;

    /// Add optional column of specified type
    TTableSchema& AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &;
    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, EValueType, ESortOrder)&;
    TTableSchema AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &&;

    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, EValueType, ESortOrder)&;
    TTableSchema& AddColumn(const TString& name, EValueType type) &;
    /// @copydoc NYT::TTableSchema::AddColumn(const TString&, EValueType, ESortOrder)&;
    TTableSchema AddColumn(const TString& name, EValueType type) &&;

    ///
    /// @brief Make table schema sorted by specified columns
    ///
    /// Resets old key columns if any
    TTableSchema& SortBy(const TSortColumns& columns) &;

    /// @copydoc NYT::TTableSchema::SortBy(const TSortColumns&)&;
    TTableSchema SortBy(const TSortColumns& columns) &&;

    /// Get yson description of table schema
    [[nodiscard]] TNode ToNode() const;

    /// Parse schema from yson node
    static NYT::TTableSchema FromNode(const TNode& node);

    friend void Deserialize(TTableSchema& tableSchema, const TNode& node);
};

/// Check for equality of all columns and all schema attributes
bool operator==(const TTableSchema& lhs, const TTableSchema& rhs);

// Pretty printer for unittests
void PrintTo(const TTableSchema& schema, std::ostream* out);

/// Create table schema by protobuf message descriptor
TTableSchema CreateTableSchema(
    const ::google::protobuf::Descriptor& messageDescriptor,
    const TSortColumns& sortColumns = TSortColumns(),
    bool keepFieldsWithoutExtension = true);

/// Create table schema by protobuf message type
template <class TProtoType, typename = std::enable_if_t<std::is_base_of_v<::google::protobuf::Message, TProtoType>>>
inline TTableSchema CreateTableSchema(
    const TSortColumns& sortColumns = TSortColumns(),
    bool keepFieldsWithoutExtension = true)
{
    static_assert(
        std::is_base_of_v<::google::protobuf::Message, TProtoType>,
        "Template argument must be derived from ::google::protobuf::Message");

    return CreateTableSchema(
        *TProtoType::descriptor(),
        sortColumns,
        keepFieldsWithoutExtension);
}

///
/// @brief Create strict table schema from `struct` type.
///
/// Names and types of columns are taken from struct member names and types.
/// `Strict` flag is set to true, all other attribute of schema and columns
/// are left with default values
TTableSchema CreateTableSchema(NTi::TTypePtr type);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Enumeration describing comparison operation used in key bound.
///
/// ERelation is a part of @ref NYT::TKeyBound that can be used as
/// lower or upper key limit in @ref TReadLimit.
///
/// Relations `Less` and `LessOrEqual` are for upper limit and
/// relations `Greater` and `GreaterOrEqual` are for lower limit.
///
/// It is a error to use relation in the limit of wrong kind.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/ypath#rich_ypath
enum class ERelation
{
    ///
    /// @brief Relation "less"
    ///
    /// Specifies range of keys that are before specified key.
    /// Can only be used in upper limit.
    Less            /* "<"  */,

    ///
    /// @brief Relation "less or equal"
    ///
    /// Specifies range of keys that are before or equal specified key.
    /// Can only be used in upper limit.
    LessOrEqual     /* "<=" */,

    ///
    /// @brief Relation "greater"
    ///
    /// Specifies range of keys that are after specified key.
    /// Can only be used in lower limit.
    Greater         /* ">"  */,

    ///
    /// @brief Relation "greater or equal"
    ///
    /// Specifies range of keys that are after or equal than specified key.
    /// Can only be used in lower limit.
    GreaterOrEqual  /* ">=" */,
};

///
/// @brief Key with relation specifying interval of keys in lower or upper limit of @ref NYT::TReadRange
///
/// @see https://ytsaurus.tech/docs/en/user-guide/common/ypath#rich_ypath
struct TKeyBound
{
    /// @cond Doxygen_Suppress
    using TSelf = TKeyBound;

    explicit TKeyBound(ERelation relation = ERelation::Less, TKey key = TKey{});

    FLUENT_FIELD_DEFAULT_ENCAPSULATED(ERelation, Relation, ERelation::Less);
    FLUENT_FIELD_DEFAULT_ENCAPSULATED(TKey, Key, TKey{});
    /// @endcond
};

///
/// @brief Description of the read limit.
///
/// It is actually a variant and must store exactly one field.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/common/ypath#rich_ypath
struct TReadLimit
{
    /// @cond Doxygen_Suppress
    using TSelf = TReadLimit;
    /// @endcond

    ///
    /// @brief KeyBound specifies table key and whether to include it
    ///
    /// It can be used in lower or upper limit when reading tables.
    FLUENT_FIELD_OPTION(TKeyBound, KeyBound);

    ///
    /// @brief Table key
    ///
    /// It can be used in exact, lower or upper limit when reading tables.
    FLUENT_FIELD_OPTION(TKey, Key);

    ///
    /// @brief Row index
    ///
    /// It can be used in exact, lower or upper limit when reading tables.
    FLUENT_FIELD_OPTION(i64, RowIndex);

    ///
    /// @brief File offset
    ///
    /// It can be used in lower or upper limit when reading files.
    FLUENT_FIELD_OPTION(i64, Offset);

    ///
    /// @brief Tablet index
    ///
    /// It can be used in lower or upper limit in dynamic table operations
    FLUENT_FIELD_OPTION(i64, TabletIndex);
};

///
/// @brief Range of a table or a file
///
/// @see https://ytsaurus.tech/docs/en/user-guide/common/ypath#rich_ypath
struct TReadRange
{
    using TSelf = TReadRange;

    ///
    /// @brief Lower limit of the range
    ///
    /// It is usually inclusive (except when @ref NYT::TKeyBound with relation @ref NYT::ERelation::Greater is used).
    FLUENT_FIELD(TReadLimit, LowerLimit);

    ///
    /// @brief Lower limit of the range
    ///
    /// It is usually exclusive (except when @ref NYT::TKeyBound with relation @ref NYT::ERelation::LessOrEqual is used).
    FLUENT_FIELD(TReadLimit, UpperLimit);

    /// Exact key or row index.
    FLUENT_FIELD(TReadLimit, Exact);

    /// Create read range from row indexes.
    static TReadRange FromRowIndices(i64 lowerLimit, i64 upperLimit)
    {
        return TReadRange()
            .LowerLimit(TReadLimit().RowIndex(lowerLimit))
            .UpperLimit(TReadLimit().RowIndex(upperLimit));
    }

    /// Create read range from keys.
    static TReadRange FromKeys(const TKey& lowerKeyInclusive, const TKey& upperKeyExclusive)
    {
        return TReadRange()
            .LowerLimit(TReadLimit().Key(lowerKeyInclusive))
            .UpperLimit(TReadLimit().Key(upperKeyExclusive));
    }
};

///
/// @brief Path with additional attributes.
///
/// Allows to specify additional attributes for path used in some operations.
///
/// @see https://ytsaurus.tech/docs/en/user-guide/storage/ypath#rich_ypath
struct TRichYPath
{
    /// @cond Doxygen_Suppress
    using TSelf = TRichYPath;
    /// @endcond

    /// Path itself.
    FLUENT_FIELD(TYPath, Path);

    /// Specifies that path should be appended not overwritten
    FLUENT_FIELD_OPTION(bool, Append);

    /// @deprecated Deprecated attribute.
    FLUENT_FIELD_OPTION(bool, PartiallySorted);

    /// Specifies that path is expected to be sorted by these columns.
    FLUENT_FIELD(TSortColumns, SortedBy);

    /// Add range to read.
    TRichYPath& AddRange(TReadRange range)
    {
        if (!Ranges_) {
            Ranges_.ConstructInPlace();
        }
        Ranges_->push_back(std::move(range));
        return *this;
    }

    TRichYPath& ResetRanges()
    {
        Ranges_.Clear();
        return *this;
    }

    ///
    /// @{
    ///
    /// Return ranges to read.
    ///
    /// NOTE: Nothing (in TMaybe) and empty TVector are different ranges.
    /// Nothing represents universal range (reader reads all table rows).
    /// Empty TVector represents empty  range (reader returns empty set of rows).
    const TMaybe<TVector<TReadRange>>& GetRanges() const
    {
        return Ranges_;
    }

    TMaybe<TVector<TReadRange>>& MutableRanges()
    {
        return Ranges_;
    }

    ///
    /// @{
    ///
    /// Get range view, that is convenient way to iterate through all ranges.
    TArrayRef<TReadRange> MutableRangesView()
    {
        if (Ranges_.Defined()) {
            return TArrayRef(Ranges_->data(), Ranges_->size());
        } else {
            return {};
        }
    }

    TArrayRef<const TReadRange> GetRangesView() const
    {
        if (Ranges_.Defined()) {
            return TArrayRef(Ranges_->data(), Ranges_->size());
        } else {
            return {};
        }
    }
    /// @}

    /// @{
    ///
    /// Get range by index.
    const TReadRange& GetRange(ssize_t i) const
    {
        return Ranges_.GetRef()[i];
    }

    TReadRange& MutableRange(ssize_t i)
    {
        return Ranges_.GetRef()[i];
    }
    /// @}

    ///
    /// @brief Specifies columns that should be read.
    ///
    /// If it's set to Nothing then all columns will be read.
    /// If empty TColumnNames is specified then each read row will be empty.
    FLUENT_FIELD_OPTION(TColumnNames, Columns);

    FLUENT_FIELD_OPTION(bool, Teleport);
    FLUENT_FIELD_OPTION(bool, Primary);
    FLUENT_FIELD_OPTION(bool, Foreign);
    FLUENT_FIELD_OPTION(i64, RowCountLimit);

    FLUENT_FIELD_OPTION(TString, FileName);

    /// Specifies original path to be shown in Web UI
    FLUENT_FIELD_OPTION(TYPath, OriginalPath);

    ///
    /// @brief Specifies that this path points to executable file
    ///
    /// Used in operation specs.
    FLUENT_FIELD_OPTION(bool, Executable);

    ///
    /// @brief Specify format to use when loading table.
    ///
    /// Used in operation specs.
    FLUENT_FIELD_OPTION(TNode, Format);

    /// @brief Specifies table schema that will be set on the path
    FLUENT_FIELD_OPTION(TTableSchema, Schema);

    /// Specifies compression codec that will be set on the path
    FLUENT_FIELD_OPTION(TString, CompressionCodec);

    /// Specifies erasure codec that will be set on the path
    FLUENT_FIELD_OPTION(EErasureCodecAttr, ErasureCodec);

    /// Specifies schema modification that will be set on the path
    FLUENT_FIELD_OPTION(ESchemaModificationAttr, SchemaModification);

    /// Specifies optimize_for attribute that will be set on the path
    FLUENT_FIELD_OPTION(EOptimizeForAttr, OptimizeFor);

    ///
    /// @brief Do not put file used in operation into node cache
    ///
    /// If BypassArtifactCache == true, file will be loaded into the job's sandbox bypassing the cache on the YT node.
    /// It helps jobs that use tmpfs to start faster,
    /// because files will be loaded into tmpfs directly bypassing disk cache
    FLUENT_FIELD_OPTION(bool, BypassArtifactCache);

    ///
    /// @brief Timestamp of dynamic table.
    ///
    /// NOTE: it is _not_ unix timestamp
    /// (instead it's transaction timestamp, that is more complex structure).
    FLUENT_FIELD_OPTION(i64, Timestamp);

    ///
    /// @brief Specify transaction that should be used to access this path.
    ///
    /// Allows to start cross-transactional operations.
    FLUENT_FIELD_OPTION(TTransactionId, TransactionId);

    using TRenameColumnsDescriptor = THashMap<TString, TString>;

    /// Specifies columnar mapping which will be applied to columns before transfer to job.
    FLUENT_FIELD_OPTION(TRenameColumnsDescriptor, RenameColumns);

    /// Create empty path with no attributes
    TRichYPath()
    { }

    ///
    /// @{
    ///
    /// @brief Create path from string
    TRichYPath(const char* path)
        : Path_(path)
    { }

    TRichYPath(const TYPath& path)
        : Path_(path)
    { }
    /// @}

private:
    TMaybe<TVector<TReadRange>> Ranges_;
};

///
/// @ref Create copy of @ref NYT::TRichYPath with schema derived from proto message.
///
///
template <typename TProtoType>
TRichYPath WithSchema(const TRichYPath& path, const TSortColumns& sortBy = TSortColumns())
{
    static_assert(std::is_base_of_v<::google::protobuf::Message, TProtoType>, "TProtoType must be Protobuf message");

    auto schemedPath = path;
    if (!schemedPath.Schema_) {
        schemedPath.Schema(CreateTableSchema<TProtoType>(sortBy));
    }
    return schemedPath;
}

///
/// @brief Create copy of @ref NYT::TRichYPath with schema derived from TRowType if possible.
///
/// If TRowType is protobuf message schema is derived from it and set to returned path.
/// Otherwise schema of original path is left unchanged (and probably unset).
template <typename TRowType>
TRichYPath MaybeWithSchema(const TRichYPath& path, const TSortColumns& sortBy = TSortColumns())
{
    if constexpr (std::is_base_of_v<::google::protobuf::Message, TRowType>) {
        return WithSchema<TRowType>(path, sortBy);
    } else {
        return path;
    }
}

///
/// @brief Get the list of ranges related to path in compatibility mode.
///
///  - If path is missing ranges, empty list is returned.
///  - If path has associated range list and the list is not empty, function returns this list.
///  - If path has associated range list and this list is empty, exception is thrown.
///
/// Before YT-17683 RichYPath didn't support empty range list and empty range actually meant universal range.
/// This function emulates this old behavior.
///
/// @see https://st.yandex-team.ru/YT-17683
const TVector<TReadRange>& GetRangesCompat(const TRichYPath& path);

////////////////////////////////////////////////////////////////////////////////

/// Statistics about table columns.
struct TTableColumnarStatistics
{
    /// Total data weight for all chunks for each of requested columns.
    THashMap<TString, i64> ColumnDataWeight;

    /// Estimated number of unique elements for each column.
    THashMap<TString, ui64> ColumnEstimatedUniqueCounts;

    /// Total weight of all old chunks that don't keep columnar statistics.
    i64 LegacyChunksDataWeight = 0;

    /// Timestamps total weight (only for dynamic tables).
    TMaybe<i64> TimestampTotalWeight;
};

////////////////////////////////////////////////////////////////////////////////

/// Description of a partition.
struct TMultiTablePartition
{
    struct TStatistics
    {
        i64 ChunkCount = 0;
        i64 DataWeight = 0;
        i64 RowCount = 0;
    };

    /// Ranges of input tables for this partition.
    TVector<TRichYPath> TableRanges;

    /// Aggregate statistics of all the table ranges in the partition.
    TStatistics AggregateStatistics;
};

/// Table partitions from GetTablePartitions command.
struct TMultiTablePartitions
{
    /// Disjoint partitions into which the input tables were divided.
    TVector<TMultiTablePartition> Partitions;
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Contains information about tablet
///
/// @see NYT::IClient::GetTabletInfos
struct TTabletInfo
{
    ///
    /// @brief Indicates the total number of rows added to the tablet (including trimmed ones).
    ///
    /// Currently only provided for ordered tablets.
    i64 TotalRowCount = 0;

    ///
    /// @brief Contains the number of front rows that are trimmed and are not guaranteed to be accessible.
    ///
    /// Only makes sense for ordered tablet.
    i64 TrimmedRowCount = 0;

    ///
    /// @brief Tablet cell barrier timestamp, which lags behind the current timestamp
    ///
    /// It is guaranteed that all transactions with commit timestamp not exceeding the barrier are fully committed;
    /// e.g. all their added rows are visible (and are included in @ref NYT::TTabletInfo::TotalRowCount).
    /// Mostly makes sense for ordered tablets.
    ui64 BarrierTimestamp;
};

////////////////////////////////////////////////////////////////////////////////

/// List of attributes to retrieve in operations like @ref NYT::ICypressClient::Get
struct TAttributeFilter
{
    /// @cond Doxygen_Suppress
    using TSelf = TAttributeFilter;
    /// @endcond

    /// List of attributes.
    FLUENT_VECTOR_FIELD(TString, Attribute);
};

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Check if none of the fields of @ref NYT::TReadLimit is set.
///
/// @return true if any field of readLimit is set and false otherwise.
bool IsTrivial(const TReadLimit& readLimit);

/// Convert yson node type to table schema type
EValueType NodeTypeToValueType(TNode::EType nodeType);

////////////////////////////////////////////////////////////////////////////////

///
/// @brief Enumeration for specifying how reading from master is performed.
///
/// Used in operations like NYT::ICypressClient::Get
enum class EMasterReadKind : int
{
    ///
    /// @brief Reading from leader.
    ///
    /// Should almost never be used since it's expensive and for regular uses has no difference from
    /// "follower" read.
    Leader /* "leader" */,

    /// @brief Reading from master follower (default).
    Follower /* "follower" */,
    Cache /* "cache" */,
    MasterCache /* "master_cache" */,
};

////////////////////////////////////////////////////////////////////////////////

/// @cond Doxygen_Suppress
namespace NDetail {

// MUST NOT BE USED BY CLIENTS
// TODO: we should use default GENERATE_ENUM_SERIALIZATION
TString ToString(EValueType type);

} // namespace NDetail
/// @endcond

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
