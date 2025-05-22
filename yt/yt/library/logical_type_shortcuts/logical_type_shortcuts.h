#include <yt/yt/client/table_client/logical_type.h>

/**
 * This file contains a bunch of shortcut functions for creating logical types.
 * They are useful in places where we have to create a lot of logical types e.g. in tests.
 */

namespace NYT::NTableClient::NLogicalTypeShortcuts {

////////////////////////////////////////////////////////////////////////////////

// Simple types:
//   TLogicalTypePtr Int8();
// and friends.

#define CREATE_SIMPLE_TYPE_FUNCTION(name) \
        inline TLogicalTypePtr name() \
        { \
            return SimpleLogicalType(ESimpleLogicalValueType::name); \
        }

CREATE_SIMPLE_TYPE_FUNCTION(Int8)
CREATE_SIMPLE_TYPE_FUNCTION(Int16)
CREATE_SIMPLE_TYPE_FUNCTION(Int32)
CREATE_SIMPLE_TYPE_FUNCTION(Int64)

CREATE_SIMPLE_TYPE_FUNCTION(Uint8)
CREATE_SIMPLE_TYPE_FUNCTION(Uint16)
CREATE_SIMPLE_TYPE_FUNCTION(Uint32)
CREATE_SIMPLE_TYPE_FUNCTION(Uint64)

CREATE_SIMPLE_TYPE_FUNCTION(Float)
CREATE_SIMPLE_TYPE_FUNCTION(Double)

CREATE_SIMPLE_TYPE_FUNCTION(Utf8)
CREATE_SIMPLE_TYPE_FUNCTION(String)

CREATE_SIMPLE_TYPE_FUNCTION(Date)
CREATE_SIMPLE_TYPE_FUNCTION(Datetime)
CREATE_SIMPLE_TYPE_FUNCTION(Timestamp)
CREATE_SIMPLE_TYPE_FUNCTION(Interval)

CREATE_SIMPLE_TYPE_FUNCTION(Json)

CREATE_SIMPLE_TYPE_FUNCTION(Null)
CREATE_SIMPLE_TYPE_FUNCTION(Void)
CREATE_SIMPLE_TYPE_FUNCTION(Uuid)

CREATE_SIMPLE_TYPE_FUNCTION(Date32)
CREATE_SIMPLE_TYPE_FUNCTION(Datetime64)
CREATE_SIMPLE_TYPE_FUNCTION(Timestamp64)
CREATE_SIMPLE_TYPE_FUNCTION(Interval64)

#undef CREATE_SIMPLE_TYPE_FUNCTION

////////////////////////////////////////////////////////////////////////////////

inline TLogicalTypePtr Yson()
{
    return SimpleLogicalType(ESimpleLogicalValueType::Any);
}

inline TLogicalTypePtr Bool()
{
    return SimpleLogicalType(ESimpleLogicalValueType::Boolean);
}

inline TLogicalTypePtr Optional(const TLogicalTypePtr& element)
{
    return OptionalLogicalType(element);
}

inline TLogicalTypePtr Decimal(int precision, int scale)
{
    return DecimalLogicalType(precision, scale);
}

inline TLogicalTypePtr List(const TLogicalTypePtr& element)
{
    return ListLogicalType(element);
}

template <typename... T>
inline TLogicalTypePtr Tuple(const T&... args)
{
    return TupleLogicalType(std::vector<TLogicalTypePtr>{args...});
}

namespace NPrivate {
inline void StructFieldList(std::vector<TStructField>* /*fields*/)
{ }

template <typename... T>
inline void StructFieldList(
    std::vector<TStructField>* fields,
    const TString& name,
    const TLogicalTypePtr& type,
    const T&... args)
{
    fields->push_back({name, type});
    StructFieldList(fields, args...);
}

} // namespace NPrivate

template <typename... T>
inline TLogicalTypePtr Struct(const T&... args)
{
    std::vector<TStructField> fields;
    NPrivate::StructFieldList(&fields, args...);
    return StructLogicalType(fields);
}

template <typename... T>
inline TLogicalTypePtr VariantTuple(const T&... args)
{
    return VariantTupleLogicalType(std::vector<TLogicalTypePtr>{args...});
}

template <typename... T>
inline TLogicalTypePtr VariantStruct(const T&... args)
{
    std::vector<TStructField> fields;
    NPrivate::StructFieldList(&fields, args...);
    return VariantStructLogicalType(fields);
}

inline TLogicalTypePtr Dict(const TLogicalTypePtr& key, const TLogicalTypePtr& value)
{
    return DictLogicalType(key, value);
}

inline TLogicalTypePtr Tagged(TString tag, const TLogicalTypePtr& element)
{
    return TaggedLogicalType(std::move(tag), element);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient::NLogicalTypeShortcuts
