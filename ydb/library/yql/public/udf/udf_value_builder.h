#pragma once

#include "udf_ptr.h"
#include "udf_types.h"
#include "udf_type_builder.h"
#include "udf_string.h"
#include "udf_type_size_check.h"
#include "udf_value.h"

#include <array>

struct ArrowArray;

namespace NYql {
namespace NUdf {

class IArrowType;

///////////////////////////////////////////////////////////////////////////////
// IDictValueBuilder
///////////////////////////////////////////////////////////////////////////////
struct TDictFlags {
    enum EDictKind: ui32 {
        Sorted = 0x01,
        Hashed = 0x02,
        Multi = 0x04,
    };
};

class IDictValueBuilder
{
public:
    using TPtr = TUniquePtr<IDictValueBuilder>;

public:
    virtual ~IDictValueBuilder() = default;

    virtual IDictValueBuilder& Add(TUnboxedValue&& key, TUnboxedValue&& value) = 0;

    virtual TUnboxedValue Build() = 0;
};

UDF_ASSERT_TYPE_SIZE(IDictValueBuilder, 8);

///////////////////////////////////////////////////////////////////////////////
// IDateBuilder
///////////////////////////////////////////////////////////////////////////////
class IDateBuilder1
{
public:
    virtual ~IDateBuilder1() = default;

    virtual bool MakeDate(ui32 year, ui32 month, ui32 day, ui16& value) const = 0;
    virtual bool SplitDate(ui16 value, ui32& year, ui32& month, ui32& day) const = 0;

    virtual bool MakeDatetime(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second, ui32& value,
        ui16 timezoneId = 0) const = 0;
    virtual bool SplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
        ui16 timezoneId = 0) const = 0;

    // deprecated
    virtual bool EnrichDate(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek) const = 0;

    // in minutes
    virtual bool GetTimezoneShift(ui32 year, ui32 month, ui32 day, ui32 hour, ui32 minute, ui32 second,
        ui16 timezoneId, i32& value) const = 0;

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT < UDF_ABI_COMPATIBILITY_VERSION(2, 23)
    virtual void Unused7() const = 0;
    virtual void Unused8() const = 0;
#else
    virtual bool FullSplitDate(ui16 value, ui32& year, ui32& month, ui32& day,
        ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
    virtual bool FullSplitDatetime(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
        ui32& dayOfYear, ui32& weekOfYear, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
#endif
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 7)
class IDateBuilder2: public IDateBuilder1
{
public:
    virtual bool FindTimezoneName(ui32 id, TStringRef& name) const = 0;
    virtual bool FindTimezoneId(const TStringRef& name, ui32& id) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 24)
class IDateBuilder3: public IDateBuilder2
{
public:
    virtual bool EnrichDate2(ui16 date, ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek) const = 0;
    virtual bool FullSplitDate2(ui16 value, ui32& year, ui32& month, ui32& day,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
    virtual bool FullSplitDatetime2(ui32 value, ui32& year, ui32& month, ui32& day, ui32& hour, ui32& minute, ui32& second,
        ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
class IDateBuilder4: public IDateBuilder3
{
public:
    virtual bool SplitTzDate32(i32 date, i32& year, ui32& month, ui32& day,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
    virtual bool SplitTzDatetime64(i64 datetime, i32& year, ui32& month, ui32& day,
            ui32& hour, ui32& minute, ui32& second,
            ui32& dayOfYear, ui32& weekOfYear, ui32& weekOfYearIso8601, ui32& dayOfWeek, ui16 timezoneId = 0) const = 0;
    virtual bool MakeTzDate32(i32 year, ui32 month, ui32 day, i32& date, ui16 timezoneId = 0) const = 0;
    virtual bool MakeTzDatetime64(i32 year, ui32 month, ui32 day,
            ui32 hour, ui32 minute, ui32 second, i64& datetime, ui16 timezoneId = 0) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 37)
class IDateBuilder: public IDateBuilder4 {
protected:
    IDateBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 24)
class IDateBuilder: public IDateBuilder3 {
protected:
    IDateBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 7)
class IDateBuilder: public IDateBuilder2 {
protected:
    IDateBuilder();
};
#else
class IDateBuilder: public IDateBuilder1 {
protected:
    IDateBuilder();
};
#endif

UDF_ASSERT_TYPE_SIZE(IDateBuilder, 8);

///////////////////////////////////////////////////////////////////////////////
// IPgBuilder
///////////////////////////////////////////////////////////////////////////////
class IPgBuilder1 {
public:
    virtual ~IPgBuilder1() = default;
    // returns Null in case of text format parsing error, error message passed via 'error' arg
    virtual TUnboxedValue ValueFromText(ui32 typeId, const TStringRef& value, TStringValue& error) const = 0;

    // returns Null in case of wire format parsing error, error message passed via 'error' arg
    virtual TUnboxedValue ValueFromBinary(ui32 typeId, const TStringRef& value, TStringValue& error) const = 0;

    // targetType is required for diagnostic only in debug mode
    virtual TUnboxedValue ConvertFromPg(TUnboxedValue source, ui32 sourceTypeId, const TType* targetType) const = 0;

    // sourceType is required for diagnostic only in debug mode
    virtual TUnboxedValue ConvertToPg(TUnboxedValue source, const TType* sourceType, ui32 targetTypeId) const = 0;

    // targetTypeId is required for diagnostic only in debug mode
    virtual TUnboxedValue NewString(i32 typeLen, ui32 targetTypeId, TStringRef data) const = 0;
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 31)
class IPgBuilder2: public IPgBuilder1
{
public:
    virtual TStringRef AsCStringBuffer(const TUnboxedValue& value) const = 0;
    virtual TStringRef AsTextBuffer(const TUnboxedValue& value) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 33)
class IPgBuilder3: public IPgBuilder2
{
public:
    virtual TUnboxedValue MakeCString(const char* value) const = 0;
    virtual TUnboxedValue MakeText(const char* value) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 35)
class IPgBuilder4: public IPgBuilder3
{
public:
    virtual TStringRef AsFixedStringBuffer(const TUnboxedValue& value, ui32 length) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 35)
class IPgBuilder: public IPgBuilder4 {
protected:
    IPgBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 33)
class IPgBuilder: public IPgBuilder3 {
protected:
    IPgBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 31)
class IPgBuilder: public IPgBuilder2 {
protected:
    IPgBuilder();
};
#else
class IPgBuilder: public IPgBuilder1 {
protected:
    IPgBuilder();
};
#endif

UDF_ASSERT_TYPE_SIZE(IPgBuilder, 8);

///////////////////////////////////////////////////////////////////////////////
// IValueBuilder
///////////////////////////////////////////////////////////////////////////////
class IValueBuilder1
{
public:
    virtual ~IValueBuilder1() = default;

    virtual TUnboxedValue NewStringNotFilled(ui32 size) const = 0;
    virtual TUnboxedValue NewString(const TStringRef& ref) const = 0;

    virtual TUnboxedValue ConcatStrings(TUnboxedValuePod first, TUnboxedValuePod second) const = 0;

    virtual TUnboxedValue AppendString(TUnboxedValuePod value, const TStringRef& ref) const = 0;
    virtual TUnboxedValue PrependString(const TStringRef& ref, TUnboxedValuePod value) const = 0;

    virtual TUnboxedValue SubString(TUnboxedValuePod value, ui32 offset, ui32 size) const = 0;

    virtual IDictValueBuilder::TPtr NewDict(const TType* dictType, ui32 flags) const = 0;

    virtual TUnboxedValue NewList(TUnboxedValue* items, ui64 count) const = 0;

    virtual TUnboxedValue ReverseList(const TUnboxedValuePod& list) const = 0;
    virtual TUnboxedValue SkipList(const TUnboxedValuePod& list, ui64 count) const = 0;
    virtual TUnboxedValue TakeList(const TUnboxedValuePod& list, ui64 count) const = 0;
    virtual TUnboxedValue ToIndexDict(const TUnboxedValuePod& list) const = 0;

    /// Default representation for Tuple, Struct or List with a known size.
    TUnboxedValue NewArray(ui32 count, TUnboxedValue*& itemsPtr) const {
        return NewArray32(count, itemsPtr);
    }

    virtual TUnboxedValue NewArray32(ui32 count, TUnboxedValue*& itemsPtr) const = 0;

    virtual TUnboxedValue NewVariant(ui32 index, TUnboxedValue&& value) const = 0;

    inline TUnboxedValue NewEmptyList() const { return NewList(nullptr, 0); }
};

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 1)
class IValueBuilder2: public IValueBuilder1 {
public:
    virtual const IDateBuilder& GetDateBuilder() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 14)
class IValueBuilder3: public IValueBuilder2 {
public:
    virtual bool GetSecureParam(TStringRef key, TStringRef& value) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 17)
class IValueBuilder4: public IValueBuilder3 {
public:
    virtual const TSourcePosition* CalleePosition() const = 0;

    TSourcePosition WithCalleePosition(const TSourcePosition& def) const {
        auto callee = CalleePosition();
        return callee ? *callee : def;
    }

    virtual TUnboxedValue Run(const TSourcePosition& callee, const IBoxedValue& value, const TUnboxedValuePod* args) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IValueBuilder5: public IValueBuilder4 {
public:
    // exports one Array or Scalar to out. should be called for each chunk of ChunkedArray
    // returns array with one element for scalars
    virtual void ExportArrowBlock(TUnboxedValuePod value, ui32 chunk, ArrowArray* out) const = 0;
    // imports all chunks. returns Scalar, ChunkedArray if chunkCount > 1, otherwise Array
    // arrays should be a pointer to array of chunkCount structs
    // the ArrowArray struct has its contents moved to a private object held alive by the result.
    virtual TUnboxedValue ImportArrowBlock(ArrowArray* arrays, ui32 chunkCount, bool isScalar, const IArrowType& type) const = 0;
    // always returns 1 for Scalar and Array, >= 1 for ChunkedArray
    virtual ui32 GetArrowBlockChunks(TUnboxedValuePod value, bool& isScalar, ui64& length) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class IValueBuilder6: public IValueBuilder5 {
public:
    virtual const IPgBuilder& GetPgBuilder() const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 27)
class IValueBuilder7: public IValueBuilder6 {
public:
    virtual TUnboxedValue NewArray64(ui64 count, TUnboxedValue*& itemsPtr) const = 0;

    TUnboxedValue NewArray(ui64 count, TUnboxedValue*& itemsPtr) const {
        return NewArray64(count, itemsPtr);
    }
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 27)
class IValueBuilder: public IValueBuilder7 {
protected:    
    IValueBuilder();
};

#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 25)
class IValueBuilder: public IValueBuilder6 {
protected:
    IValueBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IValueBuilder: public IValueBuilder5 {
protected:
    IValueBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 17)
class IValueBuilder: public IValueBuilder4 {
protected:
    IValueBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 14)
class IValueBuilder: public IValueBuilder3 {
protected:
    IValueBuilder();
};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 1)
class IValueBuilder: public IValueBuilder2 {
protected:
    IValueBuilder();
};
#else
class IValueBuilder: public IValueBuilder1 {
protected:    
    IValueBuilder();
};
#endif

UDF_ASSERT_TYPE_SIZE(IValueBuilder, 8);

class TPlainArrayCache {
private:
    const ui32 Size;
    std::array<TUnboxedValue, 2U> Cached;
    std::array<TUnboxedValue*, 2U> CachedItems;
    ui8 CacheIndex = 0U;
public:
    TPlainArrayCache(ui32 size): Size(size) { Clear(); }

    TPlainArrayCache(TPlainArrayCache&&) = delete;
    TPlainArrayCache(const TPlainArrayCache&) = delete;
    TPlainArrayCache& operator=(TPlainArrayCache&&) = delete;
    TPlainArrayCache& operator=(const TPlainArrayCache&) = delete;

    void Clear() {
        Cached.fill(TUnboxedValue());
        CachedItems.fill(nullptr);
    }

    TUnboxedValue NewArray(const IValueBuilder& builder, TUnboxedValue*& items) {
        if (!CachedItems[CacheIndex] || !Cached[CacheIndex].UniqueBoxed()) {
            CacheIndex ^= 1U;
            if (!CachedItems[CacheIndex] || !Cached[CacheIndex].UniqueBoxed()) {
                Cached[CacheIndex] = builder.NewArray(Size, CachedItems[CacheIndex]);
                items = CachedItems[CacheIndex];
                return Cached[CacheIndex];
            }
        }

        items = CachedItems[CacheIndex];
        std::fill_n(items, Size, TUnboxedValue());
        return Cached[CacheIndex];
    }
};

} // namespace NUdf
} // namespace NYql
