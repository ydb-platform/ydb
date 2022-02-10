#pragma once

#include "udf_ptr.h"
#include "udf_types.h"
#include "udf_type_builder.h"
#include "udf_string.h"
#include "udf_value.h"

#include <array>

namespace NYql {
namespace NUdf {

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
 
#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 24) 
class IDateBuilder: public IDateBuilder3 {}; 
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 7) 
class IDateBuilder: public IDateBuilder2 {};
#else
class IDateBuilder: public IDateBuilder1 {};
#endif

UDF_ASSERT_TYPE_SIZE(IDateBuilder, 8);

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
    virtual TUnboxedValue NewArray(ui32 count, TUnboxedValue*& itemsPtr) const = 0;

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
    virtual TFlatDataBlockPtr NewFlatDataBlock(ui32 initialSize, ui32 initialCapacity) const = 0;
    virtual TFlatArrayBlockPtr NewFlatArrayBlock(ui32 count) const = 0;
    TBlockPtr NewEmptyBlock() const {
        return NewFlatArrayBlock(0).Get();
    }

    virtual TSingleBlockPtr NewSingleBlock(const TUnboxedValue& value) const = 0;
};
#endif

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 19)
class IValueBuilder: public IValueBuilder5 {};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 17)
class IValueBuilder: public IValueBuilder4 {};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 14)
class IValueBuilder: public IValueBuilder3 {};
#elif UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 1)
class IValueBuilder: public IValueBuilder2 {};
#else
class IValueBuilder: public IValueBuilder1 {};
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
