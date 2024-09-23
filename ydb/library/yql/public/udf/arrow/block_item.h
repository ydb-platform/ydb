#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_string_ref.h>
#include <ydb/library/yql/public/udf/udf_type_size_check.h>

namespace NYql::NUdf {

class TBlockItem {
    using EMarkers = TUnboxedValuePod::EMarkers;

public:
    TBlockItem() noexcept = default;
    ~TBlockItem() noexcept = default;

    TBlockItem(const TBlockItem& value) noexcept = default;
    TBlockItem(TBlockItem&& value) noexcept = default;

    TBlockItem& operator=(const TBlockItem& value) noexcept = default;
    TBlockItem& operator=(TBlockItem&& value) noexcept = default;

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline explicit TBlockItem(T value);
    
    inline explicit TBlockItem(IBoxedValuePtr&& value) {
        Raw.Resource.Meta = static_cast<ui8>(EMarkers::Boxed);
        Raw.Resource.Value = value.Release();
        Raw.Resource.Value->ReleaseRef();
    }

    inline explicit TBlockItem(TStringValue&& value, ui32 size = Max<ui32>(), ui32 offset = 0U) {
        Y_DEBUG_ABORT_UNLESS(size);
        Y_DEBUG_ABORT_UNLESS(offset < std::min(TRawStringValue::OffsetLimit, value.Size()));
        Raw.StringValue.Size = std::min(value.Size() - offset, size);
        Raw.StringValue.Offset = offset;
        Raw.StringValue.Value = value.ReleaseBuf();
        Raw.StringValue.Meta = static_cast<ui8>(EMarkers::String);
    }

    inline explicit TBlockItem(bool value) {
        Raw.Simple.bool_ = value ? 1 : 0;
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
    }

    inline explicit TBlockItem(TStringRef value) {
        Raw.StringRef.Value = value.Data();
        Raw.StringRef.Size = value.Size();
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::String);
    }

    inline explicit TBlockItem(const TBlockItem* tupleItems) {
        Raw.Tuple.Value = tupleItems;
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded);
    }

    inline TBlockItem(ui64 low, ui64 high) {
        Raw.Halfs[0] = low;
        Raw.Halfs[1] = high;
    }

    inline ui64 Low() const {
        return Raw.Halfs[0];
    }

    inline ui64 High() const {
        return Raw.Halfs[1];
    }

    // TODO: deprecate As<T>() in favor of Get<T>()
    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline T As() const;

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline T Get() const;

    // TODO: deprecate AsTuple() in favor of GetElements()
    inline const TBlockItem* AsTuple() const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Embedded);
        return Raw.Tuple.Value;
    }

    inline const TBlockItem* GetElements() const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Embedded);
        return Raw.Tuple.Value;
    }

    inline TBlockItem GetElement(ui32 index) const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Embedded);
        return Raw.Tuple.Value[index];
    }

    // TUnboxedValuePod stores strings as refcounted TStringValue,
    // TBlockItem can store pointer to both refcounted string and simple string view
    inline TStringRef AsStringRef() const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::String);
        return TStringRef(Raw.StringRef.Value, Raw.StringRef.Size);
    }

    inline TStringValue AsStringValue() const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::String);
        return TStringValue(Raw.StringValue.Value);
    }
    
    inline TStringRef GetStringRefFromValue() const {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::String);
        return { Raw.StringValue.Value->Data() + (Raw.StringValue.Offset & 0xFFFFFF), Raw.StringValue.Size };
    }

    inline TBlockItem MakeOptional() const
    {
        if (Raw.Simple.Meta)
            return *this;

        TBlockItem result(*this);
        ++result.Raw.Simple.Count;
        return result;
    }

    inline TBlockItem GetOptionalValue() const
    {
        if (Raw.Simple.Meta)
            return *this;

        Y_DEBUG_ABORT_UNLESS(Raw.Simple.Count > 0U, "Can't get value from empty.");

        TBlockItem result(*this);
        --result.Raw.Simple.Count;
        return result;
    }

    inline IBoxedValuePtr GetBoxed() const
    {
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Boxed, "Value is not boxed");
        return Raw.Resource.Value;
    }

    inline void* GetRawPtr()
    {
        return &Raw;
    }

    inline const void* GetRawPtr() const
    {
        return &Raw;
    }

    inline explicit operator bool() const { return bool(Raw); }
    
    EMarkers GetMarkers() const {
        return static_cast<EMarkers>(Raw.Simple.Meta);
    }

    bool HasValue() const { return EMarkers::Empty != GetMarkers(); }

    bool IsBoxed() const { return EMarkers::Boxed == GetMarkers(); }
    bool IsEmbedded() const { return EMarkers::Embedded == GetMarkers(); }
    inline void SetTimezoneId(ui16 id) {
        UDF_VERIFY(GetMarkers() == EMarkers::Embedded, "Value is not a datetime");
        Raw.Simple.TimezoneId = id;
    }

    inline ui16 GetTimezoneId() const {
        UDF_VERIFY(GetMarkers() == EMarkers::Embedded, "Value is not a datetime");
        return Raw.Simple.TimezoneId;
    }

private:
    union TRaw {
        ui64 Halfs[2] = {0, 0};

        TRawEmbeddedValue Embedded;
        
        TRawBoxedValue Resource;

        TRawStringValue StringValue;

        struct {
            union {
                #define FIELD(type) type type##_;
                PRIMITIVE_VALUE_TYPES(FIELD);
                #undef FIELD
                // According to the YQL <-> arrow type mapping convention,
                // boolean values are processed as 8-bit unsigned integer
                // with either 0 or 1 as a condition payload.
                ui8 bool_;
                ui64 Count;
            };
            union {
                ui64 FullMeta;
                struct {
                    TTimezoneId TimezoneId;
                    ui8 Reserved[5];
                    ui8 Meta;
                };
            };
        } Simple;

        struct {
            const char* Value;
            ui32 Size;
            ui8 Reserved;
            ui8 Meta;
        } StringRef;

        struct {
            // client should know tuple size
            const TBlockItem* Value;
        } Tuple;

        explicit operator bool() const { return Simple.FullMeta | Simple.Count; }
    } Raw;
};

UDF_ASSERT_TYPE_SIZE(TBlockItem, 16);

#define VALUE_AS(xType) \
    template <> \
    inline xType TBlockItem::As<xType>() const \
    { \
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Embedded); \
        return Raw.Simple.xType##_; \
    }

#define VALUE_GET(xType) \
    template <> \
    inline xType TBlockItem::Get<xType>() const \
    { \
        Y_DEBUG_ABORT_UNLESS(GetMarkers() == EMarkers::Embedded); \
        return Raw.Simple.xType##_; \
    }

#define VALUE_CONSTR(xType) \
    template <> \
    inline TBlockItem::TBlockItem(xType value) \
    { \
        Raw.Simple.xType##_ = value; \
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Embedded); \
    }

PRIMITIVE_VALUE_TYPES(VALUE_AS)
PRIMITIVE_VALUE_TYPES(VALUE_GET)
PRIMITIVE_VALUE_TYPES(VALUE_CONSTR)
// XXX: TBlockItem constructor with <bool> parameter is implemented above.
VALUE_AS(bool)
VALUE_GET(bool)

#undef VALUE_AS
#undef VALUE_GET
#undef VALUE_CONSTR

}
