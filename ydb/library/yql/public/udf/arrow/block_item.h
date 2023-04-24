#pragma once

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_string_ref.h>
#include <ydb/library/yql/public/udf/udf_type_size_check.h>

namespace NYql::NUdf {

// ABI stable
class TBlockItem {
    enum class EMarkers : ui8 {
        Empty = 0,
        Present = 1,
    };

public:
    TBlockItem() noexcept = default;
    ~TBlockItem() noexcept = default;

    TBlockItem(const TBlockItem& value) noexcept = default;
    TBlockItem(TBlockItem&& value) noexcept = default;

    TBlockItem& operator=(const TBlockItem& value) noexcept = default;
    TBlockItem& operator=(TBlockItem&& value) noexcept = default;

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline explicit TBlockItem(T value);

    inline explicit TBlockItem(TStringRef value) {
        Raw.String.Value = value.Data();
        Raw.String.Size = value.Size();
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Present);
    }

    inline explicit TBlockItem(const TBlockItem* tupleItems) {
        Raw.Tuple.Value = tupleItems;
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Present);
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

    template <typename T, typename = std::enable_if_t<TPrimitiveDataType<T>::Result>>
    inline T As() const;

    inline const TBlockItem* AsTuple() const {
        Y_VERIFY_DEBUG(Raw.GetMarkers() == EMarkers::Present);
        return Raw.Tuple.Value;
    }

    inline TStringRef AsStringRef() const {
        Y_VERIFY_DEBUG(Raw.GetMarkers() == EMarkers::Present);
        return TStringRef(Raw.String.Value, Raw.String.Size);
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

        Y_VERIFY_DEBUG(Raw.Simple.Count > 0U, "Can't get value from empty.");

        TBlockItem result(*this);
        --result.Raw.Simple.Count;
        return result;
    }

    inline explicit operator bool() const { return bool(Raw); }
private:
    union TRaw {
        ui64 Halfs[2] = {0, 0};
        struct {
            union {
                #define FIELD(type) type type##_;
                PRIMITIVE_VALUE_TYPES(FIELD);
                #undef FIELD
                ui64 Count;
            };
            union {
                ui64 Pad;
                struct {
                    ui8 Reserved[7];
                    ui8 Meta;
                };
            };
        } Simple;

        struct {
            const char* Value;
            ui32 Size;
        } String;

        struct {
            // client should know tuple size
            const TBlockItem* Value;
        } Tuple;

        EMarkers GetMarkers() const {
            return static_cast<EMarkers>(Simple.Meta);
        }

        explicit operator bool() const { return Simple.Meta | Simple.Count; }
    } Raw;
};

UDF_ASSERT_TYPE_SIZE(TBlockItem, 16);

#define VALUE_GET(xType) \
    template <> \
    inline xType TBlockItem::As<xType>() const \
    { \
        Y_VERIFY_DEBUG(Raw.GetMarkers() == EMarkers::Present); \
        return Raw.Simple.xType##_; \
    }

#define VALUE_CONSTR(xType) \
    template <> \
    inline TBlockItem::TBlockItem(xType value) \
    { \
        Raw.Simple.xType##_ = value; \
        Raw.Simple.Meta = static_cast<ui8>(EMarkers::Present); \
    }

PRIMITIVE_VALUE_TYPES(VALUE_GET)
PRIMITIVE_VALUE_TYPES(VALUE_CONSTR)

#undef VALUE_GET
#undef VALUE_CONSTR

}
