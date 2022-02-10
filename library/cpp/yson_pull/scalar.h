#pragma once

#include "cyson_enums.h"

#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

namespace NYsonPull {
    //! \brief YSON TScalar value type tag
    enum class EScalarType {
        Entity = YSON_SCALAR_ENTITY,
        Boolean = YSON_SCALAR_BOOLEAN,
        Int64 = YSON_SCALAR_INT64,
        UInt64 = YSON_SCALAR_UINT64,
        Float64 = YSON_SCALAR_FLOAT64,
        String = YSON_SCALAR_STRING,
    };

    //! \brief YSON TScalar value variant
    class TScalar {
        //! \internal \brief YSON TScalar value underlying representation
        union TScalarValue {
            struct TScalarStringRef {
                const char* Data;
                size_t Size;
            };

            ui8 AsNothing[1];
            bool AsBoolean;
            i64 AsInt64;
            ui64 AsUInt64;
            double AsFloat64;
            TScalarStringRef AsString;

            constexpr TScalarValue()
                : AsNothing{} {
            }

            explicit constexpr TScalarValue(bool value)
                : AsBoolean{value} {
            }

            explicit constexpr TScalarValue(i64 value)
                : AsInt64{value} {
            }

            explicit constexpr TScalarValue(ui64 value)
                : AsUInt64{value} {
            }

            explicit constexpr TScalarValue(double value)
                : AsFloat64{value} {
            }

            explicit constexpr TScalarValue(TStringBuf value)
                : AsString{value.data(), value.size()} {
            }
        };
        static_assert(
            sizeof(TScalarValue) == sizeof(TStringBuf),
            "bad scalar_value size");

        EScalarType Type_;
        TScalarValue Value_;

    public:
        constexpr TScalar()
            : Type_{EScalarType::Entity} {
        }

        explicit constexpr TScalar(bool value)
            : Type_{EScalarType::Boolean}
            , Value_{value} {
        }

        explicit constexpr TScalar(i64 value)
            : Type_{EScalarType::Int64}
            , Value_{value} {
        }

        explicit constexpr TScalar(ui64 value)
            : Type_{EScalarType::UInt64}
            , Value_{value} {
        }

        explicit constexpr TScalar(double value)
            : Type_{EScalarType::Float64}
            , Value_{value} {
        }

        explicit constexpr TScalar(TStringBuf value)
            : Type_{EScalarType::String}
            , Value_{value} {
        }

        // Disambiguation for literal constants
        // In the absence of this constructor,
        // they get implicitly converted to bool (yikes!)
        explicit TScalar(const char* value)
            : Type_{EScalarType::String}
            , Value_{TStringBuf{value}} {
        }

        EScalarType Type() const {
            return Type_;
        }

#define CAST_TO(Type)                     \
    Y_ASSERT(Type_ == EScalarType::Type); \
    return Value_.As##Type

        bool AsBoolean() const {
            CAST_TO(Boolean);
        }
        i64 AsInt64() const {
            CAST_TO(Int64);
        }
        ui64 AsUInt64() const {
            CAST_TO(UInt64);
        }
        double AsFloat64() const {
            CAST_TO(Float64);
        }
#undef CAST_TO

        TStringBuf AsString() const {
            Y_ASSERT(Type_ == EScalarType::String);
            return TStringBuf{
                Value_.AsString.Data,
                Value_.AsString.Size,
            };
        }

        const TScalarValue& AsUnsafeValue() const {
            return Value_;
        }
    };

    bool operator==(const TScalar& left, const TScalar& right) noexcept;

    inline bool operator!=(const TScalar& left, const TScalar& right) noexcept {
        return !(left == right);
    }

}
