#pragma once

#include <ydb/core/base/row_version.h>

#include <util/system/types.h>
#include <util/generic/ylimits.h>
#include <util/generic/array_ref.h>

namespace NKikimr {
namespace NTable {

    using TRowId = ui64;
    using TTag  = ui32;
    using TPos  = ui32;  /* Column logical position      */
    using TTagsRef = TArrayRef<const TTag>;
    using TGroup = ui32; /* Column logical group         */

    enum class ESeek {
        Exact = 1,
        Lower = 2,      /* lower_bound, { } -> begin()  */
        Upper = 3,      /* upper_bound, { } -> end()    */
    };

    enum class EReady {
        Page    = 0,    /* Need some pages to be loaded */
        Data    = 1,
        Gone    = 2,    /* Iterator finally invalidated */
    };

    enum class EDirection {
        Forward,
        Reverse,
    };

    enum class ERowOp : ui8 { /* row operation code */
        Absent  = 0,
        Upsert  = 1,    /* Set value or update row      */
        Erase   = 2,    /* Explicit null or row drop    */
        Reset   = 3,    /* Reset cell or row to default */
    };

    enum class ECellOp : ui8 { /* cell operation code */
        Empty   = 0,    /* Cell has no any valid state  */
        Set     = 1,    /* Set cell to exact value      */
        Null    = 2,    /* Explicit cell null value     */
        Reset   = 3,    /* Reset cell value to default  */
    };

    enum class ELargeObj : ui8 {
        Inline  = 0,    /* Data stored within storage   */
        Extern  = 1,    /* Data placed in external blob */
        GlobId  = 2,    /* Explicit TGlobId reference   */
        Outer   = 3,    /* ~ is packed out of rows page */
    };

    enum EHint : ui64 {
        NoByKey = 0x1,
    };

    struct TLargeObj {
        TLargeObj() = default;

        TLargeObj(ELargeObj lob, ui64 ref) : Lob(lob), Ref(ref) { }

        explicit operator bool() const noexcept
        {
            return Lob != ELargeObj::Inline && Ref != Max<ui64>();
        }

        ELargeObj Lob = ELargeObj::Inline;
        ui64 Ref = Max<ui64>();
    };

    struct TSelectRowVersionResult {
        EReady Ready;
        TRowVersion RowVersion;

        TSelectRowVersionResult(EReady ready)
            : Ready(ready)
        { }

        TSelectRowVersionResult(const TRowVersion& rowVersion)
            : Ready(EReady::Data)
            , RowVersion(rowVersion)
        { }

        explicit operator bool() const {
            return Ready == EReady::Data;
        }
    };

#pragma pack(push, 1)

    struct TCellOp {
        constexpr TCellOp() = default;

        static constexpr TCellOp Decode(ui8 raw) noexcept
        {
            return { ECellOp(raw & 0x3f), ELargeObj(raw >> 6) };
        }

        constexpr TCellOp(ECellOp op, ELargeObj lob = ELargeObj::Inline) noexcept
            : Value(ui8(op) | (ui8(lob) << 6))
        {

        }

        constexpr ui8 Raw() const noexcept
        {
            return Value;
        }

        constexpr operator ECellOp() const noexcept
        {
            return ECellOp(Value & 0x3f);
        }

        constexpr operator ELargeObj() const noexcept
        {
            return ELargeObj(Value >> 6);
        }

        constexpr ui8 operator*() const noexcept
        {
            return Value;
        }

        TCellOp& operator=(ECellOp op) noexcept
        {
            Value = (Value & ~0x3f) | ui8(op);

            return *this;
        }

        TCellOp& operator=(ELargeObj lob) noexcept
        {
            Value = TCellOp(ECellOp(*this), lob).Raw();

            return  *this;
        }

        constexpr static bool HaveNoPayload(ECellOp cellOp) noexcept
        {
            return
                cellOp == ECellOp::Null
                || cellOp == ECellOp::Reset
                || cellOp == ECellOp::Empty;
        }

        constexpr static bool HaveNoOps(ERowOp rop) noexcept
        {
            return rop == ERowOp::Erase;
        }

    private:
        ui8 Value = 0;
    } Y_PACKED;

#pragma pack(pop)

    static_assert(sizeof(TCellOp) == 1, "Unexpected TCellOp unit size");

}
}
