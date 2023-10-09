#pragma once
// unique tag to fix pragma once gcc glueing: ./ydb/core/tablet_flat/defs.h

#include "flat_page_iface.h"
#include "util_basics.h"

#include <ydb/core/base/defs.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/scheme_types/scheme_raw_type_value.h>
#include <ydb/core/util/type_alias.h>
#include <util/generic/xrange.h>
#include <util/generic/ylimits.h>

namespace NKikimr {

using TTxType = ui32; // App-specific transaction type number (protobuf enum values like TXTYPE_*)
constexpr TTxType UnknownTxType = TTxType(-1);

namespace NScheme {

    class TTypeRegistry;

} // end of NKikimr::NScheme namespace

namespace NTable {

    using TRawVals = TArrayRef<const TRawTypeValue>;

    class TEpoch : public TTypeSafeAlias<TEpoch, i64> {
    public:
        // N.B. this catches accidental legacy conversions
        TEpoch(ui64 value) = delete;

        explicit constexpr TEpoch(i64 value) noexcept
            : TBase(value)
        { }

        constexpr i64 ToProto() const noexcept {
            return Value;
        }

        constexpr i64 ToRedoLog() const noexcept {
            return Value;
        }

        /**
         * UGC uses current memtable epoch as part of a per-key counter
         *
         * It is always non-negative and materialized as ui64.
         */
        ui64 ToCounter() const noexcept {
            const auto value = Value;
            Y_ABORT_UNLESS(value >= 0);
            return value;
        }

        /**
         * Converts array index to epoch, used in tests
         */
        static constexpr TEpoch FromIndex(i64 index) noexcept {
            return TEpoch(index);
        }

        friend inline IOutputStream& operator<<(IOutputStream& out, const TEpoch& epoch) {
            return out << epoch.Value;
        }
    };

    enum class ELookup {
        ExactMatch = 1,
        GreaterOrEqualThan = 2,
        GreaterThan = 3,
    };

    struct TTxStamp {
        constexpr TTxStamp(ui64 raw = 0) : Raw(raw) { }
        TTxStamp(const TLogoBlobID &logo)
            : TTxStamp(logo.Generation(), logo.Step()) { }
        constexpr TTxStamp(ui32 gen, ui32 step)
            : Raw((ui64(gen) << 32) | step)
        {

        }

        constexpr bool operator <(ui64 raw) const noexcept
        {
            return Raw < raw;
        }

        constexpr operator ui64() const noexcept { return Raw; }
        constexpr  ui32 Gen() const noexcept { return Raw >> 32; }
        constexpr ui32 Step() const noexcept { return Raw;  }

        ui64 Raw = 0;
    };

    struct TSnapEdge {
        TSnapEdge() = default;

        TSnapEdge(ui64 txStamp, TEpoch head)
            : TxStamp(txStamp)
            , Head(head)
        {

        }

        ui64 TxStamp = 0;
        TEpoch Head = TEpoch::Zero();

        friend bool operator==(const TSnapEdge& a, const TSnapEdge& b) {
            return a.TxStamp == b.TxStamp && a.Head == b.Head;
        }
    };
}

namespace NTabletFlatExecutor {
    using TTxStamp = NTable::TTxStamp;
    using TRawVals = NTable::TRawVals;
}
}
