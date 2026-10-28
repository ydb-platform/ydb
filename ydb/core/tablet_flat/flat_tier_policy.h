#pragma once

#include "flat_row_eggs.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/scheme_log.pb.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <optional>
#include <variant>

namespace NKikimr {
namespace NTable {

    /**
     * Relative tier bound: the moment now - Offset
     */
    struct TTierRelativeBound {
        TDuration Offset;

        bool operator==(const TTierRelativeBound&) const = default;
    };

    /**
     * Absolute tier bound: a fixed moment in microseconds since the epoch,
     * may be negative (before the epoch)
     */
    struct TTierAbsoluteBound {
        i64 Micros = 0;

        bool operator==(const TTierAbsoluteBound&) const = default;
    };

    using TTierBound = std::variant<TTierRelativeBound, TTierAbsoluteBound>;

    /**
     * A range of column values in time: [From, To) or [From, To] when ToInclusive.
     * A missing bound means the range is open on that side.
     */
    struct TTierRange {
        std::optional<TTierBound> From;
        std::optional<TTierBound> To;
        bool ToInclusive = false;

        bool operator==(const TTierRange&) const = default;
    };

    /**
     * A rule matches a row when the value falls into any of its ranges.
     * Interval("P1D") is a single range {To = Relative(1d), ToInclusive = true},
     * YEAR(c) = 2025 is a single range [2025-01-01, 2026-01-01).
     */
    struct TTierRule {
        ui32 TierId = 0; // id of the table family that keeps the tier placement
        TVector<TTierRange> Ranges;

        bool operator==(const TTierRule&) const = default;
    };

    /**
     * Tiering policy over a single column, enforced by TTierPredicate:
     * - all bounds are either relative or absolute;
     * - a relative rule is a single inclusive upper bound (Interval), relative
     *   rules are ordered from the coldest tier to the warmest one (strictly
     *   descending offsets), the first matching rule wins;
     * - ranges of different absolute rules do not overlap, so their order
     *   does not matter.
     */
    struct TTieringPolicy {
        TTag ColumnTag = Max<TTag>();
        NKikimrSchemeOp::TTTLSettings::EUnit Unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO;
        TVector<TTierRule> Rules;

        bool operator==(const TTieringPolicy&) const = default;
    };

    // Replaces the whole content of the proto with the policy
    void TieringPolicyToProto(const TTieringPolicy& policy, NTabletFlatScheme::TTieringPolicySettings& proto);
    TTieringPolicy TieringPolicyFromProto(const NTabletFlatScheme::TTieringPolicySettings& proto);

}
}
