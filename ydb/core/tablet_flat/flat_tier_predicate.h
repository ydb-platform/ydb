#pragma once

#include "flat_row_scheme.h"
#include "flat_row_state.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
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
    };

    /**
     * Absolute tier bound: a fixed moment in microseconds since the epoch,
     * may be negative (before the epoch)
     */
    struct TTierAbsoluteBound {
        i64 Micros = 0;
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
    };

    /**
     * A rule matches a row when the value falls into any of its ranges.
     * Interval("P1D") is a single range {To = Relative(1d), ToInclusive = true},
     * YEAR(c) = 2025 is a single range [2025-01-01, 2026-01-01).
     */
    struct TTierRule {
        ui32 TierId = 0;
        TVector<TTierRange> Ranges;
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
    };

    class TTierDecision {
    public:
        static TTierDecision Undecided() {
            return TTierDecision(Max<ui32>());
        }

        static TTierDecision Tier(ui32 tierId) {
            Y_ENSURE(tierId != Max<ui32>());
            return TTierDecision(tierId);
        }

        bool IsUndecided() const {
            return TierId == Max<ui32>();
        }

        ui32 GetTierId() const {
            Y_ENSURE(!IsUndecided());
            return TierId;
        }

        bool operator==(const TTierDecision&) const = default;

    private:
        explicit TTierDecision(ui32 tierId)
            : TierId(tierId)
        { }

    private:
        ui32 TierId;
    };

    /**
     * Evaluates the desired tier of a row according to a tiering policy.
     *
     * The constructor validates the policy, resolves the column in the given
     * scheme and converts range bounds into the column's native representation
     * once, so Evaluate() does a couple of comparisons per range without
     * allocations. A predicate is created for a single compaction: rows passed
     * to Evaluate() must follow the layout of the given scheme, and both the
     * policy and the scheme may change between compactions.
     * An inclusive upper bound matches row table TTL (value <= now - offset).
     */
    class TTierPredicate {
    public:
        static constexpr ui32 DefaultTierId = 0;

        TTierPredicate(const TTieringPolicy& policy, TInstant now, const TRowScheme& scheme);

        /**
         * Validates the policy alone, without a scheme (e.g. when the policy is altered)
         */
        static void Validate(const TTieringPolicy& policy);

        TTierDecision Evaluate(const TRowState& row) const;

        TPos GetColumnPos() const {
            return Pos;
        }

        /**
         * Tier ids of rules with a bound that could not be converted into the
         * column type. Ranges with such bounds never match,
         * so callers should report them instead of silently skipping tiering.
         */
        TVector<ui32> GetUnrepresentableTierIds() const;

    private:
        enum class ECmp {
            Less,
            LessOrEqual,
            GreaterOrEqual,
        };

        struct TThreshold {
            enum class EKind {
                // Compare the value with the threshold
                Value,
                // Every value of the column type satisfies the comparison
                AlwaysMatch,
                // No value of the column type satisfies the comparison
                NeverMatch,
                // Conversion failed, the comparison never holds
                Unrepresentable,
            };

            EKind Kind = EKind::Unrepresentable;
            ECmp Cmp = ECmp::LessOrEqual;
            // Threshold in the native cell representation of the column type
            TCell Bound;
            // Owns Bound data that does not fit into the cell (a long DyNumber). TString
            // shares its buffer on copy and is never modified, so Bound stays valid
            TString Storage;
        };

        struct TCompiledRange {
            std::optional<TThreshold> From;
            std::optional<TThreshold> To;
        };

        // ttlRounding keeps row table TTL rounding of relative (Interval) bounds
        TThreshold MakeThreshold(i64 micros, ECmp cmp, bool ttlRounding) const;
        i64 ToMicros(const TTierBound& bound, i64 nowUs) const;
        bool Matches(const TCell& cell, const TThreshold& threshold) const;
        int Compare(const TCell& cell, const TThreshold& threshold) const;

    private:
        const TTieringPolicy Policy;
        NScheme::TTypeInfo Type;
        NScheme::TTypeInfoOrder TypeOrder;
        TPos Pos = Max<TPos>();
        TVector<TVector<TCompiledRange>> Compiled;
    };

}
}
