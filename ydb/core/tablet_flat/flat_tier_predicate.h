#pragma once

#include "flat_row_scheme.h"
#include "flat_row_state.h"
#include "flat_tier_policy.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr {
namespace NTable {

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

        /**
         * Validates that the policy can be evaluated over a column of the given type
         */
        static void ValidateColumn(const TTieringPolicy& policy, const NScheme::TTypeInfo& type);

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
            i64 Signed = 0;
            ui64 Unsigned = 0;
            TString Serialized;
        };

        struct TCompiledRange {
            std::optional<TThreshold> From;
            std::optional<TThreshold> To;
        };

        TThreshold MakeThreshold(i64 micros, ECmp cmp) const;
        i64 ToMicros(const TTierBound& bound, i64 nowUs) const;
        bool Matches(const TCell& cell, const TThreshold& threshold) const;
        int Compare(const TCell& cell, const TThreshold& threshold) const;

    private:
        const TTieringPolicy Policy;
        NScheme::TTypeInfo Type;
        ui32 PgTypeId = 0; // Postgres type oid of a Pg column
        TPos Pos = Max<TPos>();
        TVector<TVector<TCompiledRange>> Compiled;
    };

}
}
