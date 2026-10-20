#pragma once

#include "flat_row_scheme.h"
#include "flat_row_state.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/datetime/base.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <variant>

namespace NKikimr {
namespace NTable {

    /**
     * Relative tier bound: a row matches when its value <= now - Offset
     */
    struct TTierRelativeBound {
        TDuration Offset;
    };

    using TTierBound = std::variant<TTierRelativeBound>;

    struct TTierRule {
        ui32 TierId = 0;
        TTierBound Before;
    };

    /**
     * Tiering policy over a single column. Rules are ordered from the coldest
     * tier to the warmest one (strictly descending offsets, enforced by
     * TTierPredicate), the first matching rule wins.
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
     * Bind() resolves the column in the given scheme and converts thresholds into
     * the column's native representation once, so Evaluate() is a single
     * comparison per rule without allocations. Rows passed to Evaluate() must
     * follow the layout of the scheme from the last Bind() call.
     * Comparison semantics match row table TTL (value <= now - offset).
     */
    class TTierPredicate {
    public:
        static constexpr ui32 DefaultTierId = 0;

        explicit TTierPredicate(const TTieringPolicy& policy);

        void Bind(TInstant now, const TRowScheme& scheme);

        TTierDecision Evaluate(const TRowState& row) const;

        TPos GetColumnPos() const {
            Y_ENSURE(Bound);
            return Pos;
        }

        /**
         * Tier ids of rules whose threshold could not be converted into the
         * column type on the last Bind(). Such rules never match, so callers
         * should report them instead of silently skipping tiering.
         */
        TVector<ui32> GetUnrepresentableTierIds() const;

    private:
        struct TThreshold {
            enum class EKind {
                // Compare the value with the threshold
                Value,
                // Every value of the column type is below the threshold
                AlwaysMatch,
                // Every value of the column type is above the threshold
                NeverMatch,
                // Conversion failed, the rule never matches
                Unrepresentable,
            };

            EKind Kind = EKind::Unrepresentable;
            i64 Signed = 0;
            ui64 Unsigned = 0;
            TString Serialized;
        };

        TThreshold MakeThreshold(i64 thresholdUs) const;
        bool Matches(const TCell& cell, const TThreshold& threshold) const;

    private:
        const TTieringPolicy Policy;
        NScheme::TTypeInfo Type;
        ui32 PgTypeId = 0; // Postgres type oid of a Pg column
        TPos Pos = Max<TPos>();
        TVector<TThreshold> Thresholds;
        bool Bound = false;
    };

}
}
