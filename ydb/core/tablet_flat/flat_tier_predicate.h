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
     * tier to the warmest one, the first matching rule wins.
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
     * Bind() converts thresholds into the column's native representation once,
     * so Evaluate() is a single comparison per rule without allocations.
     * Comparison semantics match row table TTL (value <= now - offset).
     */
    class TTierPredicate {
    public:
        static constexpr ui32 DefaultTierId = 0;

        TTierPredicate(const TTieringPolicy& policy, const TRowScheme& scheme);

        void Bind(TInstant now);

        TTierDecision Evaluate(const TRowState& row) const;

        TPos GetColumnPos() const {
            return Pos;
        }

    private:
        struct TThreshold {
            // false when the threshold is not representable for the column type,
            // a rule with such threshold never matches
            bool Valid = false;
            i64 Signed = 0;
            ui64 Unsigned = 0;
            TString Serialized;
        };

        TThreshold MakeThreshold(i64 thresholdUs) const;
        bool Matches(const TCell& cell, const TThreshold& threshold) const;

    private:
        const TTieringPolicy Policy;
        const NScheme::TTypeInfo Type;
        const TPos Pos;
        TVector<TThreshold> Thresholds;
        bool Bound = false;
    };

}
}
