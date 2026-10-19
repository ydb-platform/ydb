#include <ydb/core/tablet_flat/flat_tier_predicate.h>
#include <ydb/core/tablet_flat/flat_util_misc.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/parser/pg_wrapper/interface/type_desc.h>

extern "C" {
#include "postgres.h"
#include "catalog/pg_type_d.h"
}

namespace NKikimr {
namespace NTable {

namespace {

    using EUnit = NKikimrSchemeOp::TTTLSettings::EUnit;

    constexpr TTag KeyTag = 1;
    constexpr TTag ValueTag = 2;

    // 2026-10-19T12:34:56.789012Z
    const TInstant Now = TInstant::MicroSeconds(1792413296789012ull);

    struct TPgCase {
        TIntrusiveConstPtr<TRowScheme> Scheme;
        NScheme::TTypeInfo Type;

        explicit TPgCase(ui32 pgTypeId)
            : Type(NPg::TypeDescFromPgTypeId(pgTypeId))
        {
            TVector<TColumn> cols;
            cols.emplace_back("key", KeyTag, NScheme::TTypeInfo(NScheme::NTypeIds::Uint64), "");
            cols.back().KeyOrder = 0;
            cols.emplace_back("value", ValueTag, Type, "");
            Scheme = TRowScheme::Make(cols, NUtil::TAsIs());
        }

        bool Matches(const TString& text, EUnit unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO) const {
            TTieringPolicy policy;
            policy.ColumnTag = ValueTag;
            policy.Unit = unit;
            policy.Rules.push_back(TTierRule{ .TierId = 1, .Before = TTierRelativeBound{ TDuration::Days(1) } });

            TTierPredicate predicate(policy, *Scheme);
            predicate.Bind(Now);

            auto value = NPg::PgNativeBinaryFromNativeText(text, Type.GetPgTypeDesc());
            UNIT_ASSERT_C(!value.Error, *value.Error);

            TRowState row(Scheme->Tags().size());
            row.Touch(ERowOp::Upsert);
            row.Set(Scheme->ColInfo(ValueTag)->Pos, ECellOp::Set, TCell(value.Str.data(), value.Str.size()));
            return predicate.Evaluate(row) == TTierDecision::Tier(1);
        }
    };

} // namespace

Y_UNIT_TEST_SUITE(TTierPredicatePg) {

    Y_UNIT_TEST(Date) {
        TPgCase pg(DATEOID);
        // The threshold is 2026-10-18T12:34:56.789012Z
        UNIT_ASSERT(pg.Matches("2026-10-17"));
        UNIT_ASSERT(pg.Matches("2026-10-18"));
        UNIT_ASSERT(!pg.Matches("2026-10-19"));
    }

    Y_UNIT_TEST(Timestamp) {
        TPgCase pg(TIMESTAMPOID);
        UNIT_ASSERT(pg.Matches("2026-10-18 12:34:56"));
        UNIT_ASSERT(!pg.Matches("2026-10-18 12:34:57"));
    }

    Y_UNIT_TEST(Integers) {
        const ui64 seconds = (Now - TDuration::Days(1)).Seconds();
        for (ui32 pgTypeId : {INT4OID, INT8OID}) {
            TPgCase pg(pgTypeId);
            UNIT_ASSERT(pg.Matches(ToString(seconds), NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS));
            UNIT_ASSERT(!pg.Matches(ToString(seconds + 1), NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS));
        }
    }

    Y_UNIT_TEST(Unsupported) {
        TPgCase pg(TEXTOID);
        TTieringPolicy policy;
        policy.ColumnTag = ValueTag;
        UNIT_ASSERT_EXCEPTION(TTierPredicate(policy, *pg.Scheme), yexception);

        // Integer columns require a unit
        TPgCase int8(INT8OID);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(policy, *int8.Scheme), yexception);
    }

}

}
}
