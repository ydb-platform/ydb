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

        TTierPredicate MakePredicate(EUnit unit, TInstant now, TDuration offset) const {
            TTieringPolicy policy;
            policy.ColumnTag = ValueTag;
            policy.Unit = unit;
            policy.Rules.push_back(TTierRule{ .TierId = 1, .Before = TTierRelativeBound{ offset } });

            TTierPredicate predicate(policy);
            predicate.Bind(now, *Scheme);
            return predicate;
        }

        bool Matches(const TString& text, EUnit unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO,
                TInstant now = Now, TDuration offset = TDuration::Days(1)) const
        {
            const auto predicate = MakePredicate(unit, now, offset);
            UNIT_ASSERT(predicate.GetUnrepresentableTierIds().empty());

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

            // Negative values are older than a positive threshold
            UNIT_ASSERT(pg.Matches("-1", NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS));
            UNIT_ASSERT(pg.Matches(ToString(-i64(seconds)), NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS));
        }
    }

    Y_UNIT_TEST(IntegerThresholdOutOfRange) {
        const auto seconds = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
        const auto millis = NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS;
        const auto nanos = NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS;

        // The current time in milliseconds does not fit into int4, so every value is older
        TPgCase int4(INT4OID);
        UNIT_ASSERT(int4.Matches(ToString(Max<i32>()), millis));
        UNIT_ASSERT(int4.Matches("0", millis));
        UNIT_ASSERT(int4.Matches(ToString(Min<i32>()), millis));
        UNIT_ASSERT(int4.Matches(ToString(Max<i32>()), nanos));

        // In seconds int4 overflows after 2038
        const TInstant after2038 = TInstant::Seconds(Max<i32>()) + TDuration::Days(10);
        UNIT_ASSERT(int4.Matches(ToString(Max<i32>()), seconds, after2038));

        // Nanoseconds overflow neither int8 nor the threshold computation
        TPgCase int8(INT8OID);
        const i64 thresholdNs = (Now - TDuration::Days(1)).MicroSeconds() * 1000;
        UNIT_ASSERT(int8.Matches(ToString(thresholdNs), nanos));
        UNIT_ASSERT(!int8.Matches(ToString(thresholdNs + 1), nanos));
    }

    Y_UNIT_TEST(IntegerThresholdBeforeEpoch) {
        // now - offset is 10 days (864000 seconds) before the epoch
        const TInstant now = TInstant::Days(10);
        const TDuration offset = TDuration::Days(20);
        const auto seconds = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;

        TPgCase int8(INT8OID);
        UNIT_ASSERT(int8.Matches("-864001", seconds, now, offset));
        UNIT_ASSERT(int8.Matches("-864000", seconds, now, offset));
        UNIT_ASSERT(!int8.Matches("-863999", seconds, now, offset));
    }

    Y_UNIT_TEST(ThresholdsBeforeEpoch) {
        // now - offset is 1969-12-22T00:00:00, before both the Unix and the Postgres epochs
        const TInstant now = TInstant::Days(10);
        const TDuration offset = TDuration::Days(20);

        TPgCase date(DATEOID);
        UNIT_ASSERT(date.Matches("1969-12-21", {}, now, offset));
        UNIT_ASSERT(date.Matches("1969-12-22", {}, now, offset));
        UNIT_ASSERT(!date.Matches("1969-12-23", {}, now, offset));
        UNIT_ASSERT(!date.Matches("2000-01-01", {}, now, offset));
        UNIT_ASSERT(date.Matches("0001-01-01", {}, now, offset));

        TPgCase timestamp(TIMESTAMPOID);
        UNIT_ASSERT(timestamp.Matches("1969-12-22 00:00:00", {}, now, offset));
        UNIT_ASSERT(!timestamp.Matches("1969-12-22 00:00:00.000001", {}, now, offset));
    }

    Y_UNIT_TEST(Infinities) {
        // Postgres -infinity is older than any threshold, infinity is newer
        TPgCase date(DATEOID);
        UNIT_ASSERT(date.Matches("-infinity"));
        UNIT_ASSERT(!date.Matches("infinity"));

        TPgCase timestamp(TIMESTAMPOID);
        UNIT_ASSERT(timestamp.Matches("-infinity"));
        UNIT_ASSERT(!timestamp.Matches("infinity"));

        // A threshold below the timestamp range still matches -infinity only
        // (TDuration::Max) avoids the Max(x, y) macro from Postgres headers
        const TInstant zero = TInstant::Zero();
        const TDuration maxOffset = (TDuration::Max)();
        UNIT_ASSERT(timestamp.Matches("-infinity", {}, zero, maxOffset));
        UNIT_ASSERT(!timestamp.Matches("4713-01-01 00:00:00 BC", {}, zero, maxOffset));
    }

    Y_UNIT_TEST(ThresholdsOutsideValidRange) {
        // Thresholds before 4714-11-24 BC are not valid Postgres values: only -infinity is older
        const TDuration tenThousandYears = TDuration::Days(365 * 10000);

        TPgCase date(DATEOID);
        UNIT_ASSERT(date.Matches("-infinity", {}, Now, tenThousandYears));
        UNIT_ASSERT(!date.Matches("4714-11-24 BC", {}, Now, tenThousandYears));

        TPgCase timestamp(TIMESTAMPOID);
        UNIT_ASSERT(timestamp.Matches("-infinity", {}, Now, tenThousandYears));
        UNIT_ASSERT(!timestamp.Matches("4714-11-24 00:00:00 BC", {}, Now, tenThousandYears));
    }

    Y_UNIT_TEST(AroundPostgresEpoch) {
        // Postgres stores dates and timestamps relative to 2000-01-01
        const TInstant now = TInstant::Seconds(946684800) + TDuration::Days(1);
        const TDuration offset = TDuration::Days(1);

        TPgCase date(DATEOID);
        UNIT_ASSERT(date.Matches("1999-12-31", {}, now, offset));
        UNIT_ASSERT(date.Matches("2000-01-01", {}, now, offset));
        UNIT_ASSERT(!date.Matches("2000-01-02", {}, now, offset));

        TPgCase timestamp(TIMESTAMPOID);
        UNIT_ASSERT(timestamp.Matches("1999-12-31 23:59:59.999999", {}, now, offset));
        UNIT_ASSERT(timestamp.Matches("2000-01-01 00:00:00", {}, now, offset));
        UNIT_ASSERT(!timestamp.Matches("2000-01-01 00:00:00.000001", {}, now, offset));
    }

    Y_UNIT_TEST(IntegerRangeEdges) {
        const auto seconds = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
        TPgCase int4(INT4OID);
        UNIT_ASSERT(int4.Matches(ToString(Min<i32>()), seconds));
        UNIT_ASSERT(!int4.Matches(ToString(Max<i32>()), seconds));
        TPgCase int8(INT8OID);
        UNIT_ASSERT(int8.Matches(ToString(Min<i64>()), seconds));
        UNIT_ASSERT(!int8.Matches(ToString(Max<i64>()), seconds));
    }

    Y_UNIT_TEST(Unsupported) {
        TPgCase pg(TEXTOID);
        TTieringPolicy policy;
        policy.ColumnTag = ValueTag;
        UNIT_ASSERT_EXCEPTION(TTierPredicate(policy).Bind(Now, *pg.Scheme), yexception);

        // Integer columns require a unit
        TPgCase int8(INT8OID);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(policy).Bind(Now, *int8.Scheme), yexception);
    }

}

}
}
