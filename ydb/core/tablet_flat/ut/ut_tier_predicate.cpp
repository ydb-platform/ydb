#include <ydb/core/tablet_flat/flat_tier_predicate.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/types/dynumber/dynumber.h>

namespace NKikimr {
namespace NTable {

namespace {

    using EUnit = NKikimrSchemeOp::TTTLSettings::EUnit;
    using NTest::TLayoutCook;

    constexpr TTag KeyTag = 1;
    constexpr TTag ValueTag = 2;

    // 2026-10-19T12:34:56.789012Z: non-zero time of day to test rounding
    const TInstant Now = TInstant::MicroSeconds(1792413296789012ull);

    TLayoutCook MakeLayout(NScheme::TTypeId type) {
        TLayoutCook lay;
        lay
            .Col(0, KeyTag, NScheme::NTypeIds::Uint64)
            .Col(0, ValueTag, type)
            .Key({ KeyTag });
        return lay;
    }

    TTieringPolicy MakePolicy(std::initializer_list<std::pair<TDuration, ui32>> rules,
            EUnit unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO)
    {
        TTieringPolicy policy;
        policy.ColumnTag = ValueTag;
        policy.Unit = unit;
        for (const auto& [offset, tierId] : rules) {
            policy.Rules.push_back(TTierRule{ .TierId = tierId, .Before = TTierRelativeBound{ offset } });
        }
        return policy;
    }

    TRowState MakeRow(const TRowScheme& scheme, const TCell& cell, TCellOp op = ECellOp::Set) {
        TRowState row(scheme.Tags().size());
        row.Touch(ERowOp::Upsert);
        row.Set(scheme.ColInfo(ValueTag)->Pos, op, cell);
        return row;
    }

    template <typename T>
    TTierDecision EvaluateValue(const TRowScheme& scheme, const TTierPredicate& predicate, T value) {
        return predicate.Evaluate(MakeRow(scheme, TCell::Make(value)));
    }

    TTierDecision EvaluateValue(const TRowScheme& scheme, const TTierPredicate& predicate, const TString& value) {
        return predicate.Evaluate(MakeRow(scheme, TCell(value.data(), value.size())));
    }

    TTierDecision Tier(ui32 tierId) {
        return TTierDecision::Tier(tierId);
    }

    // Single rule policy decision for a value of the given type, with the
    // matching tier id 1 and the default tier id 0
    template <typename T>
    bool SingleRuleMatches(NScheme::TTypeId type, EUnit unit, TDuration offset, T value, TInstant now = Now) {
        auto lay = MakeLayout(type);
        TTierPredicate predicate(MakePolicy({{offset, 1}}, unit));
        predicate.Bind(now, *lay);
        return EvaluateValue(*lay, predicate, value) == Tier(1);
    }

    // Reference semantics of row table TTL (see TExpirationCondition in datashard)
    bool TtlExpiredUi64(NScheme::TTypeId type, EUnit unit, ui64 value, TInstant wallClock) {
        switch (type) {
            case NScheme::NTypeIds::Date:
                return TInstant::Days(value) <= wallClock;
            case NScheme::NTypeIds::Datetime:
                return TInstant::Seconds(value) <= wallClock;
            case NScheme::NTypeIds::Timestamp:
                return TInstant::MicroSeconds(value) <= wallClock;
            case NScheme::NTypeIds::Uint32:
            case NScheme::NTypeIds::Uint64:
                switch (unit) {
                    case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                        return TInstant::Seconds(value) <= wallClock;
                    case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                        return TInstant::MilliSeconds(value) <= wallClock;
                    case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                        return TInstant::MicroSeconds(value) <= wallClock;
                    case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                        return TInstant::MicroSeconds(value / 1000) <= wallClock;
                    default:
                        Y_ENSURE(false);
                }
            default:
                Y_ENSURE(false);
        }
    }

    bool TtlExpiredI64(NScheme::TTypeId type, i64 value, TInstant wallClock) {
        if (value < 0) {
            return true;
        }
        switch (type) {
            case NScheme::NTypeIds::Date32:
                return TInstant::Days(value) <= wallClock;
            case NScheme::NTypeIds::Datetime64:
                return TInstant::Seconds(value) <= wallClock;
            case NScheme::NTypeIds::Timestamp64:
                return TInstant::MicroSeconds(value) <= wallClock;
            default:
                Y_ENSURE(false);
        }
    }

    ui64 ToUnit(TInstant instant, NScheme::TTypeId type, EUnit unit) {
        switch (type) {
            case NScheme::NTypeIds::Date:
            case NScheme::NTypeIds::Date32:
                return instant.Days();
            case NScheme::NTypeIds::Datetime:
            case NScheme::NTypeIds::Datetime64:
                return instant.Seconds();
            case NScheme::NTypeIds::Timestamp:
            case NScheme::NTypeIds::Timestamp64:
                return instant.MicroSeconds();
            default:
                break;
        }
        switch (unit) {
            case NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS:
                return instant.Seconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS:
                return instant.MilliSeconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS:
                return instant.MicroSeconds();
            case NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS:
                return instant.NanoSeconds();
            default:
                Y_ENSURE(false);
        }
    }

    TString DyNumber(ui64 value) {
        auto result = NDyNumber::ParseDyNumberString(ToString(value));
        Y_ENSURE(result);
        return *result;
    }

} // namespace

Y_UNIT_TEST_SUITE(TTierPredicate) {

    Y_UNIT_TEST(TimestampBoundary) {
        const TDuration offset = TDuration::Days(1);
        const ui64 threshold = (Now - offset).MicroSeconds();

        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, threshold - 1));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, threshold));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, threshold + 1));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, ui64(0)));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, Now.MicroSeconds()));
    }

    Y_UNIT_TEST(DateAndDatetimeRounding) {
        const TDuration offset = TDuration::Days(1);
        const TInstant threshold = Now - offset;

        // The threshold is in the middle of a day, so the whole day matches
        const ui16 day = threshold.Days();
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Date, {}, offset, day));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Date, {}, offset, ui16(day + 1)));

        const ui32 second = threshold.Seconds();
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Datetime, {}, offset, second));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Datetime, {}, offset, ui32(second + 1)));
    }

    Y_UNIT_TEST(SignedTypes) {
        const TDuration offset = TDuration::Days(1);
        const TInstant threshold = Now - offset;

        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(threshold.Days())));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(threshold.Days() + 1)));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Datetime64, {}, offset, i64(threshold.Seconds())));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Datetime64, {}, offset, i64(threshold.Seconds() + 1)));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Timestamp64, {}, offset, i64(threshold.MicroSeconds())));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Timestamp64, {}, offset, i64(threshold.MicroSeconds() + 1)));

        // Values before the epoch always match a threshold after the epoch
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(-1)));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Datetime64, {}, offset, i64(-1)));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Timestamp64, {}, offset, i64(-1)));
    }

    Y_UNIT_TEST(ThresholdBeforeEpoch) {
        // now - offset is 10 days before the epoch
        const TInstant now = TInstant::Days(10);
        const TDuration offset = TDuration::Days(20);

        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(-10), now));
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(-11), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Date32, {}, offset, i32(-9), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Timestamp64, {}, offset, i64(0), now));

        // Unsigned columns cannot hold such values, nothing matches
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Timestamp, {}, offset, ui64(0), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Date, {}, offset, ui16(0), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Uint64,
            NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, ui64(0), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::DyNumber,
            NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, DyNumber(0), now));
    }

    Y_UNIT_TEST(UnsignedUnits) {
        const TDuration offset = TDuration::Days(1);
        const TInstant threshold = Now - offset;

        const ui64 seconds = threshold.Seconds();
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::Uint32, NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, ui32(seconds)));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::Uint32, NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, ui32(seconds + 1)));

        const auto type = NScheme::NTypeIds::Uint64;
        UNIT_ASSERT(SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, seconds));
        UNIT_ASSERT(!SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, seconds + 1));

        const ui64 millis = threshold.MilliSeconds();
        UNIT_ASSERT(SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS, offset, millis));
        UNIT_ASSERT(!SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS, offset, millis + 1));

        const ui64 micros = threshold.MicroSeconds();
        UNIT_ASSERT(SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS, offset, micros));
        UNIT_ASSERT(!SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS, offset, micros + 1));

        // Nanoseconds are compared as whole microseconds, like TTL does
        const ui64 nanos = micros * 1000;
        UNIT_ASSERT(SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS, offset, nanos + 999));
        UNIT_ASSERT(!SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS, offset, nanos + 1000));

        // Values that do not fit into i64 are in the far future
        UNIT_ASSERT(!SingleRuleMatches(type, NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS, offset, Max<ui64>()));
    }

    Y_UNIT_TEST(DyNumberUnits) {
        const TDuration offset = TDuration::Days(1);
        const TInstant threshold = Now - offset;
        const auto type = NScheme::NTypeIds::DyNumber;

        for (auto unit : {
                NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS})
        {
            const ui64 value = ToUnit(threshold, type, unit);
            UNIT_ASSERT_C(SingleRuleMatches(type, unit, offset, DyNumber(value - 1)), unit);
            UNIT_ASSERT_C(SingleRuleMatches(type, unit, offset, DyNumber(value)), unit);
            UNIT_ASSERT_C(!SingleRuleMatches(type, unit, offset, DyNumber(value + 1)), unit);
        }
    }

    Y_UNIT_TEST(EquivalenceWithTtl) {
        const TDuration offset = TDuration::Days(1);
        const TInstant wallClock = Now - offset;

        auto checkUnsigned = [&](NScheme::TTypeId type, EUnit unit) {
            const ui64 center = ToUnit(wallClock, type, unit);
            for (i64 delta = -1100; delta <= 1100; ++delta) {
                const ui64 value = center + delta;
                bool matches;
                switch (type) {
                    case NScheme::NTypeIds::Date:
                        matches = SingleRuleMatches(type, unit, offset, ui16(value));
                        break;
                    case NScheme::NTypeIds::Datetime:
                    case NScheme::NTypeIds::Uint32:
                        if (value > Max<ui32>()) {
                            continue;
                        }
                        matches = SingleRuleMatches(type, unit, offset, ui32(value));
                        break;
                    default:
                        matches = SingleRuleMatches(type, unit, offset, value);
                        break;
                }
                UNIT_ASSERT_VALUES_EQUAL_C(matches, TtlExpiredUi64(type, unit, value, wallClock),
                    "type# " << type << " unit# " << int(unit) << " value# " << value);
            }
        };

        checkUnsigned(NScheme::NTypeIds::Date, {});
        checkUnsigned(NScheme::NTypeIds::Datetime, {});
        checkUnsigned(NScheme::NTypeIds::Timestamp, {});
        for (auto unit : {
                NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS,
                NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS})
        {
            checkUnsigned(NScheme::NTypeIds::Uint64, unit);
            if (unit == NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS) {
                checkUnsigned(NScheme::NTypeIds::Uint32, unit);
            }
        }

        for (auto type : {NScheme::NTypeIds::Date32, NScheme::NTypeIds::Datetime64, NScheme::NTypeIds::Timestamp64}) {
            const i64 center = ToUnit(wallClock, type, {});
            for (i64 value : {center - 1, center, center + 1, i64(-1), i64(0)}) {
                const bool matches = type == NScheme::NTypeIds::Date32
                    ? SingleRuleMatches(type, {}, offset, i32(value))
                    : SingleRuleMatches(type, {}, offset, value);
                UNIT_ASSERT_VALUES_EQUAL_C(matches, TtlExpiredI64(type, value, wallClock),
                    "type# " << type << " value# " << value);
            }
        }
    }

    Y_UNIT_TEST(RulesOrder) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);
        // Coldest first: P3D -> tier 2, then P1D -> tier 1
        TTierPredicate predicate(MakePolicy({{TDuration::Days(3), 2}, {TDuration::Days(1), 1}}));
        predicate.Bind(Now, *lay);

        auto age = [&](TDuration age) {
            return EvaluateValue(*lay, predicate, (Now - age).MicroSeconds());
        };

        UNIT_ASSERT(age(TDuration::Hours(12)) == Tier(0));
        UNIT_ASSERT(age(TDuration::Days(2)) == Tier(1));
        UNIT_ASSERT(age(TDuration::Days(5)) == Tier(2));
    }

    Y_UNIT_TEST(EmptyPolicy) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);
        TTierPredicate predicate(MakePolicy({}));
        predicate.Bind(Now, *lay);

        UNIT_ASSERT(EvaluateValue(*lay, predicate, ui64(0)) == Tier(0));
    }

    Y_UNIT_TEST(Undecided) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);
        const TRowScheme& scheme = *lay;
        const TPos pos = scheme.ColInfo(ValueTag)->Pos;
        TTierPredicate predicate(MakePolicy({{TDuration::Days(1), 1}}));
        predicate.Bind(Now, scheme);

        const TCell old = TCell::Make(ui64(0));
        UNIT_ASSERT(predicate.Evaluate(MakeRow(scheme, old)) == Tier(1));

        // Explicit null
        UNIT_ASSERT(predicate.Evaluate(MakeRow(scheme, TCell(), ECellOp::Null)).IsUndecided());

        // Cell reset to the schema default
        UNIT_ASSERT(predicate.Evaluate(MakeRow(scheme, TCell(), ECellOp::Reset)).IsUndecided());

        // Value stored outside of the row
        UNIT_ASSERT(predicate.Evaluate(MakeRow(scheme, old, TCellOp(ECellOp::Set, ELargeObj::Outer))).IsUndecided());
        UNIT_ASSERT(predicate.Evaluate(MakeRow(scheme, old, TCellOp(ECellOp::Set, ELargeObj::Extern))).IsUndecided());

        // Partial update without the tiering column
        {
            TRowState row(scheme.Tags().size());
            row.Touch(ERowOp::Upsert);
            row.Set(scheme.ColInfo(KeyTag)->Pos, ECellOp::Set, TCell::Make(ui64(1)));
            UNIT_ASSERT(ECellOp(row.GetCellOp(pos)) == ECellOp::Empty);
            UNIT_ASSERT(predicate.Evaluate(row).IsUndecided());
        }

        // Erase marker
        {
            TRowState row(scheme.Tags().size());
            row.Touch(ERowOp::Erase);
            UNIT_ASSERT(predicate.Evaluate(row).IsUndecided());
        }

        // Full row replacement is decided as usual
        {
            TRowState row(scheme.Tags().size());
            row.Touch(ERowOp::Reset);
            row.Set(pos, ECellOp::Set, old);
            UNIT_ASSERT(predicate.Evaluate(row) == Tier(1));
        }
    }

    Y_UNIT_TEST(Rebind) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);
        TTierPredicate predicate(MakePolicy({{TDuration::Days(1), 1}}));
        const ui64 value = (Now - TDuration::Hours(12)).MicroSeconds();

        predicate.Bind(Now, *lay);
        UNIT_ASSERT(EvaluateValue(*lay, predicate, value) == Tier(0));

        predicate.Bind(Now + TDuration::Days(1), *lay);
        UNIT_ASSERT(EvaluateValue(*lay, predicate, value) == Tier(1));
    }

    Y_UNIT_TEST(SchemeChange) {
        constexpr TTag DroppedTag = 2;
        constexpr TTag TieringTag = 3;

        TLayoutCook before;
        before
            .Col(0, KeyTag, NScheme::NTypeIds::Uint64)
            .Col(0, DroppedTag, NScheme::NTypeIds::Uint64)
            .Col(0, TieringTag, NScheme::NTypeIds::Timestamp)
            .Key({ KeyTag });

        // The tiering column shifts to a lower position once column 2 is dropped
        TLayoutCook after;
        after
            .Col(0, KeyTag, NScheme::NTypeIds::Uint64)
            .Col(0, TieringTag, NScheme::NTypeIds::Timestamp)
            .Key({ KeyTag });

        auto makeRow = [&](const TRowScheme& scheme, ui64 value) {
            TRowState row(scheme.Tags().size());
            row.Touch(ERowOp::Upsert);
            row.Set(scheme.ColInfo(TieringTag)->Pos, ECellOp::Set, TCell::Make(value));
            return row;
        };

        auto policy = MakePolicy({{TDuration::Days(1), 1}});
        policy.ColumnTag = TieringTag;
        TTierPredicate predicate(policy);

        predicate.Bind(Now, *before);
        UNIT_ASSERT_VALUES_EQUAL(predicate.GetColumnPos(), 2u);
        UNIT_ASSERT(predicate.Evaluate(makeRow(*before, 0)) == Tier(1));

        predicate.Bind(Now, *after);
        UNIT_ASSERT_VALUES_EQUAL(predicate.GetColumnPos(), 1u);
        UNIT_ASSERT(predicate.Evaluate(makeRow(*after, 0)) == Tier(1));
        UNIT_ASSERT(predicate.Evaluate(makeRow(*after, Now.MicroSeconds())) == Tier(0));
    }

    Y_UNIT_TEST(PolicyChanges) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);

        auto decide = [&](const TTieringPolicy& policy, TDuration age) {
            TTierPredicate predicate(policy);
            predicate.Bind(Now, *lay);
            return EvaluateValue(*lay, predicate, (Now - age).MicroSeconds());
        };

        // Tier ids are not positions: tier1 has id 10, tier2 has id 20
        const auto original = MakePolicy({{TDuration::Days(3), 20}, {TDuration::Days(1), 10}});
        UNIT_ASSERT(decide(original, TDuration::Hours(12)) == Tier(0));
        UNIT_ASSERT(decide(original, TDuration::Hours(60)) == Tier(10));
        UNIT_ASSERT(decide(original, TDuration::Days(5)) == Tier(20));

        // A new tier (id 30) inserted in the middle only retargets rows between its bounds
        const auto inserted = MakePolicy({{TDuration::Days(3), 20}, {TDuration::Days(2), 30}, {TDuration::Days(1), 10}});
        UNIT_ASSERT(decide(inserted, TDuration::Hours(12)) == Tier(0));
        UNIT_ASSERT(decide(inserted, TDuration::Hours(36)) == Tier(10));
        UNIT_ASSERT(decide(inserted, TDuration::Hours(60)) == Tier(30));
        UNIT_ASSERT(decide(inserted, TDuration::Days(5)) == Tier(20));

        // A removed tier sends its rows to the default tier, other ids are stable
        const auto removed = MakePolicy({{TDuration::Days(3), 20}});
        UNIT_ASSERT(decide(removed, TDuration::Hours(60)) == Tier(0));
        UNIT_ASSERT(decide(removed, TDuration::Days(5)) == Tier(20));

        // A longer interval makes the desired tier warmer (rewarming)
        const auto longer = MakePolicy({{TDuration::Days(3), 20}, {TDuration::Days(2), 10}});
        UNIT_ASSERT(decide(original, TDuration::Hours(36)) == Tier(10));
        UNIT_ASSERT(decide(longer, TDuration::Hours(36)) == Tier(0));
    }

    Y_UNIT_TEST(InvalidPolicy) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);

        auto unknownColumn = MakePolicy({{TDuration::Days(1), 1}});
        unknownColumn.ColumnTag = 100;
        UNIT_ASSERT_EXCEPTION(TTierPredicate(unknownColumn).Bind(Now, *lay), yexception);

        auto stringLay = MakeLayout(NScheme::NTypeIds::String);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakePolicy({{TDuration::Days(1), 1}})).Bind(Now, *stringLay), yexception);

        auto uintLay = MakeLayout(NScheme::NTypeIds::Uint64);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakePolicy({{TDuration::Days(1), 1}})).Bind(Now, *uintLay), yexception);

        // Rules must go from the coldest tier to the warmest
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakePolicy({{TDuration::Days(1), 1}, {TDuration::Days(3), 2}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakePolicy({{TDuration::Days(1), 1}, {TDuration::Days(1), 2}})), yexception);

        TTierPredicate unbound(MakePolicy({{TDuration::Days(1), 1}}));
        UNIT_ASSERT_EXCEPTION(EvaluateValue(*lay, unbound, ui64(0)), yexception);

        // A failed bind leaves the predicate unbound
        TTierPredicate rebound(MakePolicy({{TDuration::Days(1), 1}}));
        rebound.Bind(Now, *lay);
        UNIT_ASSERT_EXCEPTION(rebound.Bind(Now, *stringLay), yexception);
        UNIT_ASSERT_EXCEPTION(EvaluateValue(*lay, rebound, ui64(0)), yexception);
    }

}

}
}
