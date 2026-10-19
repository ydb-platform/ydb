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

    // Interval(offset): value <= now - offset
    TTierRange Interval(TDuration offset) {
        return TTierRange{ .To = TTierRelativeBound{ offset }, .ToInclusive = true };
    }

    TTieringPolicy MakePolicy(std::initializer_list<std::pair<TDuration, ui32>> rules,
            EUnit unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO)
    {
        TTieringPolicy policy;
        policy.ColumnTag = ValueTag;
        policy.Unit = unit;
        for (const auto& [offset, tierId] : rules) {
            policy.Rules.push_back(TTierRule{ .TierId = tierId, .Ranges = { Interval(offset) } });
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
        UNIT_ASSERT(predicate.GetUnrepresentableTierIds().empty());
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

    TString DyNumber(i64 value) {
        auto result = NDyNumber::ParseDyNumberString(ToString(value));
        Y_ENSURE(result);
        return *result;
    }

    TString DyNumber(TStringBuf text) {
        auto result = NDyNumber::ParseDyNumberString(text);
        Y_ENSURE(result);
        return *result;
    }

    // Days since the epoch for a proleptic Gregorian date
    // (http://howardhinnant.github.io/date_algorithms.html#days_from_civil)
    i64 DaysFromCivil(i64 year, ui32 month, ui32 day) {
        year -= month <= 2 ? 1 : 0;
        const i64 era = (year >= 0 ? year : year - 399) / 400;
        const ui32 yoe = static_cast<ui32>(year - era * 400);
        const ui32 doy = (153 * (month > 2 ? month - 3 : month + 9) + 2) / 5 + day - 1;
        const ui32 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return era * 146097 + static_cast<i64>(doe) - 719468;
    }

    TTierAbsoluteBound Date(i64 year, ui32 month = 1, ui32 day = 1) {
        return TTierAbsoluteBound{ DaysFromCivil(year, month, day) * 86400 * 1000000 };
    }

    TTierRange Year(i64 year) {
        return TTierRange{ .From = Date(year), .To = Date(year + 1) };
    }

    TTieringPolicy MakeRangePolicy(std::initializer_list<std::pair<TVector<TTierRange>, ui32>> rules,
            EUnit unit = NKikimrSchemeOp::TTTLSettings::UNIT_AUTO)
    {
        TTieringPolicy policy;
        policy.ColumnTag = ValueTag;
        policy.Unit = unit;
        for (const auto& [ranges, tierId] : rules) {
            policy.Rules.push_back(TTierRule{ .TierId = tierId, .Ranges = ranges });
        }
        return policy;
    }

    template <typename T>
    bool RangesMatch(NScheme::TTypeId type, EUnit unit, TVector<TTierRange> ranges, T value, TInstant now = Now) {
        auto lay = MakeLayout(type);
        TTierPredicate predicate(MakeRangePolicy({{std::move(ranges), 1}}, unit));
        predicate.Bind(now, *lay);
        UNIT_ASSERT(predicate.GetUnrepresentableTierIds().empty());
        return EvaluateValue(*lay, predicate, value) == Tier(1);
    }

    // Checks the edges of a range given in column units: [from, to)
    template <typename TMake>
    void CheckEdges(NScheme::TTypeId type, EUnit unit, const TTierRange& range, i64 from, i64 to, TMake make) {
        UNIT_ASSERT_C(!RangesMatch(type, unit, {range}, make(from - 1)), "type# " << type << " unit# " << int(unit));
        UNIT_ASSERT_C(RangesMatch(type, unit, {range}, make(from)), "type# " << type << " unit# " << int(unit));
        UNIT_ASSERT_C(RangesMatch(type, unit, {range}, make(to - 1)), "type# " << type << " unit# " << int(unit));
        UNIT_ASSERT_C(!RangesMatch(type, unit, {range}, make(to)), "type# " << type << " unit# " << int(unit));
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

        // DyNumber is signed, so values before the threshold match
        UNIT_ASSERT(SingleRuleMatches(NScheme::NTypeIds::DyNumber,
            NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, DyNumber(-864000), now));
        UNIT_ASSERT(!SingleRuleMatches(NScheme::NTypeIds::DyNumber,
            NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS, offset, DyNumber(-863999), now));
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

            // Negative values are older than a positive threshold
            UNIT_ASSERT_C(SingleRuleMatches(type, unit, offset, DyNumber(-1)), unit);
            UNIT_ASSERT_C(SingleRuleMatches(type, unit, offset, DyNumber(-i64(value))), unit);
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
            } else {
                // The current time in finer units does not fit into Uint32, check the edges of its range
                for (ui32 value : {ui32(0), ui32(1), Max<ui32>() - 1, Max<ui32>()}) {
                    UNIT_ASSERT_VALUES_EQUAL_C(
                        SingleRuleMatches(NScheme::NTypeIds::Uint32, unit, offset, value),
                        TtlExpiredUi64(NScheme::NTypeIds::Uint32, unit, value, wallClock),
                        "unit# " << int(unit) << " value# " << value);
                }
            }
        }

        // Saturation of the nanoseconds threshold (now is around year 2554)
        {
            const auto unit = NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS;
            const TInstant farFuture = TInstant::MicroSeconds((Max<ui64>() - 999) / 1000 + 1) + offset;
            for (ui64 value : {ui64(0), Max<ui64>() - 1, Max<ui64>()}) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    SingleRuleMatches(NScheme::NTypeIds::Uint64, unit, offset, value, farFuture),
                    TtlExpiredUi64(NScheme::NTypeIds::Uint64, unit, value, farFuture - offset),
                    "value# " << value);
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

    Y_UNIT_TEST(AbsoluteRangeEdges) {
        const auto seconds = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
        const auto millis = NKikimrSchemeOp::TTTLSettings::UNIT_MILLISECONDS;
        const auto micros = NKikimrSchemeOp::TTTLSettings::UNIT_MICROSECONDS;
        const auto nanos = NKikimrSchemeOp::TTTLSettings::UNIT_NANOSECONDS;

        for (i64 year : {2025, 1965}) {
            const auto range = Year(year);
            const i64 fromUs = std::get<TTierAbsoluteBound>(*range.From).Micros;
            const i64 toUs = std::get<TTierAbsoluteBound>(*range.To).Micros;
            const i64 day = 86400ll * 1000000;

            CheckEdges(NScheme::NTypeIds::Date32, {}, range, fromUs / day, toUs / day, [](i64 v) { return i32(v); });
            CheckEdges(NScheme::NTypeIds::Datetime64, {}, range, fromUs / 1000000, toUs / 1000000, [](i64 v) { return v; });
            CheckEdges(NScheme::NTypeIds::Timestamp64, {}, range, fromUs, toUs, [](i64 v) { return v; });
            for (auto unit : {seconds, millis, micros}) {
                const i64 scale = unit == seconds ? 1000000 : unit == millis ? 1000 : 1;
                CheckEdges(NScheme::NTypeIds::DyNumber, unit, range, fromUs / scale, toUs / scale,
                    [](i64 v) { return DyNumber(v); });
            }
            CheckEdges(NScheme::NTypeIds::DyNumber, nanos, range, fromUs * 1000, toUs * 1000,
                [](i64 v) { return DyNumber(v); });

            if (year < 1970) {
                continue;
            }

            CheckEdges(NScheme::NTypeIds::Date, {}, range, fromUs / day, toUs / day, [](i64 v) { return ui16(v); });
            CheckEdges(NScheme::NTypeIds::Datetime, {}, range, fromUs / 1000000, toUs / 1000000, [](i64 v) { return ui32(v); });
            CheckEdges(NScheme::NTypeIds::Timestamp, {}, range, fromUs, toUs, [](i64 v) { return ui64(v); });
            CheckEdges(NScheme::NTypeIds::Uint32, seconds, range, fromUs / 1000000, toUs / 1000000, [](i64 v) { return ui32(v); });
            for (auto unit : {seconds, millis, micros}) {
                const i64 scale = unit == seconds ? 1000000 : unit == millis ? 1000 : 1;
                CheckEdges(NScheme::NTypeIds::Uint64, unit, range, fromUs / scale, toUs / scale, [](i64 v) { return ui64(v); });
            }
            // Nanoseconds are compared as whole microseconds, like TTL does
            CheckEdges(NScheme::NTypeIds::Uint64, nanos, range, fromUs * 1000, toUs * 1000, [](i64 v) { return ui64(v); });
        }
    }

    Y_UNIT_TEST(AbsoluteFractionalDyNumber) {
        // DyNumber values may be fractional, so absolute bounds are exact
        const auto seconds = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS;
        const i64 from = Date(2025).Micros / 1000000;
        const i64 to = Date(2026).Micros / 1000000;
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {Year(2025)}, DyNumber(ToString(from - 1) + ".5")));
        UNIT_ASSERT(RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {Year(2025)}, DyNumber(ToString(from) + ".5")));
        UNIT_ASSERT(RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {Year(2025)}, DyNumber(ToString(to - 1) + ".999")));
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {Year(2025)}, DyNumber(ToString(to) + ".5")));

        // A bound with a fraction of a second
        const TTierRange range{ .From = TTierAbsoluteBound{ Date(2025).Micros + 500000 } };
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {range}, DyNumber(ToString(from) + ".4")));
        UNIT_ASSERT(RangesMatch(NScheme::NTypeIds::DyNumber, seconds, {range}, DyNumber(ToString(from) + ".5")));
    }

    Y_UNIT_TEST(AbsoluteOpenAndInclusiveRanges) {
        const auto type = NScheme::NTypeIds::Timestamp;
        const ui64 from = Date(2025).Micros;

        UNIT_ASSERT(!RangesMatch(type, {}, {TTierRange{ .From = Date(2025) }}, from - 1));
        UNIT_ASSERT(RangesMatch(type, {}, {TTierRange{ .From = Date(2025) }}, Max<ui64>()));
        UNIT_ASSERT(RangesMatch(type, {}, {TTierRange{ .To = Date(2025) }}, ui64(0)));
        UNIT_ASSERT(!RangesMatch(type, {}, {TTierRange{ .To = Date(2025) }}, from));
        UNIT_ASSERT(RangesMatch(type, {}, {TTierRange{ .To = Date(2025), .ToInclusive = true }}, from));

        // A single point
        const TTierRange point{ .From = Date(2025), .To = Date(2025), .ToInclusive = true };
        UNIT_ASSERT(RangesMatch(type, {}, {point}, from));
        UNIT_ASSERT(!RangesMatch(type, {}, {point}, from + 1));
        UNIT_ASSERT(!RangesMatch(type, {}, {point}, from - 1));
    }

    Y_UNIT_TEST(AbsoluteBoundsOutsideColumnRange) {
        // Unsigned columns cannot hold values before the epoch
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::Timestamp, {}, {Year(1965)}, ui64(0)));
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::Timestamp, {}, {TTierRange{ .To = Date(1965) }}, ui64(0)));
        UNIT_ASSERT(RangesMatch(NScheme::NTypeIds::Timestamp, {}, {TTierRange{ .From = Date(1965) }}, ui64(0)));

        // Date holds days up to year 2149
        UNIT_ASSERT(!RangesMatch(NScheme::NTypeIds::Date, {}, {Year(2200)}, Max<ui16>()));
        UNIT_ASSERT(RangesMatch(NScheme::NTypeIds::Date, {}, {TTierRange{ .To = Date(2200) }}, Max<ui16>()));
    }

    Y_UNIT_TEST(MultipleRanges) {
        const auto type = NScheme::NTypeIds::Timestamp;
        const TVector<TTierRange> ranges{Year(2023), Year(2025)};
        UNIT_ASSERT(RangesMatch(type, {}, ranges, ui64(Date(2023, 6, 1).Micros)));
        UNIT_ASSERT(!RangesMatch(type, {}, ranges, ui64(Date(2024, 6, 1).Micros)));
        UNIT_ASSERT(RangesMatch(type, {}, ranges, ui64(Date(2025, 6, 1).Micros)));
    }

    Y_UNIT_TEST(RelativeRange) {
        // From 3 days ago (inclusive) to 1 day ago (exclusive)
        const TTierRange range{ .From = TTierRelativeBound{ TDuration::Days(3) }, .To = TTierRelativeBound{ TDuration::Days(1) } };
        auto age = [&](TDuration age) {
            return RangesMatch(NScheme::NTypeIds::Timestamp, {}, {range}, (Now - age).MicroSeconds());
        };
        UNIT_ASSERT(!age(TDuration::Hours(12)));
        UNIT_ASSERT(!age(TDuration::Days(1)));
        UNIT_ASSERT(age(TDuration::Days(2)));
        UNIT_ASSERT(age(TDuration::Days(3)));
        UNIT_ASSERT(!age(TDuration::Days(5)));
    }

    Y_UNIT_TEST(AbsoluteRulesOrderDoesNotMatter) {
        auto lay = MakeLayout(NScheme::NTypeIds::Timestamp);
        auto decide = [&](const TTieringPolicy& policy, i64 year) {
            TTierPredicate predicate(policy);
            predicate.Bind(Now, *lay);
            return EvaluateValue(*lay, predicate, ui64(Date(year, 6, 1).Micros));
        };

        const auto forward = MakeRangePolicy({{{Year(2024)}, 10}, {{Year(2025)}, 20}});
        const auto backward = MakeRangePolicy({{{Year(2025)}, 20}, {{Year(2024)}, 10}});
        for (const auto& policy : {forward, backward}) {
            UNIT_ASSERT(decide(policy, 2023) == Tier(0));
            UNIT_ASSERT(decide(policy, 2024) == Tier(10));
            UNIT_ASSERT(decide(policy, 2025) == Tier(20));
            UNIT_ASSERT(decide(policy, 2026) == Tier(0));
        }

        // A new year tier inserted in the middle keeps other tier ids
        const auto inserted = MakeRangePolicy({{{Year(2024)}, 10}, {{Year(2026)}, 30}, {{Year(2025)}, 20}});
        UNIT_ASSERT(decide(inserted, 2024) == Tier(10));
        UNIT_ASSERT(decide(inserted, 2025) == Tier(20));
        UNIT_ASSERT(decide(inserted, 2026) == Tier(30));
    }

    Y_UNIT_TEST(InvalidRangePolicy) {
        const TTierRange interval = Interval(TDuration::Days(1));

        // Relative and absolute bounds cannot be mixed, neither across rules nor within a range
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{Year(2025)}, 2}, {{interval}, 1}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{
            .From = Date(2025), .To = TTierRelativeBound{ TDuration::Days(1) } }}, 1}})), yexception);

        // Ranges of different absolute rules must not overlap
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{Year(2024), Year(2025)}, 1}, {{Year(2025)}, 2}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{ .From = Date(2025) }}, 1}, {{Year(2030)}, 2}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{ .To = Date(2025), .ToInclusive = true }}, 1},
            {{Year(2025)}, 2}})), yexception);
        // Adjacent ranges do not overlap, and ranges of one rule may overlap
        TTierPredicate(MakeRangePolicy({{{Year(2024)}, 1}, {{Year(2025)}, 2}}));
        TTierPredicate(MakeRangePolicy({{{Year(2024), TTierRange{ .From = Date(2024, 6, 1), .To = Date(2025) }}, 1}}));

        // Empty rules, unbounded and empty ranges
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{}, 1}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{}}, 1}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{ .From = Date(2025), .To = Date(2025) }}, 1}})), yexception);
        UNIT_ASSERT_EXCEPTION(TTierPredicate(MakeRangePolicy({{{TTierRange{ .From = Date(2026), .To = Date(2025) }}, 1}})), yexception);
    }

}

}
}
