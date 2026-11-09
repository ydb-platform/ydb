#include <ydb/core/tablet_flat/flat_compaction_router.h>
#include <ydb/core/tablet_flat/test/libs/rows/layout.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NTabletFlatExecutor {
namespace {

    using namespace NTable;

    constexpr ui32 ColdTier = 42;
    constexpr ui32 ArchiveTier = 7;
    constexpr TTag KeyTag = 0;
    constexpr TTag TimeTag = 1;
    const TInstant Now = TInstant::Days(400);

    TTieringPolicy RelativePolicy() {
        return TTieringPolicy{
            .ColumnTag = TimeTag,
            .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
            .Rules = {
                {.TierId = ArchiveTier, .Ranges = {{
                    .To = TTierRelativeBound{TDuration::Days(365)}, .ToInclusive = true}}},
                {.TierId = ColdTier, .Ranges = {{
                    .To = TTierRelativeBound{TDuration::Days(7)}, .ToInclusive = true}}},
            },
        };
    }

    struct TFixture {
        NTest::TLayoutCook Layout;
        TCompactCfg Conf{MakeHolder<TCompactionParams>()};

        TFixture() {
            Layout.Col(0, KeyTag, NScheme::NTypeIds::Uint32)
                .Col(0, TimeTag, NScheme::NTypeIds::Uint64)
                .Key({KeyTag});
            Conf.Params->Mode = TCompactionParams::EMode::TierAware;
            Conf.Params->Context = TCompactionContext::Build({}, TVector<TLogoBlobID>{});
            // Output indexes, output identities and tier identities differ.
            for (const auto& [tier, id] : {std::pair{ArchiveTier, ui32(900)},
                    std::pair{ui32(0), ui32(17)}, std::pair{ColdTier, ui32(31)}}) {
                auto& output = Conf.Outputs.emplace_back();
                output.TierId = tier;
                output.OutputId = id;
            }
        }

        THolder<TTierCompactionRouter> Router(std::optional<TTieringPolicy> policy,
                TInstant now = Now, bool allowCooling = true) const {
            auto result = MakeHolder<TTierCompactionRouter>(Conf, std::move(policy),
                now, TVector<ui32>{0, ColdTier, ArchiveTier}, allowCooling);
            result->Prepare(*Layout);
            return result;
        }

        TRowState Row(ui64 seconds, ERowOp op = ERowOp::Upsert,
                TCellOp cellOp = ECellOp::Set) const {
            TRowState row(Layout.RowScheme()->Cols.size());
            row.Touch(op);
            if (op != ERowOp::Erase) {
                row.Set(Layout.RowScheme()->ColInfo(TimeTag)->Pos, cellOp, TCell::Make(seconds));
            }
            return row;
        }

        void SetRemaining(ui32 tier) {
            const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
                TLogoBlobID(1, 1, 1, 1, 0, 0), TEpoch::FromIndex(1));
            Conf.Params->Context = TCompactionContext::Build({{part, 2, tier}},
                TVector<TLogoBlobID>{});
        }
    };

    ui32 Route(TTierCompactionRouter& router, const TRowState* row, ui32 sourceTier,
            ui32 keyValue = 1) {
        const TCell key = TCell::Make(keyValue);
        return router.RouteKey({&key, 1}, row, sourceTier);
    }

}

Y_UNIT_TEST_SUITE(TTierCompactionRouterTest) {

    Y_UNIT_TEST(RelativePolicyReturnsOutputIndexes) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        const auto archive = f.Row(TInstant::Days(1).Seconds());
        const auto cold = f.Row(TInstant::Days(370).Seconds());
        const auto fresh = f.Row(TInstant::Days(399).Seconds());

        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &archive, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &cold, 0, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, ArchiveTier, 3), 1);
    }

    Y_UNIT_TEST(RelativeInclusiveBoundaryAndFixedEvaluationTime) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        const auto boundary = f.Row((Now - TDuration::Days(7)).Seconds());
        const auto fresh = f.Row((Now - TDuration::Days(1)).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &boundary, 0), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, 0, 2), 1);

        auto later = f.Router(RelativePolicy(), Now + TDuration::Days(7));
        UNIT_ASSERT_VALUES_EQUAL(Route(*later, &fresh, 0), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, 0, 3), 1);
    }

    Y_UNIT_TEST(OwnsThePolicySnapshot) {
        TFixture f;
        auto policy = RelativePolicy();
        TTierCompactionRouter router(f.Conf, policy, Now, {0, ColdTier, ArchiveTier});
        policy.Rules.clear();
        router.Prepare(*f.Layout);
        const auto cold = f.Row(TInstant::Days(370).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(router, &cold, 0), 2);
    }

    Y_UNIT_TEST(AbsoluteRangesChooseTheMatchingTier) {
        TFixture f;
        TTieringPolicy policy{
            .ColumnTag = TimeTag,
            .Unit = NKikimrSchemeOp::TTTLSettings::UNIT_SECONDS,
            .Rules = {
                {.TierId = ColdTier, .Ranges = {{
                    .From = TTierAbsoluteBound{i64(TInstant::Days(10).MicroSeconds())},
                    .To = TTierAbsoluteBound{i64(TInstant::Days(20).MicroSeconds())}}}},
                {.TierId = ArchiveTier, .Ranges = {{
                    .From = TTierAbsoluteBound{0},
                    .To = TTierAbsoluteBound{i64(TInstant::Days(5).MicroSeconds())}}}},
            },
        };
        auto router = f.Router(policy);
        const auto archive = f.Row(TInstant::Days(1).Seconds());
        const auto cold = f.Row(TInstant::Days(10).Seconds());
        const auto outside = f.Row(TInstant::Days(20).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &archive, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &cold, 0, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &outside, ColdTier, 3), 1);
    }

    Y_UNIT_TEST(UndecidedRowsPreserveTheirOwnSourceTier) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        const auto null = f.Row(0, ERowOp::Upsert, ECellOp::Null);
        const auto reset = f.Row(0, ERowOp::Upsert, ECellOp::Reset);
        const auto erase = f.Row(0, ERowOp::Erase);
        const auto outer = f.Row(0, ERowOp::Upsert, TCellOp(ECellOp::Set, ELargeObj::Outer));
        const auto external = f.Row(0, ERowOp::Upsert, TCellOp(ECellOp::Set, ELargeObj::Extern));
        TRowState partial(f.Layout.RowScheme()->Cols.size());
        partial.Touch(ERowOp::Upsert);
        for (const auto* row : {&null, &reset, &erase, &outer, &external,
                static_cast<const TRowState*>(&partial)}) {
            UNIT_ASSERT_VALUES_EQUAL(Route(*router, row, ColdTier), 2);
        }
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &null, ArchiveTier), 0);
    }

    Y_UNIT_TEST(ResetWithAValueCanBeDecided) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        const auto row = f.Row(TInstant::Days(370).Seconds(), ERowOp::Reset);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &row, 0), 2);
    }

    Y_UNIT_TEST(KeysWithoutACommittedRowKeepTheirSourceTier) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, nullptr, ColdTier), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, nullptr, ArchiveTier, 2), 0);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, nullptr, 0, 3), 1);
    }

    Y_UNIT_TEST(NoPolicyPreservesPlacementEvenForOldRows) {
        TFixture f;
        auto router = f.Router(std::nullopt);
        const auto old = f.Row(0);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, ColdTier, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, ArchiveTier, 3), 0);
    }

    Y_UNIT_TEST(CapStopsCoolingAndWarmingAtTheIntermediateTier) {
        TFixture f;
        f.SetRemaining(ColdTier);
        auto router = f.Router(RelativePolicy());
        const auto old = f.Row(0);
        const auto fresh = f.Row((Now - TDuration::Days(1)).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, 0), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, ArchiveTier, 2), 2);
    }

    Y_UNIT_TEST(EmptyIntermediateTierAllowsDirectMovesBothWays) {
        TFixture f;
        auto router = f.Router(RelativePolicy());
        const auto old = f.Row(0);
        const auto fresh = f.Row((Now - TDuration::Days(1)).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, 0), 0);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, ArchiveTier, 2), 1);
    }

    Y_UNIT_TEST(DisablingCoolingStillAllowsWarming) {
        TFixture f;
        auto router = f.Router(RelativePolicy(), Now, false);
        const auto old = f.Row(0);
        const auto fresh = f.Row((Now - TDuration::Days(1)).Seconds());
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, 0), 1);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &old, ColdTier, 2), 2);
        UNIT_ASSERT_VALUES_EQUAL(Route(*router, &fresh, ArchiveTier, 3), 1);
    }

    Y_UNIT_TEST(RequiresPreparedContextAndTierAwareMode) {
        TFixture f;
        f.Conf.Params->Mode = TCompactionParams::EMode::Regular;
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
        f.Conf.Params->Mode = TCompactionParams::EMode::TierAware;
        f.Conf.Params->Context = nullptr;
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
    }

    Y_UNIT_TEST(RejectsFulltext) {
        TFixture f;
        f.Conf.IsFulltextCompact = true;
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
    }

    Y_UNIT_TEST(RequiresUniqueOutputsForEveryPossibleDestination) {
        TFixture f;
        f.Conf.Outputs.pop_back();
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
        f.Conf.Outputs.emplace_back().TierId = 0;
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
    }

    Y_UNIT_TEST(RejectsPolicyTargetsWithoutAnOutput) {
        TFixture f;
        auto policy = RelativePolicy();
        policy.Rules.front().TierId = 99;
        UNIT_ASSERT_EXCEPTION(f.Router(policy), yexception);
    }

    Y_UNIT_TEST(RejectsUnknownInputAndRemainingTiers) {
        TFixture f;
        f.SetRemaining(99);
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
        const TIntrusiveConstPtr<TColdPart> part = new TColdPart(
            TLogoBlobID(1, 1, 2, 1, 0, 0), TEpoch::FromIndex(2));
        f.Conf.Params->Context = TCompactionContext::Build({{part, 2, 99}},
            TVector<TLogoBlobID>{part->Label});
        UNIT_ASSERT_EXCEPTION(f.Router(RelativePolicy()), yexception);
    }

    Y_UNIT_TEST(RejectsDuplicateTierOrder) {
        TFixture f;
        UNIT_ASSERT_EXCEPTION(TTierCompactionRouter(f.Conf, RelativePolicy(), Now,
            (TVector<ui32>{0, ColdTier, ArchiveTier, ColdTier})), yexception);
    }

    Y_UNIT_TEST(RequiresPreparationOnceAndKnownSourceTiers) {
        TFixture f;
        TTierCompactionRouter router(f.Conf, RelativePolicy(), Now, {0, ColdTier, ArchiveTier});
        UNIT_ASSERT_EXCEPTION(Route(router, nullptr, 0), yexception);
        router.Prepare(*f.Layout);
        UNIT_ASSERT_EXCEPTION(router.Prepare(*f.Layout), yexception);
        UNIT_ASSERT_EXCEPTION(Route(router, nullptr, 99), yexception);
        UNIT_ASSERT(router.GetUnrepresentableTierIds().empty());
    }

}

}
