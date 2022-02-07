#include <library/cpp/testing/gtest/gtest.h>

using namespace testing;

TEST(ProbeStateTest, Example) {
    // check that our test function does not make a copy of passed argument
    auto copyless = [](auto&& x) {
        TProbe p(std::move(x));
        p.Touch();
        return p;
    };

    TProbeState state;
    auto probe = copyless(TProbe(&state));
    EXPECT_EQ(1, state.Touches);
    EXPECT_THAT(state, HasCopyMoveCounts(0, 2));
}

TEST(ProbeTest, Construct) {
    TProbeState state;
    {
        TProbe probe(&state);
        EXPECT_THAT(state, IsAlive());
    }
    EXPECT_THAT(state, IsDead());
}

TEST(ProbeTest, Copy) {
    TProbeState state;

    TProbe probe(&state);
    TProbe copy(probe);
    EXPECT_THAT(state, HasCopyMoveCounts(1, 0));
    EXPECT_THAT(state, NoAssignments());
    EXPECT_THAT(state, NoMoves());

    TProbe copy2 = TProbe::ExplicitlyCreateInvalidProbe();
    copy2 = probe;
    EXPECT_EQ(1, state.CopyAssignments);
}

TEST(ProbeTest, Move) {
    TProbeState state;
    TProbe probe(&state);
    TProbe probe2(std::move(probe));
    EXPECT_FALSE(probe.IsValid());
    EXPECT_THAT(state, NoCopies());

    EXPECT_THAT(state, HasCopyMoveCounts(0, 1));

    TProbe probe3 = TProbe::ExplicitlyCreateInvalidProbe();
    probe3 = std::move(probe2);
    EXPECT_EQ(1, state.MoveAssignments);
}
